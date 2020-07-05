require 'spec_helper'

RSpec.describe Kcl::Workers::Consumer do
  include_context 'use_kinesis'
  include_context 'use_record_processor'

  let(:target_shard) { nil }
  let(:record_processor) { MockRecordProcessor.new }
  let(:checkpointer) { Kcl::Checkpointer.new(Kcl.config) }
  let(:consumer) do
    Kcl::Workers::Consumer.new(target_shard, record_processor, kinesis, checkpointer)
  end

  describe '#start_shard_iterator' do
    let(:target_shard) { shard }
    subject { consumer.start_shard_iterator }
    it { expect(subject).not_to be_nil }
  end

  describe '#consume!' do
    before do
      # mock shard
      checkpointer.fetch_checkpoint(target_shard)
      checkpointer.lease(target_shard, 'test-worker')
    end

    after do
      checkpointer.remove_lease_owner(target_shard)
    end

    context 'with no record' do
      let(:target_shard) { shard }

      before do
        allow(record_processor).to receive(:process_record)
      end

      subject { consumer.consume! }

      it do
        expect(subject).to be_nil
        expect(record_processor).not_to have_received(:process_record)
      end
    end

    context 'with a record' do
      let(:target_shard) { shard_shadow }

      before do
        # put data for 1st shard
        kinesis.put_record(
          {
            stream_name: Kcl.config.kinesis_stream_name,
            data: Base64.strict_encode64('test'),
            partition_key: 'a'
          }
        )

        allow(record_processor).to receive(:process_record)
      end

      subject { consumer.consume! }

      it do
        expect(subject).to be_nil
        expect(record_processor).to have_received(:process_record)
      end
    end
  end
end
