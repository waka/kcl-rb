require 'spec_helper'

RSpec.describe Kcl::Worker do
  include_context 'use_kinesis'

  let(:record_processor_factory) { double('record_processor_factory') }
  let(:worker) { Kcl::Worker.new('test-worker', record_processor_factory) }

  before do
    allow(record_processor_factory).to receive(:create_processor)
  end

  describe '#sync_shards!' do
    subject { worker.sync_shards! }
    it { expect(subject.keys.size).to eq(5) }
  end

  describe '#available_lease_shard?' do
    subject { worker.avaliable_leases_count }

    context 'before consume' do
      before do
        worker.sync_shards!
      end

      it { expect(subject).to be_positive }
    end

    context 'after consume' do
      let(:consumer) { instance_double(Kcl::Workers::Consumer) }

      before do
        allow(Kcl::Workers::Consumer).to receive(:new).and_return(consumer)
        allow(consumer).to receive(:consume!).and_return(true)

        worker.sync_shards!
        worker.consume_shards!
      end

      it { expect(subject).to be_positive }
    end
  end
end
