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
end
