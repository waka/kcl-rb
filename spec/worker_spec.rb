require 'spec_helper'

RSpec.describe Kcl::Worker do
  include_context 'use_kinesis'

  let(:worker) { Kcl::Worker.new('test-worker', nil) }

  describe '#sync_shards!' do
    subject { worker.sync_shards! }
    it { expect(subject.keys.size).to eq(5) }
  end

  describe '#available_lease_shard?' do
    before do
      worker.sync_shards!
    end

    subject { worker.available_lease_shard? }

    it { expect(subject).to be_truthy }
  end
end
