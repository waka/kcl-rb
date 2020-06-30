require 'spec_helper'

RSpec.describe Kcl::Checkpointer do
  include_context 'use_dynamodb'

  let(:shard) { Kcl::Workers::ShardInfo.new('SHARD-00001', 0, {}) }
  let(:checkpointer) { Kcl::Checkpointer.new(Kcl.config) }

  describe '#fetch_checkpoint' do
    subject { checkpointer.fetch_checkpoint(shard) }

    it do
      expect(subject.shard_id).to eql(shard.shard_id)
    end
  end

  describe '#update_checkpoint' do
    subject { checkpointer.update_checkpoint(shard) }

    it do
      expect(subject.shard_id).to eql(shard.shard_id)
    end
  end

  describe '#lease' do
    let(:next_assigned_to) { 'test-worker' }
    subject { checkpointer.lease(shard, next_assigned_to) }

    it do
      expect(subject.assigned_to).to eql(next_assigned_to)
    end
  end

  describe '#remove_lease' do
    subject { checkpointer.remove_lease(shard) }

    it do
      expect(subject.shard_id).to eql(shard.shard_id)
    end
  end

  describe '#remove_lease_owner' do
    subject { checkpointer.remove_lease_owner(shard) }

    it do
      expect(subject.assigned_to).to be_nil
    end
  end
end
