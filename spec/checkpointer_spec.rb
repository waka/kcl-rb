require 'spec_helper'

RSpec.describe Kcl::Checkpointer do
  include_context 'use_kinesis'

  let(:checkpointer) { Kcl::Checkpointer.new(Kcl.config) }

  describe '#initialize' do
    it 'exists dynamodb table' do
      dynamodb = checkpointer.dynamodb
      expect(dynamodb.exists?(Kcl.config.dynamodb_table_name)).to be_truthy
    end
  end

  describe '#fetch_checkpoint' do
    subject { checkpointer.fetch_checkpoint(shard) }

    it do
      expect(subject.shard_id).to eql(shard.shard_id)
      expect(subject.checkpoint).to eql(shard.checkpoint)
      expect(subject.assigned_to).to eql(shard.assigned_to)
    end
  end

  describe '#update_checkpoint' do
    before do
      shard.checkpoint = Kcl::Checkpoints::Sentinel::SHARD_END
      shard.assigned_to = 'test-worker'
      shard.lease_timeout = (Time.now.utc + Kcl.config.dynamodb_failover_seconds).to_s
      checkpointer.update_checkpoint(shard)
    end

    subject { checkpointer.fetch_checkpoint(shard) }

    it do
      expect(subject.checkpoint).to eql(Kcl::Checkpoints::Sentinel::SHARD_END)
    end
  end

  describe '#lease' do
    let(:next_assigned_to) { 'test-worker' }

    before do
      checkpointer.lease(shard, next_assigned_to)
    end

    subject { checkpointer.fetch_checkpoint(shard) }

    it do
      expect(subject.assigned_to).to eql(next_assigned_to)
      expect(subject.lease_timeout).not_to eql('')
    end
  end

  describe '#remove_lease' do
    let(:next_assigned_to) { 'test-worker' }

    before do
      checkpointer.lease(shard, next_assigned_to)
      checkpointer.remove_lease(shard)
    end

    subject { checkpointer.fetch_checkpoint(shard) }

    it do
      expect(subject.checkpoint).to be_nil
      expect(subject.assigned_to).to be_nil
    end
  end

  describe '#remove_lease_owner' do
    let(:next_assigned_to) { 'test-worker' }

    before do
      checkpointer.lease(shard, next_assigned_to)
      checkpointer.remove_lease_owner(shard)
    end

    subject { checkpointer.fetch_checkpoint(shard) }

    it do
      expect(subject.assigned_to).to be_nil
    end
  end
end
