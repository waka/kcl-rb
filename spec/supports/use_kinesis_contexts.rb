RSpec.shared_context 'use_kinesis' do
  let(:kinesis) { Kcl::Proxies::KinesisProxy.new(Kcl.config) }
  let(:kinesis_shards) { kinesis.shards }
  let(:shard_shadow) do
    Kcl::Workers::ShardInfo.new(
      kinesis_shards[0].shard_id,
      kinesis_shards[0].parent_shard_id,
      kinesis_shards[0].sequence_number_range
    )
  end
  let(:shard) do
    Kcl::Workers::ShardInfo.new(
      kinesis_shards[1].shard_id,
      kinesis_shards[1].parent_shard_id,
      kinesis_shards[1].sequence_number_range
    )
  end
end
