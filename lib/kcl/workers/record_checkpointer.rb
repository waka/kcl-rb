module Kcl::Workers
  class RecordCheckpointer
    def initialize(shard, checkpointer)
      @shard = shard
      @checkpointer = checkpointer
    end

    def update_checkpoint(sequence_number)
      # checkpoint the last sequence of a closed shard
      @shard.checkpoint = sequence_number || Kcl::Checkpoints::Sentinel::SHARD_END
      @checkpointer.update_checkpoint(@shard)
    end
  end
end
