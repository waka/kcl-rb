module Kcl::Workers
  class RecordCheckpointer
    def initialize(shard, checkpointer)
      @shard = shard
      @checkpointer = checkpointer
    end

    def update_checkpoint(sequence_number)
      # checkpoint the last sequence of a closed shard
      if sequence_number.nil?
        @shard.checkpoint = Kcl::Checkpoints::Sentinel::SHARD_END
      else
        @shard.checkpoint = sequence_number
      end

      @checkpointer.update_checkpoint(@shard)
    end
  end
end
