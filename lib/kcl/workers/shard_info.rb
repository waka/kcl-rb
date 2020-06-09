module Kcl::Workers
  class ShardInfo
    attr_reader :shard_id,
      :parent_shard_ids,
      :starting_sequence_number,
      :ending_sequence_number

    # @param [String] shard_id
    # @param [String] parent_shard_id
    # @param [Hash] sequence_number_range
    def initialize(shard_id, parent_shard_id, sequence_number_range)
      @shard_id        = shard_id
      @parent_shard_id = parent_shard_id
      @starting_sequence_number = sequence_number_range[:starting_sequence_number]
      @ending_sequence_number   = sequence_number_range[:ending_sequence_number]
    end

    def lease_owner
      0
    end

    def completed?
      true
    end
  end
end
