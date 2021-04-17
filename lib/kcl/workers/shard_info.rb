module Kcl
  module Workers
    class ShardInfo
      attr_reader :shard_id,
        :parent_shard_id,
        :starting_sequence_number,
        :ending_sequence_number
      attr_accessor :assigned_to, :checkpoint, :lease_timeout

      # @param [String] shard_id
      # @param [String] parent_shard_id
      # @param [Hash] sequence_number_range
      def initialize(shard_id, parent_shard_id, sequence_number_range)
        @shard_id        = shard_id
        @parent_shard_id = parent_shard_id || 0
        @starting_sequence_number = sequence_number_range[:starting_sequence_number]
        @ending_sequence_number   = sequence_number_range[:ending_sequence_number]
        @assigned_to     = nil
        @checkpoint      = nil
        @lease_timeout   = nil
      end

      def lease_owner
        @assigned_to
      end

      def lease_owner=(assigned_to)
        @assigned_to = assigned_to
      end

      def completed?
        @checkpoint == Kcl::Checkpoints::Sentinel::SHARD_END
      end

      # For debug
      def to_h
        {
          shard_id: shard_id,
          parent_shard_id: parent_shard_id,
          starting_sequence_number: starting_sequence_number,
          ending_sequence_number: ending_sequence_number,
          assigned_to: assigned_to,
          checkpoint: checkpoint,
          lease_timeout: lease_timeout
        }
      end
    end
  end
end
