module Kcl::Types
  # Container for the parameters to the RecordProcessor's method.
  class InitializationInput
    attr_reader :extended_sequence_number,
      :pending_checkpoint_sequence_number,
      :shard_id

    # @param [Kcl::Types::ExtendedSequenceNumber] extended_sequence_number
    # @param [Kcl::Types::ExtendedSequenceNumber] pending_checkpoint_sequence_number
    # @param [String] shard_id
    def initialize(extended_sequence_number, pending_checkpoint_sequence_number, shard_id)
      @extended_sequence_number           = extended_sequence_number
      @pending_checkpoint_sequence_number = pending_checkpoint_sequence_number
      @shard_id                           = shard_id
    end
  end
end
