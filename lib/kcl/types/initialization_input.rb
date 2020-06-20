module Kcl::Types
  # Container for the parameters to the RecordProcessor's method.
  class InitializationInput
    attr_reader :shard_id, :extended_sequence_number

    # @param [String] shard_id
    # @param [Kcl::Types::ExtendedSequenceNumber] extended_sequence_number
    def initialize(shard_id, extended_sequence_number)
      @shard_id                 = shard_id
      @extended_sequence_number = extended_sequence_number
    end
  end
end
