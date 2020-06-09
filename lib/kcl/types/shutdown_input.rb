module Kcl::Types
  # Container for the parameters to the IRecordProcessor's method.
  class ShutdownInput
    attr_reader :check_pointer, :shutdown_reason

    # @param [Kcl::RecordProcessor::Checkpointer] check_pointer
    # @param [Kcl::Worker::ShutdownReason] shutdown_reason
    def initialize(check_pointer:, shutdown_reason:)
      @check_pointer   = check_pointer
      @shutdown_reason = shutdown_reason
    end
  end
end
