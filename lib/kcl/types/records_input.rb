module Kcl
  module Types
    # Container for the parameters to the IRecordProcessor's method.
    class RecordsInput
      attr_reader :records, :millis_behind_latest, :record_checkpointer

      # @param [Array] records
      # @param [Number] millis_behind_latest
      # @param [Kcl::Workers::RecordCheckpointer] record_checkpointer
      def initialize(records, millis_behind_latest, record_checkpointer)
        @records              = records
        @millis_behind_latest = millis_behind_latest
        @record_checkpointer  = record_checkpointer
      end
    end
  end
end
