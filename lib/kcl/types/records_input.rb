module Kcl::Types
  # Container for the parameters to the IRecordProcessor's method.
  class RecordsInput
    attr_reader :records, :millis_behind_latest

    # @param [Array] records
    # @param [Number] millis_behind_latest
    def initialize(records, millis_behind_latest)
      @records              = records
      @millis_behind_latest = millis_behind_latest
    end
  end
end
