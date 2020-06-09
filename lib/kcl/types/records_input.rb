module Kcl::Types
  # Container for the parameters to the IRecordProcessor's method.
  class RecordsInput
    attr_reader :cache_entry_time,
      :cache_exit_time,
      :check_pointer,
      :millis_behind_latest,
      :records

    # @param [Time] cache_entry_time
    # @param [Time] cache_exit_time
    # @param [Kcl::RecordProcessor::Checkpointer] check_pointer
    # @param [Number] millis_behind_latest
    # @param [Array] records
    def initialize(cache_entry_time, cache_exit_time, check_pointer, millis_behind_latest, records)
      @cache_entry_time     = cache_entry_time
      @cache_exit_time      = cache_exit_time
      @check_pointer        = check_pointer
      @millis_behind_latest = millis_behind_latest
      @records              = records
    end

    # @return [Float]
    def time_spent_in_cache
      if cache_entry_time.nil? || cache_exit_time.nil?
        return 0
      end
      cache_exit_time - cache_entry_time
    end
  end
end
