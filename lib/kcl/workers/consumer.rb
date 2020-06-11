module Kcl::Workers
  class Consumer
    def initialize(record_processor)
      @record_processor = record_processor
    end

    def consume!(shard, kinesis, checkpointer)
      initialize_input = create_initialize_input(shard)
      @record_processor.after_initialize(initialize_input)

      result = kinesis.get_records(shard_iterator)
      records_input = create_records_input(result[:records], result[:millis_behind_latest])
      @record_processor.process_records(records_input)

      shutdown_input = create_shutdown_input(reason, record_checkpointer)
      @record_processor.shutdown(shutdown_input)
    end

    def create_initialize_input(shard)
      Kcl::Types::InitializationInput.new(
        shard.shard_id,
        Kcl::Types::ExtendedSequenceNumber.new(shard.checkpoint)
      )
    end

    def create_records_input(records, millis_behind_latest)
      Kcl::Types::RecordsInput.new(
        records,
        millis_behind_latest
      )
    end

    def create_shutdown_input(shutdown_reason, record_checkpointer)
      Kcl::Types::ShutdownInput.new(
        shutdown_reason,
        record_checkpointer
      )
    end
  end
end
