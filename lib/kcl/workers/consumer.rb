module Kcl::Workers
  # Shard : Consumer = 1 : 1
  # - get records from stream
  # - send to record processor
  # - create record checkpoint
  class Consumer
    def initialize(shard, record_processor, kinesis_proxy, checkpointer)
      @shard = shard
      @record_processor = record_processor
      @kinesis = kinesis_proxy
      @checkpointer = checkpointer
    end

    def consume!
      initialize_input = create_initialize_input
      @record_processor.after_initialize(initialize_input)

      record_checkpointer = Kcl::Workers::RecordCheckpointer.new(@shard, @checkpointer)
      shard_iterator = start_shard_iterator

      loop do
        result = @kinesis.get_records(shard_iterator)

        records_input = create_records_input(
          result[:records],
          result[:millis_behind_latest],
          record_checkpointer
        )
        @record_processor.process_records(records_input)

        shard_iterator = result[:next_shard_iterator]
        break if result[:records].empty? || shard_iterator.nil?
      end

      shutdown_reason = shard_iterator.nil? ?
        Kcl::Workers::ShutdownReason::TERMINATE :
        Kcl::Workers::ShutdownReason::REQUESTED
      shutdown_input = create_shutdown_input(shutdown_reason, record_checkpointer)
      @record_processor.shutdown(shutdown_input)
    end

    def start_shard_iterator
      shard = @checkpointer.fetch_checkpoint(@shard)
      if shard.checkpoint.nil?
        return @kinesis.get_shard_iterator(
          @shard.shard_id,
          Kcl::Checkpoints::Sentinel::TRIM_HORIZON
        )
      end

      @kinesis.get_shard_iterator(
        @shard.shard_id,
        Kcl::Checkpoints::Sentinel::AFTER_SEQUENCE_NUMBER,
        @shard.checkpoint
      )
    end

    def create_initialize_input
      Kcl::Types::InitializationInput.new(
        @shard.shard_id,
        Kcl::Types::ExtendedSequenceNumber.new(@shard.checkpoint)
      )
    end

    def create_records_input(records, millis_behind_latest, record_checkpointer)
      Kcl::Types::RecordsInput.new(
        records,
        millis_behind_latest,
        record_checkpointer
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
