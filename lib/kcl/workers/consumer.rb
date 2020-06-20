module Kcl::Workers
  # Shard : Consumer = 1 : 1
  # - get records from stream
  # - send to record processor
  # - create record checkpoint
  class Consumer
    def initialize(shard, record_processor)
      @shard = shard
      @record_processor = record_processor
    end

    def consume!
      initialize_input = create_initialize_input
      @record_processor.after_initialize(initialize_input)

      result = kinesis.get_records(shard_iterator)
      records_input = create_records_input(
        result[:records],
        result[:millis_behind_latest]
      )
      @record_processor.process_records(records_input)

      shutdown_input = create_shutdown_input(
        Kcl::Workers::ShutdownReason::TERMINATE,
        'TODO'
      )
      @record_processor.shutdown(shutdown_input)
    end

    def shard_iterator
      kinesis.get_shard_iterator(@shard.shard_id)
    end

    def create_initialize_input
      Kcl::Types::InitializationInput.new(
        @shard.shard_id,
        Kcl::Types::ExtendedSequenceNumber.new(@shard.checkpoint)
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

    private

    def kinesis
      if @kinesis.nil?
        @kinesis = Kcl::Proxies::KinesisProxy.new(Kcl.config)
        Kcl.logger.info('Created Kinesis session')
      end
      @kinesis
    end

    def checkpointer
      if @checkpointer.nil?
        @checkpointer = Kcl::Checkpointer.new(Kcl.config)
        Kcl.logger.info('Created Checkpoint')
      end
      @checkpointer
    end
  end
end
