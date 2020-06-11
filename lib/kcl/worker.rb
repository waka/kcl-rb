require 'eventmachine'

class Kcl::Worker
  PROCESS_INTERVAL = 1 # by sec

  def self.run(id, record_processor_factory)
    worker = self.new(id, record_processor_factory)
    worker.start
  end

  def initialize(id, record_processor_factory)
    @id = id
    @record_processor_factory = record_processor_factory
    @live_shards  = {} # Map<String, Boolean>
    @shards       = {} # Map<String, Kcl::Workers::ShardInfo>
    @kinesis      = nil # Kcl::Proxies::KinesisProxy
    @checkpointer = nil # Kcl::Checkpointer
    @timer        = nil
  end

  # Start consuming data from the stream,
  # and pass it to the application record processors.
  def start
    Kcl.logger.info("Start worker at #{object_id}")

    setup_resources

    EM.run do
      trap_signals

      @timer = EM::PeriodicTimer.new(PROCESS_INTERVAL) do
        sync_shards!

        # Count the number of leases hold by worker excluding the processed shard
        counter = @shards.values.inject(0) do |num, shard|
          shard.lease_owner == @id && shard.completed? ? num + 1 : num
        end
        consume_shards! if Kcl.config.max_lease_count > counter
      end
    end

    cleanup
  rescue => ex
    Kcl.logger.error("#{ex.class}: #{ex.message}")
    raise ex
  end

  # Shutdown gracefully
  def shutdown(signal = :NONE)
    unless @timer.nil?
      @timer.cancel
      @timer = nil
    end
    EM.stop

    Kcl.logger.info("Shutdown worker with signal #{signal} at #{object_id}")
  rescue => ex
    Kcl.logger.error("#{ex.class}: #{ex.message}")
    raise ex
  end

  def setup_resources
    if instance_variable_get(:@kinesis).nil?
      @kinesis = Kcl::Proxies::KinesisProxy.new(Kcl.config)
      Kcl.logger.info('Created Kinesis session')
    end

    if instance_variable_get(:@checkpoint).nil?
      @checkpointer = Kcl::Checkpointer.new(Kcl.config)
      Kcl.logger.info('Created Checkpoint')
    end
  end

  def sync_shards!
    @live_shards.transform_values! {|_| false }

    @kinesis.get_shards.each do |shard|
      @live_shards[shard.shard_id] = true
      @shards[shard.shard_id] = Kcl::Workers::ShardInfo.new(
        shard.shard_id,
        shard.parent_shard_id,
        shard.sequence_number_range
      )
      Kcl.logger.info("Found new shard with #{shard.shard_id}")
    end

    @live_shards.each do |shard_id, alive|
      next if alive
      @shards.delete(shard_id)
      @checkpointer.remove_lease(shard_id)
    end
  end

  # Process records by shard
  def consume_shards!
    threads = []
    @shards.each do |shard_id, shard|
      threads << Thread.new do
        # already owner of the shard
        next if shard.lease_owner == @id

        shard.checkpoint = @checkpointer.fetch_checkpoint(shard_id)
        # shard is closed and processed all records
        next if shard.completed?

        record_processor = @record_processor_factory.create_processor
        process_records(record_processor, shard)
      end
    end
    threads.each(&:join)
  end

  # Process records by shard
  def process_records(record_processor, shard)
    shard.lease_owner = @id

    consumer = Kcl::Workers::Consumer.new(record_processor)
    consumer.consume(shard) do |records|
    end

    # cleanup
    shard.lease_owner = ''
    @checkpointer.remove_lease(shard_id)
  end

  # Cleanup resources
  def cleanup
    @live_shards  = {}
    @shards       = {}
    @kinesis      = nil
    @checkpointer = nil
  end

  private

  def trap_signals
    [:HUP, :INT, :TERM].each do |signal|
      trap signal do
        EM.add_timer(0) { shutdown(signal) }
      end
    end
  end
end
