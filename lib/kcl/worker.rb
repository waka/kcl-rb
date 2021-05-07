require "eventmachine"

module Kcl
  class Worker
    PROCESS_INTERVAL = 2 # by sec

    def self.run(id, record_processor_factory)
      worker = self.new(id, record_processor_factory)
      worker.start
    end

    def initialize(id, record_processor_factory)
      @id = id
      @record_processor_factory = record_processor_factory
      @live_shards  = {} # Map<String, Boolean>
      @shards = {} # Map<String, Kcl::Workers::ShardInfo>
      @consumers = [] # [Array<Thread>] args the arguments passed from input. This array will be modified.
      @kinesis = nil # Kcl::Proxies::KinesisProxy
      @checkpointer = nil # Kcl::Checkpointer
      @timer = nil
    end

    # process 1                               process 2
    # kinesis.shards sync periodically
    # shards.start
    # go through shards, assign itself
    # on finish shard, release shard
    #
    # kinesis.shards sync periodically in parallel thread
    #

    # consumer should not block main thread
    # available_lease_shard? should divide all possible shards by worker ids

    # Start consuming data from the stream,
    # and pass it to the application record processors.
    def start
      Kcl.logger.info("Start worker at #{object_id}")

      EM.run do
        trap_signals

        @timer = EM::PeriodicTimer.new(PROCESS_INTERVAL) do
          Kcl.logger.info("threads count: #{@consumers.count}")
          sync_shards!
          consume_shards!
        end
      end

      cleanup
      Kcl.logger.info("Finish worker at #{object_id}")
    rescue => e
      Kcl.logger.error("#{e.class}: #{e.message}")
      raise e
    end

    # Shutdown gracefully
    def shutdown(signal = :NONE)
      terminate_timer!
      terminate_consumers!

      EM.stop

      Kcl.logger.info("Shutdown worker with signal #{signal} at #{object_id}")
    rescue => e
      Kcl.logger.error("#{e.class}: #{e.message}")
      raise e
    end

    # Cleanup resources
    def cleanup
      @live_shards = {}
      @shards = {}
      @kinesis = nil
      @checkpointer = nil
      @consumers = []
    end


    def terminate_consumers!
      Kcl.logger.info("Stop #{@consumers.count} consumers in draining mode...")

      # except main thread
      @consumers.each do |consumer|
        consumer[:stop] = true
        consumer.join
      end
    end

    def terminate_timer!
      unless @timer.nil?
        @timer.cancel
        @timer = nil
      end
    end

    # Add new shards and delete unused shards
    def sync_shards!
      @live_shards.transform_values! { |_| false }

      kinesis.shards.each do |shard|
        @live_shards[shard.shard_id] = true
        next if @shards[shard.shard_id]
        @shards[shard.shard_id] = Kcl::Workers::ShardInfo.new(
          shard.shard_id,
          shard.parent_shard_id,
          shard.sequence_number_range
        )
        Kcl.logger.info("Found new shard at shard_id: #{shard.shard_id}")
      end

      @live_shards.each do |shard_id, alive|
        next if alive
        checkpointer.remove_lease(@shards[shard_id])
        @shards.delete(shard_id)
        Kcl.logger.info("Remove shard at shard_id: #{shard_id}")
      end

      @shards
    end

    # Count the number of leases hold by worker excluding the processed shard
    def avaliable_leases_count
      stats = @shards.values.inject(Hash.new(0)) do |memo, shard|
        memo[shard.lease_owner] += 1 unless shard.completed?
        memo
      end

      Kcl.logger.info("stats: #{stats} #{@id}")
      number_of_workers = stats.keys.compact.push(@id).uniq.count
      shards_per_worker = @shards.count.to_f / number_of_workers

      return stats[nil] if number_of_workers == 1 # all free shards are available if there is single worker
      return 0 if stats[@id] >= shards_per_worker # no shards are available if current worker already took his portion of shards
      return stats[nil] if stats[nil] < 2 # if there are not to much free shards - take all of them

      [shards_per_worker.round - stats[@id], stats[nil]].min # how many free shards the worker can take
    end

    # Process records by shard
    def consume_shards!
      counter = 0
      @consumers.delete_if { |consumer| !consumer.alive? }

      @shards.each do |shard_id, shard|
        Kcl.logger.info("available: #{avaliable_leases_count}")
        # break if available_leases_count is not positive
        break if counter >= avaliable_leases_count

        # the shard has owner already
        next if shard.lease_owner.present?

        begin
          shard = checkpointer.fetch_checkpoint(shard)
        rescue Kcl::Errors::CheckpointNotFoundError
          Kcl.logger.info("Not found checkpoint of shard at #{shard.to_h}")
          next
        end

        # shard is closed and processed all records
        next if shard.completed?

        # count the shard as consumed
        begin
          shard = checkpointer.lease(shard, @id)
        rescue Aws::DynamoDB::Errors::ConditionalCheckFailedException
          Kcl.logger.info("Lease failed of shard at #{shard.to_h}")
          next
        end

        counter += 1

        @consumers << Thread.new do
          begin
            consumer = Kcl::Workers::Consumer.new(
              shard,
              @record_processor_factory.create_processor,
              kinesis,
              checkpointer
            )
            consumer.consume!
          ensure
            shard = checkpointer.remove_lease_owner(shard)
            Kcl.logger.info("Finish to consume shard at shard_id: #{shard_id}")
          end
        end
      end
    end

    private

    def kinesis
      if @kinesis.nil?
        @kinesis = Kcl::Proxies::KinesisProxy.new(Kcl.config)
        Kcl.logger.info('Created Kinesis session in worker')
      end
      @kinesis
    end

    def checkpointer
      if @checkpointer.nil?
        @checkpointer = Kcl::Checkpointer.new(Kcl.config)
        Kcl.logger.info('Created Checkpoint in worker')
      end
      @checkpointer
    end

    def trap_signals
      [:HUP, :INT, :TERM].each do |signal|
        trap signal do
          EM.add_timer(0) { shutdown(signal) }
        end
      end
    end
  end
end
