require "eventmachine"
require "securerandom"

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
      Kcl.logger.info(message: "Start worker", object_id: object_id)

      EM.run do
        trap_signals

        @timer = EM::PeriodicTimer.new(PROCESS_INTERVAL) do
          Thread.current[:uuid] = SecureRandom.uuid
          sync_shards!
          rebalance_shards!
          cleanup_dead_consumers
          consume_shards!
        end
      end

      cleanup
      Kcl.logger.info(message: "Finish worker", object_id: object_id)
    rescue => e
      Kcl.logger.error(e)
      raise e
    end

    # Shutdown gracefully
    def shutdown(signal = :NONE)
      terminate_timer!
      terminate_consumers!

      EM.stop

      Kcl.logger.info(message: "Shutdown worker with signal #{signal} at #{object_id}")
    rescue => e
      Kcl.logger.error(e)
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
      Kcl.logger.info(message: "Stop #{@consumers.count} consumers in draining mode...")

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

        Kcl.logger.info(message: "Found new shard", shard:  shard.to_h)
      end

      @live_shards.each do |shard_id, alive|
        if alive
          begin
            @shards[shard_id] = checkpointer.fetch_checkpoint(@shards[shard_id])
          rescue Kcl::Errors::CheckpointNotFoundError
            Kcl.logger.warn(message: "Not found checkpoint of shard", shard: shard.to_h)
            next
          end
        else
          checkpointer.remove_lease(@shards[shard_id])
          @shards.delete(shard_id)
          Kcl.logger.info(message: "Remove shard", shard_id:  shard_id)
        end
      end

      @shards
    end

    def groups_stats
      @shards.group_by {|k, shard| shard.lease_owner }.transform_values {|v| v.map(&:first) }
    end

    def detailed_stats
      owner_stats = Hash.new(0)
      reassign_count = 0
      shards_count = 0
      workers = [@id]

      @shards.each do |shard_id, shard|
        next if shard.completed?

        shards_count += 1
        owner_stats[shard.lease_owner] += 1
        workers << shard.lease_owner unless shard.abendoned?
      end

      number_of_workers = workers.compact.uniq.count
      shards_per_worker = number_of_workers == 1 ? shards_count : (shards_count.to_f / number_of_workers).round

      {

        id: @id,
        owner_stats: owner_stats,
        groups_stats: groups_stats,
        number_of_workers: number_of_workers,
        shards_per_worker: shards_per_worker,
        shards_count: shards_count
      }
    end

    # Count the number of leases hold by worker excluding the processed shard
    def rebalance_shards!
      stats = detailed_stats
      reassign_count = 0

      unless @stats == stats
        @stats = stats
        Kcl.logger.info(message: "Rebalancing...", **stats)
      end

      @shards.each do |shard_id, shard|
        break if reassign_count >= stats[:shards_per_worker]
        next if shard.lease_owner == @id

        if shard.new_owner == @id
          reassign_count += 1

          if reassign_count >= stats[:shards_per_worker]
            break
          else
            next
          end
        end

        next if !shard.abendoned? && stats[:owner_stats][shard.assigned_to] <= stats[:shards_per_worker]

        Kcl.logger.debug(message: "Rebalance", shard: shard_id, from: shard.lease_owner, to: @id)

        @shards[shard_id] = checkpointer.ask_for_lease(shard, @id)
        reassign_count += 1
        stats[:owner_stats][shard.assigned_to] -= 1
      rescue Aws::DynamoDB::Errors::ConditionalCheckFailedException
        Kcl.logger.error(message: "Rebalance failed", shard: shard, to: @id)
        next
      end
    end

    def cleanup_dead_consumers
      @consumers.delete_if { |consumer| !consumer.alive? }
    end

    # Process records by shard
    def consume_shards!
      @shards.each do |shard_id, shard|
        # shard is closed and processed all records
        next if shard.completed?

        # the shard has owner already
        next unless shard.can_be_owned_by?(@id)

        # count the shard as consumed
        begin
          @shards[shard_id] = checkpointer.lease(shard, @id)
        rescue Aws::DynamoDB::Errors::ConditionalCheckFailedException
          Kcl.logger.warn(message: "Lease failed of shard", shard: shard.to_h)
          next
        end

        @consumers << Thread.new do
          Thread.current[:uuid] = SecureRandom.uuid
          consumer = Kcl::Workers::Consumer.new(
            shard,
            @record_processor_factory.create_processor,
            kinesis,
            checkpointer
          )
          consumer.consume!
        end
      end
    end

    private

    def kinesis
      if @kinesis.nil?
        @kinesis = Kcl::Proxies::KinesisProxy.new(Kcl.config)
        Kcl.logger.info(message: "Created Kinesis session in worker")
      end
      @kinesis
    end

    def checkpointer
      if @checkpointer.nil?
        @checkpointer = Kcl::Checkpointer.new(Kcl.config)
        Kcl.logger.info(message: "Created Checkpoint in worker")
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
