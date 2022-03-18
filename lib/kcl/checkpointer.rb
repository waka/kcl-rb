require 'time'

module Kcl
  class Checkpointer
    DYNAMO_DB_LEASE_PRIMARY_KEY = 'shard_id'.freeze
    DYNAMO_DB_LEASE_OWNER_KEY   = 'assigned_to'.freeze
    DYNAMO_DB_LEASE_NEW_OWNER_KEY   = 'reassign_to'.freeze
    DYNAMO_DB_LEASE_TIMEOUT_KEY = 'lease_timeout'.freeze
    DYNAMO_DB_CHECKPOINT_SEQUENCE_NUMBER_KEY = 'checkpoint'.freeze
    DYNAMO_DB_PARENT_SHARD_KEY  = 'parent_shard_id'.freeze

    attr_reader :dynamodb

    # @param [Kcl::Config] config
    def initialize(config)
      @dynamodb = Kcl::Proxies::DynamoDbProxy.new(config)
      @table_name = config.dynamodb_table_name

      return if @dynamodb.exists?(@table_name)
      @dynamodb.create_table(
        @table_name,
        [{
          attribute_name: DYNAMO_DB_LEASE_PRIMARY_KEY,
          attribute_type: 'S'
        }],
        [{
          attribute_name: DYNAMO_DB_LEASE_PRIMARY_KEY,
          key_type: 'HASH'
        }],
        {
          read_capacity_units: config.dynamodb_read_capacity,
          write_capacity_units: config.dynamodb_write_capacity
        }
      )
      Kcl.logger.info(message: "Created DynamoDB table", table_name: @table_name)
    end

    # Retrieves the checkpoint for the given shard
    # @params [Kcl::Workers::ShardInfo] shard
    # @return [Kcl::Workers::ShardInfo]
    def fetch_checkpoint(shard)
      checkpoint = @dynamodb.get_item(
        @table_name,
        { "#{DYNAMO_DB_LEASE_PRIMARY_KEY}" => shard.shard_id }
      )
      return shard if checkpoint.nil?

      if checkpoint[DYNAMO_DB_CHECKPOINT_SEQUENCE_NUMBER_KEY]
        shard.checkpoint = checkpoint[DYNAMO_DB_CHECKPOINT_SEQUENCE_NUMBER_KEY]
      end
      if checkpoint[DYNAMO_DB_LEASE_OWNER_KEY]
        shard.assigned_to = checkpoint[DYNAMO_DB_LEASE_OWNER_KEY]
      end
      if checkpoint[DYNAMO_DB_LEASE_NEW_OWNER_KEY]
        shard.new_owner = checkpoint[DYNAMO_DB_LEASE_NEW_OWNER_KEY]
      end
      if checkpoint[DYNAMO_DB_LEASE_TIMEOUT_KEY]
        shard.lease_timeout = checkpoint[DYNAMO_DB_LEASE_TIMEOUT_KEY]
      end
      Kcl.logger.debug(message: "Retrieves checkpoint of shard", shard: shard.to_h)

      shard
    end

    # Write the checkpoint for the given shard
    # @params [Kcl::Workers::ShardInfo] shard
    # @return [Kcl::Workers::ShardInfo]
    def update_checkpoint(shard)
      item = {
        "#{DYNAMO_DB_LEASE_PRIMARY_KEY}" => shard.shard_id,
        "#{DYNAMO_DB_CHECKPOINT_SEQUENCE_NUMBER_KEY}" => shard.checkpoint,
        "#{DYNAMO_DB_LEASE_OWNER_KEY}" => shard.assigned_to,
        "#{DYNAMO_DB_LEASE_NEW_OWNER_KEY}" => shard.reassign_to,
        "#{DYNAMO_DB_LEASE_TIMEOUT_KEY}" => shard.lease_timeout
      }

      if shard.parent_shard_id != 0
        item[DYNAMO_DB_PARENT_SHARD_KEY] = shard.parent_shard_id
      end

      result = @dynamodb.put_item(@table_name, item)
      if result
        Kcl.logger.info(message: "Write checkpoint of shard", shard: shard.to_h)
      else
        Kcl.logger.warn(message: "Failed write checkpoint of shard", shard: shard.to_h)
      end

      shard
    end

    def ask_for_lease(shard, next_assigned_to)
      lease(shard, next_assigned_to, ask: true)
    end

    # Attempt to gain a lock on the given shard
    # @params [Kcl::Workers::ShardInfo] shard
    # @params [String] next_assigned_to
    # @return [Kcl::Workers::ShardInfo]
    def lease(shard, next_assigned_to, ask: false)
      now = Time.now.utc
      next_lease_timeout = now + Kcl.config.dynamodb_failover_seconds

      item_exists = @dynamodb.get_item(@table_name, { "#{DYNAMO_DB_LEASE_PRIMARY_KEY}" => shard.shard_id })

      if item_exists && shard.lease_timeout
        condition_expression = 'shard_id = :shard_id AND lease_timeout = :lease_timeout'
        expression_attributes = {
          ':shard_id' => shard.shard_id,
          ':lease_timeout' => shard.lease_timeout
        }
      elsif item_exists
        condition_expression = 'shard_id = :shard_id AND attribute_not_exists(lease_timeout)'
        expression_attributes = {
          ':shard_id' => shard.shard_id
        }
      else
        condition_expression = 'attribute_not_exists(lease_timeout)'
        expression_attributes = nil
      end

      item = {
        "#{DYNAMO_DB_LEASE_PRIMARY_KEY}" => shard.shard_id,
        "#{DYNAMO_DB_LEASE_OWNER_KEY}"   => next_assigned_to,
        "#{DYNAMO_DB_LEASE_TIMEOUT_KEY}" => next_lease_timeout.to_s
      }

      if ask
        item[DYNAMO_DB_LEASE_NEW_OWNER_KEY] = next_assigned_to
      end

      if shard.checkpoint != ''
        item[DYNAMO_DB_CHECKPOINT_SEQUENCE_NUMBER_KEY] = shard.checkpoint
      end

      if shard.parent_shard_id != 0
        item[DYNAMO_DB_PARENT_SHARD_KEY] = shard.parent_shard_id
      end

      result = @dynamodb.conditional_update_item(
        @table_name,
        item,
        condition_expression,
        expression_attributes
      )
      if result
        shard.assigned_to   = next_assigned_to
        shard.lease_timeout = next_lease_timeout.to_s
      else
        Kcl.logger.warn(mesage: "Failed to get lease for shard at", commit: commit, shard: shard.to_h)
      end

      shard
    end

    # Remove the shard entry
    # @params [Kcl::Workers::ShardInfo] shard
    # @return [Kcl::Workers::ShardInfo]
    def remove_lease(shard)
      result = @dynamodb.remove_item(
        @table_name,
        { "#{DYNAMO_DB_LEASE_PRIMARY_KEY}" => shard.shard_id }
      )
      if result
        shard.assigned_to   = nil
        shard.checkpoint    = nil
        shard.lease_timeout = nil
      else
        Kcl.logger.warn(message: "Failed to remove lease for shard", shard: shard.to_h)
      end

      shard
    end

    # Remove lease owner for the shard entry
    # @params [Kcl::Workers::ShardInfo] shard
    # @return [Kcl::Workers::ShardInfo]
    def remove_lease_owner(shard)
      result = @dynamodb.update_item(
        @table_name,
        { "#{DYNAMO_DB_LEASE_PRIMARY_KEY}" => shard.shard_id },
        "remove #{DYNAMO_DB_LEASE_OWNER_KEY}, #{DYNAMO_DB_LEASE_TIMEOUT_KEY}"
      )
      if result
        shard.assigned_to = nil
        shard.lease_timeout = nil
      else
        Kcl.logger.warn(message: "Failed to remove lease owner for shard", shard: shard.to_h)
      end

      shard
    end
  end
end
