require 'time'

class Kcl::Checkpointer
  DYNAMO_DB_LEASE_PRIMARY_KEY = 'shard_id'
  DYNAMO_DB_LEASE_OWNER_KEY   = 'assigned_to'
  DYNAMO_DB_LEASE_TIMEOUT_KEY = 'lease_timeout'
  DYNAMO_DB_CHECKPOINT_SEQUENCE_NUMBER_KEY = 'checkpoint'
  DYNAMO_DB_PARENT_SHARD_KEY  = 'parent_shard_id'

  attr_reader :dynamodb

  # @param [Kcl::Config] config
  def initialize(config)
    @dynamodb = Kcl::Proxies::DynamoDbProxy.new(config)
    @table_name = config.dynamodb_table_name

    unless @dynamodb.exists?(@table_name)
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
      Kcl.logger.info("Created DynamoDB table: #{@table_name}")
    end
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
    Kcl.logger.info("Retrieves checkpoint of shard at #{shard.to_h}")

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
      "#{DYNAMO_DB_LEASE_TIMEOUT_KEY}" => shard.lease_timeout.to_s
    }
    if shard.parent_shard_id > 0
      item[DYNAMO_DB_PARENT_SHARD_KEY] = shard.parent_shard_id
    end

    result = @dynamodb.put_item(@table_name, item)
    if result
      Kcl.logger.info("Write checkpoint of shard at #{shard.to_h}")
    else
      Kcl.logger.info("Failed to write checkpoint for shard at #{shard.to_h}")
    end

    shard
  end

  # Attempt to gain a lock on the given shard
  # @params [Kcl::Workers::ShardInfo] shard
  # @params [String] next_assigned_to
  # @return [Kcl::Workers::ShardInfo]
  def lease(shard, next_assigned_to)
    now = Time.now.utc
    next_lease_timeout = now + Kcl.config.dynamodb_failover_seconds

    checkpoint = @dynamodb.get_item(
      @table_name,
      { "#{DYNAMO_DB_LEASE_PRIMARY_KEY}" => shard.shard_id }
    )
    assigned_to   = checkpoint && checkpoint[DYNAMO_DB_LEASE_OWNER_KEY]
    lease_timeout = checkpoint && checkpoint[DYNAMO_DB_LEASE_TIMEOUT_KEY]

    if assigned_to && lease_timeout
      if now > Time.parse(lease_timeout) && assigned_to != next_assigned_to
        raise Kcl::Errors::LeaseNotAquiredError
      end
      condition_expression = "shard_id = :shard_id AND assigned_to = :assigned_to AND lease_timeout = :lease_timeout"
      expression_attributes = {
        ':shard_id' => shard.shard_id,
        ':assigned_to' => assigned_to,
        ':lease_timeout' => lease_timeout
      }
      Kcl.logger.info("Attempting to get a lock for shard: #{shard.to_h}")
    else
      condition_expression = 'attribute_not_exists(assigned_to)'
      expression_attributes = nil
    end

    item = {
      "#{DYNAMO_DB_LEASE_PRIMARY_KEY}" => shard.shard_id,
      "#{DYNAMO_DB_LEASE_OWNER_KEY}"   => next_assigned_to,
      "#{DYNAMO_DB_LEASE_TIMEOUT_KEY}" => next_lease_timeout.to_s
    }
    if shard.checkpoint != ''
      item[DYNAMO_DB_CHECKPOINT_SEQUENCE_NUMBER_KEY] = shard.checkpoint
    end
    if shard.parent_shard_id > 0
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
      shard.lease_timeout = next_lease_timeout
      Kcl.logger.info("Get lease for shard at #{shard.to_h}")
    else
      Kcl.logger.info("Failed to get lease for shard at #{shard.to_h}")
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
      Kcl.logger.info("Remove lease for shard at #{shard.to_h}")
    else
      Kcl.logger.info("Failed to remove lease for shard at #{shard.to_h}")
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
      "remove #{DYNAMO_DB_LEASE_OWNER_KEY}"
    )
    if result
      shard.assigned_to = nil
      Kcl.logger.info("Remove lease owner for shard at #{shard.to_h}")
    else
      Kcl.logger.info("Failed to remove lease owner for shard at #{shard.to_h}")
    end

    shard
  end
end
