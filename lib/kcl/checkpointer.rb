class Kcl::Checkpointer
  DYNAMO_DB_LEASE_PRIMARY_KEY = 'shard_id'
  DYNAMO_DB_LEASE_OWNER_KEY   = 'assigned_to'
  DYNAMO_DB_LEASE_TIMEOUT_KEY = 'lease_timeout'
  DYNAMO_DB_CHECKPOINT_SEQUENCE_NUMBER_KEY = 'checkpoint'
  DYNAMO_DB_PARENT_SHARD_KEY  = 'parent_shard_id'

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

  def fetch_lease
  end

  def remove_lease(shard_id)
    result = @dynamodb.remove_item(
      @table_name,
      { DYNAMO_DB_LEASE_PRIMARY_KEY: shard_id }
    )
    if result
      Kcl.logger.info("Remove lease for shard at #{shard_id}")
    else
      Kcl.logger.info("Failed to remove lease for shard at #{shard_id}")
    end
  end

  def remove_lease_owner(shard_id)
  end

  def sequence
  end

  def fetch_checkpoint
  end
end
