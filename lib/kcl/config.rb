module Kcl
  class Config
    attr_accessor :dynamodb_endpoint,
      :dynamodb_table_name,
      :dynamodb_read_capacity,
      :dynamodb_write_capacity,
      :dynamodb_failover_seconds,
      :kinesis_endpoint,
      :kinesis_stream_name,
      :logger,
      :log_level,
      :max_lease_count,
      :worker_count

    # Set default values
    def initialize
      @dynamodb_endpoint       = nil
      @dynamodb_table_name     = nil
      @dynamodb_read_capacity  = 10
      @dynamodb_write_capacity = 10
      @dynamodb_failover_seconds = 10
      @kinesis_endpoint        = nil
      @kinesis_stream_name     = nil
      @logger                  = nil
      @max_lease_count         = 2
      @worker_count            = 2
    end
  end
end
