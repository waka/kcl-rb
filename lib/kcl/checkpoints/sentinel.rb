# Enumeration of the sentinel values of checkpoints.
# Used during initialization of ShardConsumers to determine the starting point
# in the shard and to flag that a shard has been completely processed.
module Kcl::Checkpoints
  module Sentinel
    # Start from the first available record in the shard.
    TRIM_HORIZON = 'TRIM_HORIZON'
    # Start from the latest record in the shard.
    LATEST       = 'LATEST'
    # We've completely processed all records in this shard.
    SHARD_END    = 'SHARD_END'
    # Start from the record at or after the specified server-side timestamp.
    AT_TIMESTAMP = 'AT_TIMESTAMP'
  end
end
