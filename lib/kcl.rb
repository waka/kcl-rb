require 'kcl/checkpointer'
require 'kcl/checkpoints/sentinel'
require 'kcl/config'
require 'kcl/errors'
require 'kcl/logger'
require 'kcl/proxies/dynamo_db_proxy'
require 'kcl/proxies/kinesis_proxy'
require 'kcl/record_processor'
require 'kcl/record_processor_factory'
require 'kcl/types/extended_sequence_number'
require 'kcl/types/initialization_input'
require 'kcl/types/records_input'
require 'kcl/types/shutdown_input'
require 'kcl/worker'
require 'kcl/workers/consumer'
require 'kcl/workers/shard_info'
require 'kcl/workers/shutdown_reason'

module Kcl
  def self.configure
    yield config
  end

  def self.config
    @_config ||= Kcl::Config.new
  end

  def self.logger
    @_logger ||= config.logger || Kcl::Logger.new($stdout)
  end
end
