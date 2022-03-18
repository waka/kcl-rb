require 'bundler/setup'
require 'kcl'

# load shared_contexts
Dir["#{__dir__}/supports/**/*.rb"].each { |f| require f }

RSpec.configure do |config|
  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  true
end

# use localstack
Kcl.configure do |config|
  config.dynamodb_endpoint = ENV["AWS_ENDPOINT"]
  config.dynamodb_table_name = 'kcl-rb-test'
  config.kinesis_endpoint = ENV["AWS_ENDPOINT"]
  config.kinesis_stream_name = 'kcl-rb-test'
  config.logger = Kcl::Logger.new('/dev/null')
end

proxy = Kcl::Proxies::KinesisProxy.new(Kcl.config)

begin
  proxy.client.create_stream({ stream_name: Kcl.config.kinesis_stream_name, shard_count: 5 })
  Kcl.logger.info(message: "stream created", stream_name: Kcl.config.kinesis_stream_name, shard_count: 5)
rescue Aws::Kinesis::Errors::ResourceInUseException
end
