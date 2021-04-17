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
  config.dynamodb_endpoint = 'https://localhost:4566'
  config.dynamodb_table_name = 'kcl-rb-test'
  config.kinesis_endpoint = 'https://localhost:4566'
  config.kinesis_stream_name = 'kcl-rb-test'
  config.logger = Kcl::Logger.new('/dev/null')
end
