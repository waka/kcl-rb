require 'bundler/setup'
require 'pry'
require 'kcl'

# load shared_contexts
Dir["#{__dir__}/supports/**/*.rb"].each {|f| require f }

RSpec.configure do |config|
  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!
end

Kcl.configure do |config|
  config.aws_region = 'ap-northeast-1'
  config.aws_access_key_id = 'dummy'
  config.aws_secret_access_key = 'dummy'
  config.dynamodb_endpoint = 'https://localhost:4566'
  config.dynamodb_table_name = 'kcl-rb-demo'
  config.kinesis_endpoint = 'https://localhost:4566'
  config.kinesis_stream_name = 'kcl-rb-demo'
  config.logger = Kcl::Logger.new('/dev/null')
  config.use_ssl = false
end
