require 'json'
require 'securerandom'

require_relative './kcl_demo/demo_record_processor'
require_relative './kcl_demo/demo_record_processor_factory'

module KclDemo
  class App
    def self.initialize
      Kcl.configure do |config|
        config.aws_region = 'ap-northeast-1'
        config.aws_access_key_id = 'dummy'
        config.aws_secret_access_key = 'dummy'
        config.dynamodb_endpoint = 'https://localhost:4566'
        config.dynamodb_table_name = 'kcl-rb-demo'
        config.kinesis_endpoint = 'https://localhost:4566'
        config.kinesis_stream_name = 'kcl-rb-demo'
        config.use_ssl = false
      end
    end

    def self.config
      Kcl.config
    end

    def self.run
      factory = KclDemo::DemoRecordProcessorFactory.new
      Kcl::Worker.run('kcl-demo', factory)
    end

    def self.seed(record_count = 1000)
      proxy = Kcl::Proxies::KinesisProxy.new(config)

      # puts records
      record_count.times do |i|
        str = SecureRandom.alphanumeric
        hash = JSON.generate({ id: i, name: str })
        resp = proxy.put_record(
          {
            stream_name: config.kinesis_stream_name,
            data: Base64.strict_encode64(hash),
            partition_key: str
          }
        )
        puts resp
      end
    end
  end
end
