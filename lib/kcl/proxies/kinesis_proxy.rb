require 'aws-sdk-kinesis'

module Kcl::Proxies
  class KinesisProxy
    attr_reader :client

    def initialize(config)
      @client = Aws::Kinesis::Client.new(
        {
          access_key_id: config.aws_access_key_id,
          secret_access_key: config.aws_secret_access_key,
          region: config.aws_region,
          endpoint: config.kinesis_endpoint,
          ssl_verify_peer: config.use_ssl
        }
      )
      @stream_name = config.kinesis_stream_name
    end

    # @return [Array]
    def shards
      res = @client.describe_stream({ stream_name: @stream_name })
      res.stream_description.shards
    end

    # @param [String] shard_id
    # @param [String] shard_iterator_type
    # @return [String]
    def get_shard_iterator(shard_id, shard_iterator_type = nil, sequence_number = nil)
      params = {
        stream_name: @stream_name,
        shard_id: shard_id,
        shard_iterator_type: shard_iterator_type || Kcl::Checkpoints::Sentinel::LATEST
      }
      if shard_iterator_type == Kcl::Checkpoints::Sentinel::AFTER_SEQUENCE_NUMBER
        params[:starting_sequence_number] = sequence_number
      end
      res = @client.get_shard_iterator(params)
      res.shard_iterator
    end

    # @param [String] shard_iterator
    # @return [Hash]
    def get_records(shard_iterator)
      res = @client.get_records({ shard_iterator: shard_iterator })
      { records: res.records, next_shard_iterator: res.next_shard_iterator }
    end

    # @param [Hash] data
    # @return [Hash]
    def put_record(data)
      res = @client.put_record(data)
      { shard_id: res.shard_id, sequence_number: res.sequence_number }
    end
  end
end
