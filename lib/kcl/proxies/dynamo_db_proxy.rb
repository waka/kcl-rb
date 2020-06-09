require 'aws-sdk-dynamodb'

module Kcl::Proxies
  class DynamoDbProxy
    attr_reader :client

    def initialize(config)
      @client = Aws::DynamoDB::Client.new({
        access_key_id: config.aws_access_key_id,
        secret_access_key: config.aws_secret_access_key,
        region: config.aws_region,
        endpoint: config.dynamodb_endpoint,
        ssl_verify_peer: config.use_ssl,
      })
    end

    def exists?(table_name)
      @client.describe_table({ table_name: table_name })
      true
    rescue Aws::DynamoDB::Errors::NotFound,
           Aws::DynamoDB::Errors::ResourceNotFoundException
      false
    end

    def create_table(table_name, attributes = [], schema = [], throughputs = {})
      @client.create_table({
        table_name: table_name,
        attribute_definitions: attributes,
        key_schema: schema,
        provisioned_throughput: throughputs
      })
    end

    def delete_table(table_name)
      @client.delete_table({ table_name: table_name })
      true
    rescue Aws::DynamoDB::Errors::ResourceNotFoundException
      false
    end

    def get_item(table_name, conditions)
      response = @client.get_item({
        table_name: table_name,
        key: conditions
      })
      response.item
    rescue Aws::DynamoDB::Errors::ResourceNotFoundException
      nil
    end

    def put_item(table_name, item)
      @client.put_item({
        table_name: table_name,
        item: item
      })
      true
    rescue Aws::DynamoDB::Errors::ResourceNotFoundException
      false
    end

    def remove_item(table_name, conditions)
      @client.delete_item({
        table_name: table_name,
        key: conditions
      })
      true
    rescue Aws::DynamoDB::Errors::ResourceNotFoundException
      false
    end
  end
end
