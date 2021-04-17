require 'aws-sdk-dynamodb'

module Kcl
  module Proxies
    class DynamoDbProxy
      attr_reader :client

      def initialize(config)
        @client = Aws::DynamoDB::Client.new(
          {
            access_key_id: config.aws_access_key_id,
            secret_access_key: config.aws_secret_access_key,
            region: config.aws_region,
            endpoint: config.dynamodb_endpoint,
            ssl_verify_peer: config.use_ssl
          }
        )
      end

      # @params [String] table_name
      def exists?(table_name)
        @client.describe_table({ table_name: table_name })
        true
      rescue Aws::DynamoDB::Errors::NotFound,
             Aws::DynamoDB::Errors::ResourceNotFoundException
        false
      end

      # @params [String] table_name
      # @params [Array]  attributes
      # @params [Array]  schema
      # @params [Hash]   throughputs
      def create_table(table_name, attributes = [], schema = [], throughputs = {})
        @client.create_table(
          {
            table_name: table_name,
            attribute_definitions: attributes,
            key_schema: schema,
            provisioned_throughput: throughputs
          }
        )
      end

      # @params [String] table_name
      def delete_table(table_name)
        @client.delete_table({ table_name: table_name })
        true
      rescue Aws::DynamoDB::Errors::ResourceNotFoundException
        false
      end

      # @params [String] table_name
      # @params [Hash]   conditions
      # @return [Hash]
      def get_item(table_name, conditions)
        response = @client.get_item(
          {
            table_name: table_name,
            key: conditions
          }
        )
        response.item
      rescue Aws::DynamoDB::Errors::ResourceNotFoundException
        nil
      end

      # @params [String]  table_name
      # @params [Hash]    item
      # @return [Boolean]
      def put_item(table_name, item)
        @client.put_item(
          {
            table_name: table_name,
            item: item
          }
        )
        true
      rescue Aws::DynamoDB::Errors::ResourceNotFoundException
        false
      end

      # @params [String]  table_name
      # @params [Hash]    conditions
      # @params [String]  update_expression
      # @return [Boolean]
      def update_item(table_name, conditions, update_expression)
        @client.update_item(
          {
            table_name: table_name,
            key: conditions,
            update_expression: update_expression
          }
        )
        true
      rescue Aws::DynamoDB::Errors::ResourceNotFoundException
        false
      end

      # @params [String]  table_name
      # @params [Hash]    item
      # @params [String]  condition_expression
      # @params [Hash]    expression_attributes
      # @return [Boolean]
      def conditional_update_item(table_name, item, condition_expression, expression_attributes)
        @client.put_item(
          {
            table_name: table_name,
            item: item,
            condition_expression: condition_expression,
            expression_attribute_values: expression_attributes
          }
        )
        true
      rescue Aws::DynamoDB::Errors::ResourceNotFoundException
        false
      end

      # @params [String]  table_name
      # @params [Hash]    conditions
      # @return [Boolean]
      def remove_item(table_name, conditions)
        @client.delete_item(
          {
            table_name: table_name,
            key: conditions
          }
        )
        true
      rescue Aws::DynamoDB::Errors::ResourceNotFoundException
        false
      end
    end
  end
end
