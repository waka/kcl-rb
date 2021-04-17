# kcl-rb

## Overview

The Amazon Kinesis Client Library for Pure Ruby (Amazon KCL) enables Ruby developers to easily consume and process data from [Amazon Kinesis](http://aws.amazon.com/kinesis).

Already [KCL for Ruby](https://github.com/awslabs/amazon-kinesis-client-ruby) is provided by AWS, but Java is required for the operating environment because MultiLangDaemon is used.
**kcl-rb** is built on Pure Ruby, not depend on Java.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'kcl-rb'
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install kcl-rb

## Usage

It's okay if you develop it according to [the KCL specifications](https://docs.aws.amazon.com/streams/latest/dev/kinesis-record-processor-implementation-app-java.html).

### Implement the RecordProcessor

```rb
class RecordProcessor < Kcl::RecordProcessor
  def after_initialize(initialization_input)
    puts "SHARD_ID: #{initialization_input.shard_id}"
  end

  def process_records(records_input)
    puts "Current behind: #{records_input.millis_behind_latest}"
    records_input.records.each do |record|
      puts "Record: #{record}"
    end
  end

  def shutdown(shutdown_input)
    puts "Shutdown reason: #{shutdown_input.shutdown_reason}"

    if shutdown_input.shutdown_reason == Kcl::Workers::ShutdownReason::TERMINATE
      shutdown_input.record_checkpointer.update_checkpoint(nil)
    end
  end
end
```

### Implement a Class Factory for the RecordProcessor

```rb
class RecordProcessorFactory < Kcl::RecordProcessorFactory
  def create_processor
    RecordProcessor.new
  end
end
```

### Initialize KCL configurations

```rb
Kcl.configure do |config|
  config.dynamodb_endpoint = 'https://localhost:4566'
  config.dynamodb_table_name = 'kcl-rb'
  config.kinesis_endpoint = 'https://localhost:4566'
  config.kinesis_stream_name = 'kcl-rb'
end
```

If you want to see all the setting items, please see [config class file](https://github.com/waka/kcl-rb/blob/master/lib/kcl/config.rb).

### Run a Worker

```rb
worker_id = 'kcl-worker'
factory = RecordProcessorFactory.new
Kcl::Worker.run(worker_id, factory)
```

If you want more concrete example, look under [the demo directory](https://github.com/waka/kcl-rb/tree/master/demo).

## Development

### Prerequisites

- Install Ruby 2.7.1
- Install docker
- Install Terraform

### Build & Run for RSpec

Create Kinesis resources on localstack using Terraform

```sh
$ docker-compose up -d
$ cd terraform
$ terraform init
$ terraform apply
```

Build dependencies.

```
$ bundle install --path vendor/bundle
```

And run RSpec.

```sh
$ bundle exec rspec
```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/waka/kcl-rb.


## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
