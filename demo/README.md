# kcl-rb Demo App

## Build and Run

Run localstack container (mock for Kinesis and DynamoDB).

```
$ docker-compose up
```

Create resources on localstack using Terraform

```
$ cd terraform
$ terraform init
$ terraform plan
$ terraform apply
```

Build dependencies

```
$ bundle install --path vendor/bundle
```

Run Demo KCL application

```
$ bundle exec rake run
```

Put records to Kinesis stream

```
$ RECORD_COUNT=10 bundle exec rake seed
```

You can see in console that the input data is distributed and processed by each consumer.
