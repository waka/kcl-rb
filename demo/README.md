# kcl-rb Demo

## Build and Run

### For development

Run localstack container

```
$ docker-compose up
```

Create resource on localstack using Terraform

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
