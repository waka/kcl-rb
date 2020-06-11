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

Put records to Kinesis stream

```
$ bundle exec rake seed
```

Run KCL application

```
$ bundle exec rake run
```
