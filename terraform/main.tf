#-------------------------------------------------------------------------------
# 開発環境 (LocalStack)
#-------------------------------------------------------------------------------

provider "aws" {
  version                     = "~> 2.60"
  access_key                  = "dummy"
  secret_key                  = "dummy"
  region                      = "ap-northeast-1"
  insecure                    = true
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  
  endpoints {
    dynamodb = "https://localhost:4566"
    kinesis  = "https://localhost:4566"
  }
}


#-------------------------------------------------------------------------------
# Kinesis stream
#-------------------------------------------------------------------------------

resource "aws_kinesis_stream" "kcl-rb-test_stream" {
  name             = "kcl-rb-test"
  shard_count      = 5
  retention_period = 24

  tags = {
    Environment = "test"
  }
}

