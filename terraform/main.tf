# ============================================
# FIXED main.tf
# AWS Provider & Core Resources
# ============================================

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ============================================
# S3 Bucket - Data Lake
# ============================================

resource "aws_s3_bucket" "data_lake" {
  bucket = var.bucket_name

  tags = {
    Name        = "Flight Data Lake"
    Project     = var.project_name
    Environment = var.environment
    CreatedBy   = "Terraform"
  }
}

# ✅ ADDED: Block public access (SECURITY)
resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Enable versioning
resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

# ✅ ADDED: Server-side encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# ============================================
# S3 Folders Structure
# ============================================

resource "aws_s3_object" "raw_folder" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "raw/"
  content = ""
}

resource "aws_s3_object" "processed_folder" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "processed/"
  content = ""
}

resource "aws_s3_object" "archive_folder" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "archive/"
  content = ""
}

resource "aws_s3_object" "analytics_folder" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "analytics/"
  content = ""
}

# ============================================
# DynamoDB Table - Real-time Data
# ============================================

# ============================================
# DynamoDB Table - TEMPORARY FIX (No Stream)
# Use this while we fix the provider version issue
# ============================================

resource "aws_dynamodb_table" "flights_realtime" {
  name           = "${var.project_name}-realtime-${var.environment}"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "flight_id"
  range_key      = "timestamp"

  attribute {
    name = "flight_id"
    type = "S"
  }

  attribute {
    name = "timestamp"
    type = "S"
  }

  ttl {
    attribute_name = "expiration_time"
    enabled        = true
  }

  tags = {
    Name        = "Flight Real-time Table"
    Project     = var.project_name
    Environment = var.environment
    CreatedBy   = "Terraform"
  }
}


# ============================================
# SQS Queue - Message Streaming
# ============================================

resource "aws_sqs_queue" "flight_queue" {
  name                      = var.queue_name
  delay_seconds             = 0
  max_message_size          = 262144
  message_retention_seconds = 1209600
  receive_wait_time_seconds = 20
  visibility_timeout_seconds = 300

  tags = {
    Name        = "Flight Queue"
    Project     = var.project_name
    Environment = var.environment
    CreatedBy   = "Terraform"
  }
}

# ============================================
# IAM Role - Lambda
# ============================================

resource "aws_iam_role" "lambda_role" {
  name = "lambda-${var.project_name}-consumer-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# Lambda basic execution role (for CloudWatch logs)
resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Lambda SQS access
resource "aws_iam_role_policy_attachment" "lambda_sqs" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSQSFullAccess"
}

# Lambda DynamoDB access
resource "aws_iam_role_policy_attachment" "lambda_dynamodb" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess"
}

# Lambda S3 access
resource "aws_iam_role_policy_attachment" "lambda_s3" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

# ============================================
# IAM Role - Glue (for batch processing)
# ============================================

resource "aws_iam_role" "glue_role" {
  name = "glue-${var.project_name}-job-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "glue_cloudwatch" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"
}

resource "aws_iam_role_policy_attachment" "glue_dynamodb" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess"
}

# ============================================
# CloudWatch Log Groups
# ============================================

resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${var.project_name}-consumer"
  retention_in_days = 7

  tags = {
    Name        = "Lambda Logs"
    Project     = var.project_name
    Environment = var.environment
    CreatedBy   = "Terraform"
  }
}

resource "aws_cloudwatch_log_group" "glue_logs" {
  name              = "/aws-glue/${var.project_name}-job"
  retention_in_days = 7

  tags = {
    Name        = "Glue Logs"
    Project     = var.project_name
    Environment = var.environment
    CreatedBy   = "Terraform"
  }
}

# ============================================
# Data Source - Current AWS Account
# ============================================

data "aws_caller_identity" "current" {}