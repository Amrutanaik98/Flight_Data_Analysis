# ============================================
# Lambda Function - SQS to DynamoDB Consumer
# ============================================

data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/lambda_function.py"
  output_path = "${path.module}/lambda_function.zip"
}

resource "aws_lambda_function" "flight_consumer" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = "${var.project_name}-consumer-${var.environment}"
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.11"
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = 60
  memory_size      = 256

  # Environment variables (REMOVED AWS_REGION)
  environment {
    variables = {
      DYNAMODB_TABLE_NAME = aws_dynamodb_table.flights_realtime.name
    }
  }

  tags = {
    Name        = "Flight Consumer Lambda"
    Project     = var.project_name
    Environment = var.environment
    CreatedBy   = "Terraform"
  }

  depends_on = [
    aws_iam_role_policy_attachment.lambda_basic,
    aws_iam_role_policy_attachment.lambda_sqs,
    aws_iam_role_policy_attachment.lambda_dynamodb
  ]
}

resource "aws_lambda_permission" "sqs_invoke_lambda" {
  statement_id  = "AllowExecutionFromSQS"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.flight_consumer.function_name
  principal     = "sqs.amazonaws.com"
  source_arn    = aws_sqs_queue.flight_queue.arn
}

resource "aws_lambda_event_source_mapping" "sqs_lambda_trigger" {
  event_source_arn = aws_sqs_queue.flight_queue.arn
  function_name    = aws_lambda_function.flight_consumer.function_name
  batch_size       = 5
  maximum_batching_window_in_seconds = 10

  tags = {
    Name        = "Flight SQS Lambda Trigger"
    Project     = var.project_name
    Environment = var.environment
    CreatedBy   = "Terraform"
  }

  depends_on = [aws_lambda_permission.sqs_invoke_lambda]
}