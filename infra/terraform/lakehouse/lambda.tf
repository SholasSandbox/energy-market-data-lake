data "archive_file" "ingest_lambda" {
  type        = "zip"
  source_file = "${path.module}/../../../lambda/ingest_elexon.py"
  output_path = "${path.module}/.terraform/build/ingest_elexon.zip"
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.lambda_function_name}"
  retention_in_days = var.lambda_log_retention_days
  tags              = local.common_tags
}

resource "aws_lambda_function" "ingest" {
  function_name    = var.lambda_function_name
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.11"
  handler          = "ingest_elexon.lambda_handler"
  filename         = data.archive_file.ingest_lambda.output_path
  source_code_hash = data.archive_file.ingest_lambda.output_base64sha256
  timeout          = var.lambda_timeout_seconds
  memory_size      = var.lambda_memory_size
  tags             = local.common_tags

  environment {
    variables = {
      BACKFILL_DAYS             = tostring(var.backfill_days)
      ELEXON_BASE_URL           = var.elexon_base_url
      ENTSOE_BASE_URL           = var.entsoe_base_url
      ENTSOE_TOKEN              = var.entsoe_token
      ENTSOE_ZONES              = var.entsoe_zones
      ENTSOG_BASE_URL           = var.entsog_base_url
      ENTSOG_DEMAND_INDICATOR   = var.entsog_demand_indicator
      ENTSOG_FLOW_INDICATOR     = var.entsog_flow_indicator
      ENTSOG_INCLUDE_EXEMPTIONS = tostring(var.entsog_include_exemptions)
      ENTSOG_LIMIT              = tostring(var.entsog_limit)
      ENTSOG_PERIOD_TYPE        = var.entsog_period_type
      ENTSOG_POINT_DIRECTIONS   = var.entsog_point_directions
      ENTSOG_TIMEZONE           = var.entsog_timezone
      HTTP_TIMEOUT_SECONDS      = tostring(var.http_timeout_seconds)
      S3_BUCKET                 = local.data_bucket_name
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.lambda,
    aws_iam_role_policy.lambda_s3,
    aws_iam_role_policy_attachment.lambda_basic_execution,
  ]
}

resource "aws_cloudwatch_event_rule" "daily_ingestion" {
  name                = "${var.project_prefix}-daily-ingestion"
  description         = "Daily energy market ingestion trigger."
  schedule_expression = var.schedule_expression
  state               = var.schedule_enabled ? "ENABLED" : "DISABLED"
  tags                = local.common_tags
}

resource "aws_cloudwatch_event_target" "daily_ingestion" {
  rule      = aws_cloudwatch_event_rule.daily_ingestion.name
  target_id = var.eventbridge_target_id
  arn       = aws_lambda_function.ingest.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "${var.project_prefix}-daily-ingestion-invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_ingestion.arn
}
