output "aws_region" {
  description = "AWS region used by this Terraform root."
  value       = var.aws_region
}

output "data_bucket_name" {
  description = "Data lake bucket used by Lambda, Glue, and Athena."
  value       = local.data_bucket_name
}

output "lambda_function_name" {
  description = "Ingestion Lambda function name."
  value       = aws_lambda_function.ingest.function_name
}

output "lambda_role_name" {
  description = "Lambda execution role name."
  value       = aws_iam_role.lambda.name
}

output "glue_role_name" {
  description = "Glue execution role name."
  value       = aws_iam_role.glue.name
}

output "glue_database_name" {
  description = "Glue catalog database name."
  value       = aws_glue_catalog_database.lakehouse.name
}

output "raw_crawler_name" {
  description = "Raw Glue crawler name."
  value       = aws_glue_crawler.raw.name
}

output "curated_crawler_name" {
  description = "Curated Glue crawler name."
  value       = aws_glue_crawler.curated.name
}

output "energy_specific_crawler_names" {
  description = "Optional energy-specific Glue crawler names keyed by source or curated dataset."
  value       = { for key, crawler in aws_glue_crawler.energy_specific : key => crawler.name }
}

output "glue_job_name" {
  description = "Glue ETL job name."
  value       = aws_glue_job.raw_to_parquet.name
}

output "athena_workgroup_name" {
  description = "Athena workgroup name."
  value       = aws_athena_workgroup.lakehouse.name
}

output "athena_output_location" {
  description = "Athena query output location."
  value       = local.athena_output_location
}

output "athena_query_role_arn" {
  description = "ARN of the dedicated bounded Athena query role."
  value       = aws_iam_role.athena_query.arn
}

output "dashboard_bucket_name" {
  description = "Separate public/static dashboard bucket used by Phase 8 publishing."
  value       = local.dashboard_bucket_name
}

output "dashboard_cloudfront_distribution_id" {
  description = "CloudFront distribution ID for public-safe dashboard delivery when enabled."
  value       = try(aws_cloudfront_distribution.dashboard_static[0].id, null)
}

output "dashboard_cloudfront_domain_name" {
  description = "CloudFront domain name for public-safe dashboard delivery when enabled."
  value       = try(aws_cloudfront_distribution.dashboard_static[0].domain_name, null)
}

output "ai_orchestration_lambda_function_name" {
  description = "Phase 8 deterministic AI insight Lambda function name."
  value       = try(aws_lambda_function.ai_orchestration[0].function_name, null)
}

output "ai_orchestration_state_machine_arn" {
  description = "Phase 8 Step Functions state machine ARN."
  value       = try(aws_sfn_state_machine.ai_orchestration[0].arn, null)
}

output "ai_orchestration_failure_topic_arn" {
  description = "SNS topic ARN for Phase 8 orchestration failures."
  value       = try(aws_sns_topic.ai_orchestration_failures[0].arn, null)
}

output "managed_workflow_cost_budget_name" {
  description = "Managed workflow monthly cost budget name when enabled."
  value       = try(aws_budgets_budget.managed_workflow_cost[0].name, null)
}
