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
