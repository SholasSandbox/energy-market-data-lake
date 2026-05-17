locals {
  common_tags = merge(
    {
      Environment = var.environment
      Project     = var.project_prefix
      ManagedBy   = "terraform"
    },
    var.tags
  )

  phase8_tags = merge(
    local.common_tags,
    {
      Phase = "phase-8-ai-orchestration"
    }
  )

  phase12_tags = merge(
    local.common_tags,
    {
      Phase = "phase-12-dashboard-delivery"
    }
  )

  data_bucket_name             = var.data_bucket_name
  dashboard_bucket_name        = var.dashboard_bucket_name
  dashboard_cloudfront_enabled = var.create_dashboard_bucket && var.dashboard_cloudfront_enabled
  dashboard_origin_id          = "${var.project_prefix}-dashboard-static-origin"
  raw_path                     = "s3://${local.data_bucket_name}/raw"
  curated_path                 = "s3://${local.data_bucket_name}/curated"
  glue_script_key              = "scripts/etl_raw_to_parquet.py"
  glue_script_location         = "s3://${local.data_bucket_name}/${local.glue_script_key}"
  athena_results_prefix        = "athena-results/"
  athena_output_location       = "s3://${local.data_bucket_name}/${local.athena_results_prefix}"
}
