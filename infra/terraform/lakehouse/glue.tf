resource "aws_s3_object" "glue_script" {
  bucket = local.data_bucket_name
  key    = local.glue_script_key
  source = "${path.module}/../../../glue/etl_raw_to_parquet.py"
  etag   = filemd5("${path.module}/../../../glue/etl_raw_to_parquet.py")
  tags   = local.common_tags
}

resource "aws_glue_catalog_database" "lakehouse" {
  name = var.glue_database_name
}

resource "aws_glue_crawler" "raw" {
  name          = var.raw_crawler_name
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.lakehouse.name
  table_prefix  = "raw_"
  tags          = local.common_tags

  s3_target {
    path = "s3://${local.data_bucket_name}/raw/"
  }
}

resource "aws_glue_crawler" "curated" {
  name          = var.curated_crawler_name
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.lakehouse.name
  table_prefix  = "curated_"
  tags          = local.common_tags

  s3_target {
    path = "s3://${local.data_bucket_name}/curated/"
  }
}

resource "aws_glue_crawler" "energy_specific" {
  for_each = var.enable_energy_specific_crawlers ? local.energy_specific_crawlers : {}

  name          = each.value.name
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.lakehouse.name
  table_prefix  = each.value.table_prefix
  tags = merge(
    local.common_tags,
    {
      DataDomain = each.value.data_domain
      Dataset    = each.value.dataset
    }
  )

  s3_target {
    path = each.value.path
  }
}

resource "aws_glue_job" "raw_to_parquet" {
  name              = var.glue_job_name
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  max_retries       = 0
  tags              = local.common_tags

  command {
    name            = "glueetl"
    script_location = local.glue_script_location
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--RAW_PATH"                         = local.raw_path
    "--CURATED_PATH"                     = local.curated_path
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  }

  depends_on = [
    aws_iam_role_policy.glue_s3,
    aws_iam_role_policy_attachment.glue_service_role,
    aws_s3_object.glue_script,
  ]
}
