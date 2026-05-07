variable "aws_region" {
  description = "AWS region for the lakehouse resources."
  type        = string
  default     = "eu-west-2"
}

variable "project_prefix" {
  description = "Prefix used for named AWS resources."
  type        = string
  default     = "energy-market"
}

variable "environment" {
  description = "Deployment environment label."
  type        = string
  default     = "dev"
}

variable "create_data_bucket" {
  description = "When true, Terraform creates the data lake bucket. When false, data_bucket_name must refer to an existing bucket."
  type        = bool
  default     = false
}

variable "data_bucket_name" {
  description = "Data lake bucket name. Required whether the bucket is created or existing."
  type        = string
}

variable "lambda_function_name" {
  description = "Lambda function name for ingestion."
  type        = string
  default     = "energy-market-elexon-ingest"
}

variable "lambda_memory_size" {
  description = "Lambda memory size in MB."
  type        = number
  default     = 256
}

variable "lambda_timeout_seconds" {
  description = "Lambda timeout in seconds."
  type        = number
  default     = 900
}

variable "lambda_log_retention_days" {
  description = "CloudWatch Logs retention for Lambda logs."
  type        = number
  default     = 14
}

variable "backfill_days" {
  description = "Default ingestion backfill window."
  type        = number
  default     = 1
}

variable "http_timeout_seconds" {
  description = "HTTP timeout used by the ingestion Lambda."
  type        = number
  default     = 30
}

variable "elexon_base_url" {
  description = "Elexon API base URL."
  type        = string
  default     = "https://data.elexon.co.uk/bmrs/api/v1"
}

variable "entsoe_base_url" {
  description = "ENTSO-E API base URL."
  type        = string
  default     = "https://web-api.tp.entsoe.eu/api"
}

variable "entsoe_token" {
  description = "ENTSO-E token. Prefer setting through a tfvars file excluded from git or a secret pipeline."
  type        = string
  default     = ""
  sensitive   = true
}

variable "entsoe_zones" {
  description = "Comma-separated ENTSO-E zones."
  type        = string
  default     = "GB,FR,DE,NL"
}

variable "entsog_base_url" {
  description = "ENTSOG API base URL."
  type        = string
  default     = "https://transparency.entsog.eu/api/v1"
}

variable "entsog_point_directions" {
  description = "Comma-separated ENTSOG pointDirection values."
  type        = string
  default     = "BE-TSO-0001ITP-00061entry,BE-TSO-0001ITP-00115exit,CZ-TSO-0001ITP-00537entry,BE-TSO-0001ITP-00555exit"
}

variable "entsog_flow_indicator" {
  description = "ENTSOG flow indicator."
  type        = string
  default     = "Physical Flow"
}

variable "entsog_demand_indicator" {
  description = "ENTSOG demand proxy indicator."
  type        = string
  default     = "Allocation"
}

variable "entsog_period_type" {
  description = "ENTSOG period type."
  type        = string
  default     = "day"
}

variable "entsog_timezone" {
  description = "ENTSOG request timezone."
  type        = string
  default     = "WET"
}

variable "entsog_limit" {
  description = "ENTSOG API result limit."
  type        = number
  default     = 1000
}

variable "entsog_include_exemptions" {
  description = "Whether ENTSOG requests include exemptions. Use 0 for the reproducible gas proof."
  type        = number
  default     = 0
}

variable "schedule_enabled" {
  description = "Whether the EventBridge ingestion schedule is enabled."
  type        = bool
  default     = false
}

variable "schedule_expression" {
  description = "EventBridge schedule expression for ingestion."
  type        = string
  default     = "cron(0 2 * * ? *)"
}

variable "eventbridge_target_id" {
  description = "EventBridge target ID for the Lambda ingestion target. The existing manually-created target currently uses 1."
  type        = string
  default     = "1"
}

variable "glue_database_name" {
  description = "Glue database name."
  type        = string
  default     = "energy_market_lake"
}

variable "raw_crawler_name" {
  description = "Glue raw crawler name."
  type        = string
  default     = "energy-market-raw-crawler"
}

variable "curated_crawler_name" {
  description = "Glue curated crawler name."
  type        = string
  default     = "energy-market-curated-crawler"
}

variable "glue_job_name" {
  description = "Glue ETL job name."
  type        = string
  default     = "energy-market-etl-raw-to-parquet"
}

variable "glue_worker_type" {
  description = "Glue worker type."
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Glue worker count."
  type        = number
  default     = 2
}

variable "athena_workgroup_name" {
  description = "Athena workgroup name."
  type        = string
  default     = "energy-market-workgroup"
}

variable "tags" {
  description = "Tags applied to supported resources."
  type        = map(string)
  default     = {}
}
