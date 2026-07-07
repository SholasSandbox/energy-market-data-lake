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

variable "create_dashboard_bucket" {
  description = "When true, Terraform creates the separate public/static dashboard bucket."
  type        = bool
  default     = false
}

variable "dashboard_bucket_name" {
  description = "Separate public/static dashboard bucket name used by Phase 8 publishing."
  type        = string
  default     = ""
}

variable "dashboard_cloudfront_enabled" {
  description = "When true, create CloudFront delivery for the Terraform-managed dashboard bucket. Requires create_dashboard_bucket = true."
  type        = bool
  default     = false
}

variable "dashboard_cloudfront_price_class" {
  description = "CloudFront price class for static dashboard delivery."
  type        = string
  default     = "PriceClass_100"

  validation {
    condition = contains([
      "PriceClass_100",
      "PriceClass_200",
      "PriceClass_All",
    ], var.dashboard_cloudfront_price_class)
    error_message = "dashboard_cloudfront_price_class must be PriceClass_100, PriceClass_200, or PriceClass_All."
  }
}

variable "ai_orchestration_enabled" {
  description = "Whether Terraform creates the Phase 8 AI insight orchestration resources."
  type        = bool
  default     = false
}

variable "ai_orchestration_lambda_function_name" {
  description = "Lambda function name for Phase 8 deterministic AI insight orchestration."
  type        = string
  default     = "energy-market-news-ai-orchestration"
}

variable "ai_orchestration_lambda_package_path" {
  description = "Path to the built Phase 8 Lambda zip package. Run scripts/build_phase8_lambda_package.sh before terraform plan/apply."
  type        = string
  default     = ".terraform/build/news_ai_orchestration.zip"
}

variable "ai_orchestration_lambda_timeout_seconds" {
  description = "Timeout in seconds for the Phase 8 orchestration Lambda."
  type        = number
  default     = 120
}

variable "ai_orchestration_lambda_memory_size" {
  description = "Memory size in MB for the Phase 8 orchestration Lambda."
  type        = number
  default     = 512
}

variable "ai_orchestration_log_retention_days" {
  description = "CloudWatch Logs retention for Phase 8 orchestration logs."
  type        = number
  default     = 14
}

variable "ai_orchestration_state_machine_name" {
  description = "Step Functions state machine name for Phase 8 AI insight orchestration."
  type        = string
  default     = "energy-market-ai-insight-orchestration"
}

variable "ai_orchestration_schedule_expression" {
  description = "EventBridge schedule expression for Phase 8 orchestration."
  type        = string
  default     = "cron(30 7 * * ? *)"
}

variable "ai_orchestration_schedule_enabled" {
  description = "Whether the Phase 8 EventBridge schedule is enabled."
  type        = bool
  default     = false
}

variable "ai_orchestration_dashboard_data_key" {
  description = "S3 key in the data lake bucket for the dashboard-data.json input artifact."
  type        = string
  default     = "dashboard/dashboard-data.json"
}

variable "ai_orchestration_news_limit_per_feed" {
  description = "Maximum RSS articles read per configured feed during Phase 8 orchestration."
  type        = number
  default     = 4
}

variable "ai_orchestration_news_max_articles" {
  description = "Maximum curated news articles retained in the Phase 8 news summary."
  type        = number
  default     = 18
}

variable "ai_orchestration_feeds" {
  description = "RSS feeds used by the deterministic Phase 8 news ingestion step."
  type        = list(string)
  default = [
    "https://www.energyvoice.com/feed/",
    "https://www.energylivenews.com/feed/",
    "https://www.power-technology.com/feed/",
    "https://www.offshore-energy.biz/feed/",
    "https://oilprice.com/rss/main",
    "https://www.renewableenergyworld.com/feed/",
    "https://www.pv-magazine.com/feed/",
  ]
}

variable "ai_orchestration_sns_email" {
  description = "Optional email address subscribed to Phase 8 orchestration failure notifications. Leave blank to create the topic without an email subscription."
  type        = string
  default     = ""
}

variable "ai_orchestration_managed_ai_enabled" {
  description = "When true, route the AI orchestration workflow through the managed Bedrock AI merge step. Keep false until an explicit deployment boundary."
  type        = bool
  default     = false
}

variable "ai_orchestration_bedrock_model_id" {
  description = "Bedrock model ID used by the managed AI orchestration path."
  type        = string
  default     = "mistral.ministral-3-8b-instruct"
}

variable "ai_orchestration_bedrock_model_arn" {
  description = "Optional explicit Bedrock foundation model ARN for least-privilege InvokeModel permission. Leave blank to derive from region and model ID."
  type        = string
  default     = ""
}

variable "ai_orchestration_bedrock_provider" {
  description = "Managed AI provider request shape used by the Bedrock adapter."
  type        = string
  default     = "mistral"

  validation {
    condition     = contains(["anthropic", "mistral"], var.ai_orchestration_bedrock_provider)
    error_message = "ai_orchestration_bedrock_provider must be anthropic or mistral."
  }
}

variable "ai_orchestration_bedrock_max_tokens" {
  description = "Maximum output tokens for the managed AI Bedrock invocation."
  type        = number
  default     = 1600

  validation {
    condition     = var.ai_orchestration_bedrock_max_tokens >= 1 && var.ai_orchestration_bedrock_max_tokens <= 4096
    error_message = "ai_orchestration_bedrock_max_tokens must be between 1 and 4096."
  }
}

variable "ai_orchestration_bedrock_temperature" {
  description = "Temperature for the managed AI Bedrock invocation."
  type        = number
  default     = 0.2

  validation {
    condition     = var.ai_orchestration_bedrock_temperature >= 0 && var.ai_orchestration_bedrock_temperature <= 1
    error_message = "ai_orchestration_bedrock_temperature must be between 0 and 1."
  }
}

variable "managed_workflow_cost_budget_enabled" {
  description = "When true, create a monthly cost budget for the managed workflow's project-service cost guardrail."
  type        = bool
  default     = false
}

variable "managed_workflow_cost_budget_name" {
  description = "Name for the managed workflow monthly cost budget."
  type        = string
  default     = "energy-market-managed-workflow-monthly-cost"
}

variable "managed_workflow_cost_budget_limit_usd" {
  description = "Monthly USD limit for the managed workflow project-service cost budget."
  type        = number
  default     = 1

  validation {
    condition     = var.managed_workflow_cost_budget_limit_usd > 0
    error_message = "managed_workflow_cost_budget_limit_usd must be greater than zero."
  }
}

variable "managed_workflow_cost_budget_notification_email" {
  description = "Optional email address subscribed to managed workflow cost budget notifications."
  type        = string
  default     = ""
}

variable "managed_workflow_cost_budget_services" {
  description = "AWS service names included in the managed workflow cost budget filter."
  type        = list(string)
  default = [
    "Amazon Bedrock",
    "AWS Lambda",
    "AWS Step Functions",
    "Amazon Simple Storage Service",
    "Amazon CloudFront",
    "Amazon Simple Notification Service",
    "AmazonCloudWatch",
  ]
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

variable "enable_energy_specific_crawlers" {
  description = "When true, create additional source- and dataset-specific Glue crawlers for the energy market lakehouse. Default false keeps live resource creation as an explicit boundary."
  type        = bool
  default     = false
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

variable "athena_query_role_name" {
  description = "IAM role name for bounded read-only lakehouse queries through the dedicated Athena workgroup."
  type        = string
  default     = "energy-market-athena-query-role"
}

variable "tags" {
  description = "Tags applied to supported resources."
  type        = map(string)
  default     = {}
}
