# Terraform Lakehouse

<!-- markdownlint-disable MD013 -->

This Terraform root recreates the serverless energy lakehouse resources used by the ENTSOG gas build:

- Lambda ingestion function and execution role
- EventBridge ingestion schedule
- Optional data lake S3 bucket
- Glue role, database, raw crawler, curated crawler, and ETL job
- Athena workgroup and query result location
- Phase 8 deterministic AI insight Lambda, Step Functions state machine,
  failure SNS topic, and optional dashboard snapshot bucket
- Optional Phase 12 CloudFront distribution for private S3 dashboard delivery

The Terraform state backend is intentionally separate from the data lake bucket.

Datastores in scope:

- S3 data lake bucket for raw, curated, failed, archive, and Athena result objects
- Glue Data Catalog for table and partition metadata
- CloudWatch Logs for operational logs
- S3 Terraform state bucket for durable Terraform state
- Optional separate S3 dashboard/static bucket for public-safe snapshot JSON
- Optional CloudFront distribution with Origin Access Control for private S3
  dashboard delivery
- Optional SSM Parameter Store or Secrets Manager for future secrets

## Backend

Use a Terraform remote backend with S3:

```bash
export AWS_REGION=eu-west-2
export TF_STATE_BUCKET=energy-market-terraform-state-464975959576-eu-west-2

aws s3api create-bucket \
  --bucket "${TF_STATE_BUCKET}" \
  --region "${AWS_REGION}" \
  --create-bucket-configuration LocationConstraint="${AWS_REGION}"

aws s3api put-public-access-block \
  --bucket "${TF_STATE_BUCKET}" \
  --public-access-block-configuration \
  "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

aws s3api put-bucket-versioning \
  --bucket "${TF_STATE_BUCKET}" \
  --versioning-configuration Status=Enabled

aws s3api put-bucket-encryption \
  --bucket "${TF_STATE_BUCKET}" \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
```

Then create the backend config:

```bash
cp backend.hcl.example backend.hcl
```

Edit `backend.hcl` so `bucket` points to a dedicated Terraform state bucket, for example:

```hcl
bucket       = "energy-market-terraform-state-464975959576-eu-west-2"
key          = "energy-market-data-lake/dev/terraform.tfstate"
region       = "eu-west-2"
encrypt      = true
use_lockfile = true
```

The backend bucket should have:

- Block Public Access
- Versioning
- Server-side encryption
- Least-privilege IAM access

Do not use the raw/curated data lake bucket for Terraform state.

## Variables

Start from the example:

```bash
cp terraform.tfvars.example terraform.tfvars
```

The data bucket is optional:

```hcl
create_data_bucket = false
data_bucket_name   = "energy-market-lake-464975959576-20260405"
```

Use this mode when the bucket already exists. To create a fresh data bucket:

```hcl
create_data_bucket = true
data_bucket_name   = "energy-market-lake-<account-or-unique-suffix>"
```

Avoid angle-bracket placeholders in your shell commands; replace them in the file first.

Phase 8 orchestration is optional and disabled by default:

```hcl
create_dashboard_bucket = false
dashboard_bucket_name   = "energy-market-dashboard-public-464975959576-20260405"

dashboard_cloudfront_enabled     = false
dashboard_cloudfront_price_class = "PriceClass_100"

ai_orchestration_enabled             = false
ai_orchestration_schedule_enabled    = false
ai_orchestration_dashboard_data_key  = "dashboard/dashboard-data.json"
ai_orchestration_sns_email           = ""
```

Set `ai_orchestration_enabled = true` only after the Lambda zip exists and the
dashboard input JSON has been uploaded to the data lake bucket at
`ai_orchestration_dashboard_data_key`.

Set `dashboard_cloudfront_enabled = true` only when
`create_dashboard_bucket = true` and you are ready for Terraform to manage the
dashboard bucket and CloudFront delivery path. CloudFront uses Origin Access
Control, so the S3 bucket remains private and grants read access only to the
distribution.

Build the Phase 8 Lambda package before planning or applying orchestration
resources:

```bash
../../../scripts/build_phase8_lambda_package.sh
```

The build writes:

```text
.terraform/build/news_ai_orchestration.zip
```

The package includes `lambda/news_ai_orchestration.py`, the shared
`energy_market/` runtime code, `schemas/`, and Lambda-only Python dependencies
from `requirements-lambda.txt`.

## Commands

Initialize with the S3 backend:

```bash
terraform init -backend-config=backend.hcl
```

For syntax-only local validation without touching remote state:

```bash
terraform init -backend=false
terraform validate
```

Plan:

```bash
terraform plan -out tfplan
```

Apply:

```bash
terraform apply tfplan
```

Start one orchestration execution manually after apply:

```bash
aws stepfunctions start-execution \
  --state-machine-arn "$(terraform output -raw ai_orchestration_state_machine_arn)" \
  --region "$(terraform output -raw aws_region)"
```

Create a CloudFront invalidation after publishing new dashboard assets:

```bash
aws cloudfront create-invalidation \
  --distribution-id "$(terraform output -raw dashboard_cloudfront_distribution_id)" \
  --paths "/index.html" "/assets/*" "/dashboard-data.json" "/dashboard_snapshot_v1.sample.json"
```

## Existing Manual Resources

This Terraform root uses the current resource names by default, such as:

- `energy-market-elexon-ingest`
- `energy-market-lambda-role`
- `energy-market-glue-role`
- `energy-market-etl-raw-to-parquet`
- `energy-market-curated-crawler`

If those resources already exist in the target account, do one of the following before `terraform apply`:

- import the existing resources into Terraform state, or
- use a different `project_prefix` / resource names for a fresh parallel environment.

Examples for importing the existing resources:

```bash
terraform import aws_iam_role.lambda energy-market-lambda-role
terraform import aws_iam_role_policy_attachment.lambda_basic_execution 'energy-market-lambda-role/arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
terraform import aws_iam_role_policy.lambda_s3 'energy-market-lambda-role:energy-market-lambda-s3-policy'
terraform import aws_iam_role.glue energy-market-glue-role
terraform import aws_iam_role_policy_attachment.glue_service_role 'energy-market-glue-role/arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
terraform import aws_iam_role_policy.glue_s3 'energy-market-glue-role:energy-market-glue-s3-policy'
terraform import aws_cloudwatch_log_group.lambda /aws/lambda/energy-market-elexon-ingest
terraform import aws_lambda_function.ingest energy-market-elexon-ingest
terraform import aws_cloudwatch_event_rule.daily_ingestion energy-market-daily-ingestion
terraform import aws_cloudwatch_event_target.daily_ingestion energy-market-daily-ingestion/1
terraform import aws_lambda_permission.allow_eventbridge energy-market-elexon-ingest/energy-market-daily-ingestion-invoke
terraform import aws_s3_object.glue_script energy-market-lake-464975959576-20260405/scripts/etl_raw_to_parquet.py
terraform import aws_glue_catalog_database.lakehouse 464975959576:energy_market_lake
terraform import aws_glue_crawler.raw energy-market-raw-crawler
terraform import aws_glue_crawler.curated energy-market-curated-crawler
terraform import aws_glue_job.raw_to_parquet energy-market-etl-raw-to-parquet
```

Import addresses may change if you rename resources in the Terraform files. The full import runbook, preflight checks, optional data bucket imports, and expected first-plan drift are documented in `docs/terraform-import-checklist.md`.

## Gotchas During Rebuild Or Import

- **Terraform cannot create its own backend bucket in the same root.** Create the S3 state bucket first, then run `terraform init -backend-config=backend.hcl`.
- **Terraform state bucket and data lake bucket are different buckets.** Do not store Terraform state under the raw/curated data lake bucket.
- **Import does not generate Terraform code.** It only links an existing AWS resource to a Terraform address. Run `terraform plan` after each import batch and reconcile drift.
- **Existing data bucket is not managed when `create_data_bucket = false`.** Do not import `aws_s3_bucket.data_lake[0]` unless you intentionally switch to Terraform-managed bucket creation.
- **Changing `S3_BUCKET` requires IAM policy alignment.** Lambda and Glue may read the new bucket name from configuration but still fail S3 writes if their role policies point at an older bucket.
- **Do not enable the EventBridge schedule until manual validation passes.** Keep `schedule_enabled = false` while importing and reconciling resources.
- **Do not enable the Phase 8 schedule until one manual Step Functions execution passes.** Keep `ai_orchestration_schedule_enabled = false` while validating failure handling.
- **Build the Phase 8 Lambda package before planning enabled orchestration resources.** Terraform reads `.terraform/build/news_ai_orchestration.zip` when `ai_orchestration_enabled = true`.
- **The public dashboard snapshot is updated last.** The state machine publishes `dashboard_snapshot_v1.json` only after all validation gates pass; failed runs route to SNS/`failed/` and leave the previous public snapshot in place.
- **CloudFront dashboard delivery is opt-in.** Keep `dashboard_cloudfront_enabled = false` until the dashboard bucket is Terraform-managed and you are ready to publish static assets through CloudFront.
- **CloudFront Origin Access Control keeps the dashboard bucket private.** Do not loosen S3 public access settings to make the dashboard work; fix the CloudFront distribution or bucket policy instead.
- **Secrets can leak into state.** `entsoe_token` is sensitive, but Terraform state can still contain sensitive values. Prefer an empty token here until a Secrets Manager or SSM pattern is added.
- **Glue crawler names and table names can drift.** Confirm `curated_dataset_gas` points at the intended `s3://.../curated/dataset=gas/` location after any import or crawler run.
- **AWS-generated attributes may cause noisy plans.** Review `terraform plan` carefully before apply; adjust lifecycle rules only when the drift is harmless and understood.
- **The current account already has resources with these names.** Import them first, or use a different `project_prefix` for a parallel environment.

## Notes

- `entsoe_token` is sensitive. Prefer a local `terraform.tfvars` excluded from git or a secret delivery pipeline.
- `schedule_enabled` defaults to `false`; enable it after manual ingestion and ETL validation pass.
- The Glue job script is uploaded from `glue/etl_raw_to_parquet.py`.
- The Lambda deployment package is built from `lambda/ingest_elexon.py`.
- The Phase 8 Lambda deployment package is built by `scripts/build_phase8_lambda_package.sh`.
- The Phase 12 CloudFront distribution is optional and disabled by default.
