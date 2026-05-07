# Terraform Import Checklist

This checklist imports the existing manually-created AWS resources into the Terraform root at:

```text
infra/terraform/lakehouse/
```

Use this path when you want Terraform to adopt the current serverless lakehouse rather than creating a fresh parallel environment.

## Current AWS Snapshot

Observed in `eu-west-2` on `2026-05-06`:

| Resource | Current AWS state | Terraform action |
| --- | --- | --- |
| Data lake S3 bucket | `energy-market-lake-464975959576-20260405` exists | Referenced by Terraform when `create_data_bucket = false`; do not import unless you intentionally switch to Terraform-managed bucket ownership. |
| Lambda | `energy-market-elexon-ingest` exists and uses bucket `energy-market-lake-464975959576-20260405` | Import. |
| Lambda role | `energy-market-lambda-role` exists | Import role, inline S3 policy, and AWS managed policy attachment. |
| Lambda log group | `/aws/lambda/energy-market-elexon-ingest` exists with no retention configured | Import; first plan should set retention to `14` days unless you change `lambda_log_retention_days`. |
| EventBridge rule | `energy-market-daily-ingestion` exists and is currently `ENABLED` | Import; Terraform default is `schedule_enabled = false`, so first plan should disable it unless you set `schedule_enabled = true`. |
| EventBridge target | Rule target ID is `1` | Import using `energy-market-daily-ingestion/1`. |
| Lambda invoke permission | Statement ID is `energy-market-daily-ingestion-invoke` | Import using `energy-market-elexon-ingest/energy-market-daily-ingestion-invoke`. |
| Glue role | `energy-market-glue-role` exists | Import role, inline S3 policy, and AWS managed policy attachment. |
| Glue database | `energy_market_lake` exists | Import using `<account_id>:energy_market_lake`. |
| Raw crawler | `energy-market-raw-crawler` exists; currently targets the older `20260504` bucket | Import; first plan should retarget it to the Terraform `data_bucket_name`. |
| Curated crawler | `energy-market-curated-crawler` exists and targets the gas proof bucket | Import. |
| Glue job | `energy-market-etl-raw-to-parquet` exists and uses the gas proof bucket script | Import. |
| Glue ETL script object | `s3://energy-market-lake-464975959576-20260405/scripts/etl_raw_to_parquet.py` exists | Import because Terraform manages this object upload. |
| Athena workgroup | `energy-market-workgroup` does not currently exist | Do not import. Terraform can create it on apply, or you can change variables to match an existing workgroup. |

## Preconditions

Run from the repo root:

```bash
cd /Users/[redacted-user]/Workspace/cloud-projects/energy-market-data-lake
git status --short
```

Then enter the Terraform root:

```bash
cd infra/terraform/lakehouse
export AWS_REGION=eu-west-2
export ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
export S3_BUCKET=energy-market-lake-464975959576-20260405
```

Create local Terraform config files if they do not already exist:

```bash
cp backend.hcl.example backend.hcl
cp terraform.tfvars.example terraform.tfvars
```

Confirm `terraform.tfvars` is aligned with the existing bucket:

```hcl
create_data_bucket = false
data_bucket_name   = "energy-market-lake-464975959576-20260405"
schedule_enabled   = false
```

Initialize Terraform with the remote S3 backend:

```bash
terraform init -backend-config=backend.hcl
```

## Read-Only Preflight

Use these commands before importing so you know exactly what Terraform is adopting:

```bash
aws lambda get-function-configuration \
  --function-name energy-market-elexon-ingest \
  --region "${AWS_REGION}" \
  --query '{FunctionName:FunctionName,Runtime:Runtime,Role:Role,Timeout:Timeout,MemorySize:MemorySize,State:State,EnvBucket:Environment.Variables.S3_BUCKET}' \
  --output table

aws iam list-attached-role-policies \
  --role-name energy-market-lambda-role \
  --query 'AttachedPolicies[].PolicyArn' \
  --output table

aws iam list-role-policies \
  --role-name energy-market-lambda-role \
  --query 'PolicyNames' \
  --output table

aws iam list-attached-role-policies \
  --role-name energy-market-glue-role \
  --query 'AttachedPolicies[].PolicyArn' \
  --output table

aws iam list-role-policies \
  --role-name energy-market-glue-role \
  --query 'PolicyNames' \
  --output table

aws logs describe-log-groups \
  --log-group-name-prefix /aws/lambda/energy-market-elexon-ingest \
  --region "${AWS_REGION}" \
  --query 'logGroups[].{Name:logGroupName,Retention:retentionInDays}' \
  --output table

aws events describe-rule \
  --name energy-market-daily-ingestion \
  --region "${AWS_REGION}" \
  --query '{Name:Name,State:State,ScheduleExpression:ScheduleExpression,Arn:Arn}' \
  --output table

aws events list-targets-by-rule \
  --rule energy-market-daily-ingestion \
  --region "${AWS_REGION}" \
  --query 'Targets[].{Id:Id,Arn:Arn}' \
  --output table

aws glue get-crawler \
  --name energy-market-raw-crawler \
  --region "${AWS_REGION}" \
  --query '{Name:Crawler.Name,Role:Crawler.Role,DatabaseName:Crawler.DatabaseName,Targets:Crawler.Targets.S3Targets[].Path}' \
  --output table

aws glue get-crawler \
  --name energy-market-curated-crawler \
  --region "${AWS_REGION}" \
  --query '{Name:Crawler.Name,Role:Crawler.Role,DatabaseName:Crawler.DatabaseName,Targets:Crawler.Targets.S3Targets[].Path}' \
  --output table

aws glue get-job \
  --job-name energy-market-etl-raw-to-parquet \
  --region "${AWS_REGION}" \
  --query '{Name:Job.Name,Role:Job.Role,GlueVersion:Job.GlueVersion,WorkerType:Job.WorkerType,NumberOfWorkers:Job.NumberOfWorkers,Script:Job.Command.ScriptLocation}' \
  --output table

aws athena list-work-groups \
  --region "${AWS_REGION}" \
  --query 'WorkGroups[].Name' \
  --output table
```

## Import Commands

Run these from `infra/terraform/lakehouse`.

```bash
terraform import aws_iam_role.lambda energy-market-lambda-role

terraform import aws_iam_role_policy_attachment.lambda_basic_execution \
  'energy-market-lambda-role/arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'

terraform import aws_iam_role_policy.lambda_s3 \
  'energy-market-lambda-role:energy-market-lambda-s3-policy'

terraform import aws_cloudwatch_log_group.lambda \
  /aws/lambda/energy-market-elexon-ingest

terraform import aws_lambda_function.ingest \
  energy-market-elexon-ingest

terraform import aws_cloudwatch_event_rule.daily_ingestion \
  energy-market-daily-ingestion

terraform import aws_cloudwatch_event_target.daily_ingestion \
  energy-market-daily-ingestion/1

terraform import aws_lambda_permission.allow_eventbridge \
  energy-market-elexon-ingest/energy-market-daily-ingestion-invoke

terraform import aws_iam_role.glue energy-market-glue-role

terraform import aws_iam_role_policy_attachment.glue_service_role \
  'energy-market-glue-role/arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'

terraform import aws_iam_role_policy.glue_s3 \
  'energy-market-glue-role:energy-market-glue-s3-policy'

terraform import aws_s3_object.glue_script \
  "${S3_BUCKET}/scripts/etl_raw_to_parquet.py"

terraform import aws_glue_catalog_database.lakehouse \
  "${ACCOUNT_ID}:energy_market_lake"

terraform import aws_glue_crawler.raw \
  energy-market-raw-crawler

terraform import aws_glue_crawler.curated \
  energy-market-curated-crawler

terraform import aws_glue_job.raw_to_parquet \
  energy-market-etl-raw-to-parquet
```

Check state after the import batch:

```bash
terraform state list | sort
```

Expected imported addresses:

```text
aws_cloudwatch_event_rule.daily_ingestion
aws_cloudwatch_event_target.daily_ingestion
aws_cloudwatch_log_group.lambda
aws_glue_catalog_database.lakehouse
aws_glue_crawler.curated
aws_glue_crawler.raw
aws_glue_job.raw_to_parquet
aws_iam_role.glue
aws_iam_role.lambda
aws_iam_role_policy.glue_s3
aws_iam_role_policy.lambda_s3
aws_iam_role_policy_attachment.glue_service_role
aws_iam_role_policy_attachment.lambda_basic_execution
aws_lambda_function.ingest
aws_lambda_permission.allow_eventbridge
aws_s3_object.glue_script
```

## Optional Imports

### Athena Workgroup

Only import the named workgroup if it exists:

```bash
aws athena get-work-group \
  --work-group energy-market-workgroup \
  --region "${AWS_REGION}"

terraform import aws_athena_workgroup.lakehouse \
  energy-market-workgroup
```

As of the current snapshot, `energy-market-workgroup` does not exist, so Terraform should create it later.

Do not import the `primary` Athena workgroup into this resource unless you also change `athena_workgroup_name = "primary"` and deliberately decide Terraform should manage that existing AWS default workgroup.

### Data Lake Bucket Ownership

Skip this section while using:

```hcl
create_data_bucket = false
```

If you later decide Terraform should manage the existing data lake bucket itself, switch to `create_data_bucket = true`, then import the bucket and its child configuration resources:

```bash
terraform import 'aws_s3_bucket.data_lake[0]' \
  "${S3_BUCKET}"

terraform import 'aws_s3_bucket_public_access_block.data_lake[0]' \
  "${S3_BUCKET}"

terraform import 'aws_s3_bucket_server_side_encryption_configuration.data_lake[0]' \
  "${S3_BUCKET}"

terraform import 'aws_s3_bucket_versioning.data_lake[0]' \
  "${S3_BUCKET}"

terraform import 'aws_s3_bucket_lifecycle_configuration.data_lake[0]' \
  "${S3_BUCKET}"
```

Expect a larger plan after bucket import because Terraform will compare lifecycle, versioning, encryption, public access block, and tags against the configuration.

## Plan Review

After imports:

```bash
terraform fmt -recursive .
terraform validate
terraform plan -out tfplan
```

Review the plan carefully before applying. Expected first-plan changes may include:

- Tagging imported resources with `Environment`, `Project`, and `ManagedBy`.
- Setting Lambda log retention to `14` days.
- Disabling the EventBridge schedule because `schedule_enabled = false`.
- Retargeting the raw crawler from the older `20260504` bucket to the configured `20260405` data bucket.
- Creating `aws_athena_workgroup.lakehouse` because `energy-market-workgroup` does not currently exist.

Stop and investigate if Terraform wants to replace Lambda, IAM roles, Glue crawlers, or the Glue job. Those should usually be adopted in place.

## Recovery Commands

If you import the wrong address, remove only the Terraform state binding. This does not delete the AWS resource:

```bash
terraform state rm <terraform_address>
```

Example:

```bash
terraform state rm aws_cloudwatch_event_target.daily_ingestion
```

Then rerun the correct `terraform import` command.
