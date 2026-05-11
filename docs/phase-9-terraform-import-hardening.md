# Phase 9: Terraform Import And Operating Hardening

<!-- markdownlint-disable MD013 -->

Use this tracker to adopt the existing manually-created AWS lakehouse resources
into Terraform state, review drift, and harden the operating posture without
enabling schedules prematurely.

## Goal

Current state:

- Phase 8 AI insight orchestration resources are already managed in the S3
  Terraform backend.
- Older lakehouse resources exist in AWS but are not yet imported into this
  Terraform state.
- The Phase 8 EventBridge schedule is deployed and disabled.
- The older daily ingestion EventBridge rule is live and enabled.

Target state:

- Terraform state accurately represents the live AWS resources needed for the
  lakehouse and Phase 8 orchestration.
- Expected drift is either removed or documented.
- Phase 8 remains reproducible from Terraform.
- CloudWatch alarms are considered only after state is clean.
- Schedules remain intentionally disabled unless a later operating decision
  changes that.

## Branch

```text
phase9-terraform-import-hardening
```

This branch was created from `feature/aws-ai-insight-orchestration`. If Phase 8
is merged first, rebase this branch onto `main` before importing resources.

## Step 1 Status: Branch And Backend Preflight

Completed on 2026-05-11:

- Created branch `phase9-terraform-import-hardening`.
- Recreated local ignored Terraform config files:
  - `infra/terraform/lakehouse/backend.hcl`
  - `infra/terraform/lakehouse/terraform.tfvars`
- Initialized Terraform against the S3 backend:
  - bucket: `energy-market-terraform-state-464975959576-eu-west-2`
  - key: `energy-market-data-lake/phase8-ai-orchestration/terraform.tfstate`
  - region: `eu-west-2`
- Pulled a pre-import state backup to:
  - `/tmp/phase9-terraform-preflight/state-before-phase9.json`
- Ran `terraform validate`.
- Confirmed import map below.

Commands used:

```bash
cd /Users/[redacted-user]/Workspace/cloud-projects/energy-market-data-lake
git switch -c phase9-terraform-import-hardening

cd infra/terraform/lakehouse
terraform init -reconfigure -backend-config=backend.hcl
terraform state pull > /tmp/phase9-terraform-preflight/state-before-phase9.json
terraform state list | sort
terraform validate
```

Validation result:

```text
Success! The configuration is valid.
```

## Backend And Bucket Preflight

Confirmed:

| Bucket | State |
| --- | --- |
| `energy-market-terraform-state-464975959576-eu-west-2` | exists |
| `energy-market-lake-464975959576-20260405` | exists |
| `energy-market-dashboard-public-464975959576-20260511` | exists |

Confirmed schedules:

| EventBridge rule | Live state | Terraform intent |
| --- | --- | --- |
| `energy-market-daily-ingestion` | `ENABLED` | `schedule_enabled = false` |
| `energy-market-ai-orchestration-schedule` | `DISABLED` | `ai_orchestration_schedule_enabled = false` |

The daily ingestion rule is the first expected drift item. Phase 9 should decide
whether to preserve it as enabled or let Terraform disable it.

## Current Terraform State

These Phase 8 resources are already in Terraform state. Do not import them
again:

```text
aws_cloudwatch_event_rule.ai_orchestration_schedule[0]
aws_cloudwatch_event_target.ai_orchestration_schedule[0]
aws_cloudwatch_log_group.ai_orchestration_lambda[0]
aws_iam_role.ai_orchestration_eventbridge[0]
aws_iam_role.ai_orchestration_lambda[0]
aws_iam_role.ai_orchestration_state_machine[0]
aws_iam_role_policy.ai_orchestration_eventbridge[0]
aws_iam_role_policy.ai_orchestration_lambda_s3[0]
aws_iam_role_policy.ai_orchestration_state_machine[0]
aws_iam_role_policy_attachment.ai_orchestration_lambda_basic_execution[0]
aws_lambda_function.ai_orchestration[0]
aws_s3_bucket.dashboard_static[0]
aws_s3_bucket_public_access_block.dashboard_static[0]
aws_s3_bucket_server_side_encryption_configuration.dashboard_static[0]
aws_s3_bucket_versioning.dashboard_static[0]
aws_sfn_state_machine.ai_orchestration[0]
aws_sns_topic.ai_orchestration_failures[0]
```

Phase 8 live checks:

| Resource | Live state |
| --- | --- |
| Lambda `energy-market-news-ai-orchestration` | active |
| State machine `energy-market-ai-insight-orchestration` | active |
| SNS topic `energy-market-ai-orchestration-failures` | exists |
| EventBridge rule `energy-market-ai-orchestration-schedule` | disabled |

## Import Map

Run imports from `infra/terraform/lakehouse`.

| AWS resource | Terraform address | Import ID | Import in Phase 9? | Notes |
| --- | --- | --- | --- | --- |
| Lambda role | `aws_iam_role.lambda` | `energy-market-lambda-role` | yes | Existing ingestion role. |
| Lambda basic execution attachment | `aws_iam_role_policy_attachment.lambda_basic_execution` | `energy-market-lambda-role/arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole` | yes | Confirmed attached. |
| Lambda S3 inline policy | `aws_iam_role_policy.lambda_s3` | `energy-market-lambda-role:energy-market-lambda-s3-policy` | yes | Confirmed inline policy exists. |
| Lambda log group | `aws_cloudwatch_log_group.lambda` | `/aws/lambda/energy-market-elexon-ingest` | yes | Live retention is unset; Terraform intent is 14 days. |
| Ingestion Lambda | `aws_lambda_function.ingest` | `energy-market-elexon-ingest` | yes | Live memory 256 MB, timeout 900s, Python 3.11. |
| Daily ingestion rule | `aws_cloudwatch_event_rule.daily_ingestion` | `energy-market-daily-ingestion` | yes | Live state is enabled; Terraform intent currently disabled. |
| Daily ingestion target | `aws_cloudwatch_event_target.daily_ingestion` | `energy-market-daily-ingestion/1` | yes | Target ID is `1`. |
| Lambda EventBridge permission | `aws_lambda_permission.allow_eventbridge` | `energy-market-elexon-ingest/energy-market-daily-ingestion-invoke` | yes | Existing statement ID. |
| Glue role | `aws_iam_role.glue` | `energy-market-glue-role` | yes | Existing Glue role. |
| Glue service role attachment | `aws_iam_role_policy_attachment.glue_service_role` | `energy-market-glue-role/arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole` | yes | Confirmed attached. |
| Glue S3 inline policy | `aws_iam_role_policy.glue_s3` | `energy-market-glue-role:energy-market-glue-s3-policy` | yes | Confirmed inline policy exists. |
| Glue ETL script object | `aws_s3_object.glue_script` | `energy-market-lake-464975959576-20260405/scripts/etl_raw_to_parquet.py` | yes | Object exists. |
| Glue database | `aws_glue_catalog_database.lakehouse` | `464975959576:energy_market_lake` | yes | Database exists. |
| Raw crawler | `aws_glue_crawler.raw` | `energy-market-raw-crawler` | yes | Live target points to older `20260504` bucket. |
| Curated crawler | `aws_glue_crawler.curated` | `energy-market-curated-crawler` | yes | Live target points to `20260405/curated/`. |
| Glue ETL job | `aws_glue_job.raw_to_parquet` | `energy-market-etl-raw-to-parquet` | yes | Live script points to `20260405/scripts/etl_raw_to_parquet.py`. |
| Athena workgroup | `aws_athena_workgroup.lakehouse` | `energy-market-workgroup` | no | Workgroup does not exist; Terraform can create it later. |
| Data lake bucket | `aws_s3_bucket.data_lake[0]` | `energy-market-lake-464975959576-20260405` | no | `create_data_bucket = false`; bucket is referenced, not managed. |
| Dashboard bucket | `aws_s3_bucket.dashboard_static[0]` | `energy-market-dashboard-public-464975959576-20260511` | no | Already in Terraform state. |

## Import Command Batch

Do not run this batch until the operator has reviewed the map and confirmed
the state backup exists.

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
  energy-market-lake-464975959576-20260405/scripts/etl_raw_to_parquet.py

terraform import aws_glue_catalog_database.lakehouse \
  464975959576:energy_market_lake

terraform import aws_glue_crawler.raw \
  energy-market-raw-crawler

terraform import aws_glue_crawler.curated \
  energy-market-curated-crawler

terraform import aws_glue_job.raw_to_parquet \
  energy-market-etl-raw-to-parquet
```

## Expected First-Plan Drift

Expect these plan items after import:

- `energy-market-daily-ingestion` may be changed from `ENABLED` to `DISABLED`
  because `schedule_enabled = false`.
- `/aws/lambda/energy-market-elexon-ingest` may get log retention set to
  `14` days.
- `energy-market-raw-crawler` may be retargeted from the older
  `20260504/raw/` bucket path to the configured `20260405/raw/` bucket path.
- Terraform may add standard tags such as `Environment`, `Project`, `ManagedBy`,
  and `Workload`.
- `aws_athena_workgroup.lakehouse` may be created because
  `energy-market-workgroup` does not currently exist.
- `aws_lambda_function.ingest` may show package or environment drift if the
  deployed zip differs from the local `lambda/ingest_elexon.py` package.

Stop and investigate if Terraform wants to replace IAM roles, Lambda functions,
Glue crawlers, or the Glue job.

## Phase 9 Checklist

- [x] Create Phase 9 branch.
- [x] Confirm S3 backend configuration.
- [x] Create local ignored `backend.hcl`.
- [x] Create local ignored `terraform.tfvars`.
- [x] Pull pre-import state backup to `/tmp`.
- [x] Run `terraform validate`.
- [x] Confirm Phase 8 resources are already in Terraform state.
- [x] Confirm Phase 8 schedule remains disabled.
- [x] Produce import map.
- [ ] Review import map before mutation.
- [ ] Import older lakehouse resources into Terraform state.
- [ ] Run `terraform state list`.
- [ ] Run and review `terraform plan`.
- [ ] Remove or document expected drift.
- [ ] Confirm Phase 8 resources remain reproducible from Terraform.
- [ ] Add CloudWatch alarms only after state is clean.
- [ ] Keep schedules disabled unless a later decision explicitly enables them.

## Next State

```text
Phase 9 Step 2: imports completed in controlled batches, with state list
captured and no resources destroyed.
```
