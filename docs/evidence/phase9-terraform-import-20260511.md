# Phase 9 Terraform Import Evidence - 2026-05-11

## State Transition

Moved from import-map-ready to imported Terraform state for the older
manually-created lakehouse resources.

Target state:

- older lakehouse resources are adopted into Terraform state
- Phase 8 resources are not re-imported
- state backup exists before mutation
- post-import state list is captured
- Terraform plan is reviewed before any apply
- no schedules are enabled by this step

## Context

- Branch: `phase9-terraform-import-hardening`
- AWS account: `464975959576`
- Region: `eu-west-2`
- Terraform backend bucket:
  `energy-market-terraform-state-464975959576-eu-west-2`
- Terraform state key:
  `energy-market-data-lake/phase8-ai-orchestration/terraform.tfstate`

Local-only backup artifacts:

```text
/tmp/phase9-terraform-import/state-before-import.json
/tmp/phase9-terraform-import/state-after-import.json
/tmp/phase9-terraform-import/state-after-import-list.txt
/tmp/phase9-terraform-import/plan-after-import.txt
/tmp/phase9-terraform-import/tfplan-after-import-show.txt
```

## Imported Resources

The import batch completed successfully for:

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

Phase 8 resources were already in state and were not imported again.

## Post-Import State

Post-import `terraform state list` contains 43 entries, including data sources.

The managed resource set now includes:

- Phase 8 AI orchestration Lambda, IAM, Step Functions, SNS, schedule, and
  dashboard bucket resources
- older ingestion Lambda, IAM, EventBridge, Glue, and ETL resources

## Validation

```text
terraform validate
```

Result:

```text
Success! The configuration is valid.
```

## Plan Summary

```text
Plan: 1 to add, 20 to change, 0 to destroy.
```

The plan was saved locally as:

```text
infra/terraform/lakehouse/tfplan
```

No `terraform apply` was run.

## Drift Observed

The first post-import plan wants to:

- create `aws_athena_workgroup.lakehouse`
- disable `energy-market-daily-ingestion`
- set `/aws/lambda/energy-market-elexon-ingest` log retention to 14 days
- retarget `energy-market-raw-crawler` from the older `20260504/raw/` bucket
  path to `20260405/raw/`
- add standard tags to imported older resources
- remove the extra `Phase=phase-8-ai-orchestration` tag from Phase 8 resources
- update Lambda/package metadata for the imported ingestion Lambda
- update the managed Glue script object metadata

No destroys were proposed.

## Schedule Check

After import and plan:

```text
energy-market-ai-orchestration-schedule = DISABLED
energy-market-daily-ingestion           = ENABLED
```

The Phase 8 schedule remains disabled as intended. The older daily ingestion
rule remains live because the plan has not been applied.

## Data Portability Note

Terraform can recreate infrastructure in a future clean account, but it should
not be expected to move stale historical S3 data, old Athena results, or old
dashboard evidence. Those are intentionally treated as non-portable run
artifacts unless a separate backup/restore process is designed.
