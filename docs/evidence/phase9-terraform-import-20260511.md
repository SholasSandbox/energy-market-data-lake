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

## Targeted Daily Schedule Disable

The operator accepted Terraform disabling the older daily ingestion schedule
for now. A targeted plan was used to avoid applying unrelated drift.

Command:

```bash
terraform plan \
  -target=aws_cloudwatch_event_rule.daily_ingestion \
  -out=tfplan-disable-daily-ingestion

terraform apply tfplan-disable-daily-ingestion
```

Targeted apply result:

```text
Apply complete! Resources: 0 added, 1 changed, 0 destroyed.
```

Verified live schedule state after apply:

```text
energy-market-ai-orchestration-schedule = DISABLED
energy-market-daily-ingestion           = DISABLED
```

Remaining full plan after this targeted apply:

```text
Plan: 1 to add, 19 to change, 0 to destroy.
```

No other drift was applied.

## Phase 8 Tag Preservation

Decision:

- preserve `Phase=phase-8-ai-orchestration` on Phase 8 resources
- encode the tag in Terraform instead of leaving it as unmanaged AWS drift
- keep the tag scoped to Phase 8 resources, not the older lakehouse resources

Terraform change:

- added `local.phase8_tags`
- applied it to Phase 8 resources only

Validation:

```text
terraform fmt
terraform validate
terraform plan
```

Plan result after preserving the Phase 8 tag:

```text
Plan: 1 to add, 10 to change, 0 to destroy.
```

The plan no longer tries to remove
`Phase=phase-8-ai-orchestration` from Phase 8 resources.

No `terraform apply` was run for this tag-preservation step because the live
resources already have the tag and the goal was to make Terraform ownership
match that live state.

## Low-Risk Governance Drift Apply

Decision:

- apply governance and operating controls
- exclude Lambda package/code drift
- exclude Glue script object drift
- defer Glue job argument updates because targeting the job also pulled in the
  Glue script object dependency

Targeted plan:

```bash
terraform plan \
  -target=aws_athena_workgroup.lakehouse \
  -target=aws_cloudwatch_log_group.lambda \
  -target=aws_glue_crawler.raw \
  -target=aws_glue_crawler.curated \
  -target=aws_iam_role.lambda \
  -target=aws_iam_role.glue \
  -out=tfplan-low-risk-governance
```

Targeted plan result:

```text
Plan: 1 to add, 5 to change, 0 to destroy.
```

Applied:

```bash
terraform apply tfplan-low-risk-governance
```

Apply result:

```text
Apply complete! Resources: 1 added, 5 changed, 0 destroyed.
```

Applied changes:

- created Athena workgroup `energy-market-workgroup`
- set `/aws/lambda/energy-market-elexon-ingest` log retention to 14 days
- retargeted `energy-market-raw-crawler` to the active `20260405/raw/` bucket
  path
- confirmed `energy-market-curated-crawler` remains on `20260405/curated/`
- added standard Terraform tags to older Lambda and Glue IAM roles and crawlers

Verified state:

```text
Athena workgroup             = energy-market-workgroup
Athena workgroup metrics     = enabled
Athena workgroup enforcement = enabled
Lambda log retention         = 14 days
Raw crawler target           = s3://energy-market-lake-464975959576-20260405/raw/
Curated crawler target       = s3://energy-market-lake-464975959576-20260405/curated/
```

Remaining full plan after this targeted apply:

```text
Plan: 0 to add, 5 to change, 0 to destroy.
```

Remaining deferred drift:

- `aws_glue_job.raw_to_parquet`
- `aws_iam_role_policy.ai_orchestration_state_machine[0]`
- `aws_lambda_function.ai_orchestration[0]`
- `aws_lambda_function.ingest`
- `aws_s3_object.glue_script`

These remain deferred because they are executable artifacts or tightly coupled
to executable-artifact drift.

## Executable Artifact Drift Baseline

Completed on 2026-05-12.

Decision:

- inspect executable-artifact drift before any further apply
- keep AWS and Terraform actions read-only
- do not store secret Lambda environment values in evidence

Baseline output directory:

```text
/tmp/phase9-artifact-baseline/
```

Terraform baseline:

```text
Plan: 0 to add, 5 to change, 0 to destroy.
```

Lambda package comparison:

- `energy-market-elexon-ingest`
  - local Terraform package hash:
    `O+87gZ8+OMKKUwvzsXhA2sCVrAbDOwymkLU7MYS/Goc=`
  - live package hash:
    `LpuQEhsU45t3ne5cbEvumah4ljmMPwo8FaxzhW30Z/Y=`
  - source diff lines: `0`
  - classification: expected package update, source-equivalent
- `energy-market-news-ai-orchestration`
  - local Terraform package hash:
    `ElgyDWfVG22HqYn8vx9hieJDenug/+AnmwINSjzB++g=`
  - live package hash:
    `ElgyDWfVG22HqYn8vx9hieJDenug/+AnmwINSjzB++g=`
  - package diff lines: `0`
  - classification: metadata-only drift

Glue comparison:

- `aws_s3_object.glue_script`
  - live S3 script matches `glue/etl_raw_to_parquet.py`
  - diff lines: `0`
  - classification: metadata-only/tag drift
- `aws_glue_job.raw_to_parquet`
  - live `DefaultArguments` is `null`
  - Terraform adds required `RAW_PATH`, `CURATED_PATH`, metrics, and
    continuous logging args
  - classification: operating configuration drift

Glue script requirement:

```text
getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_PATH", "CURATED_PATH"])
```

The ETL script requires `RAW_PATH` and `CURATED_PATH`. Adding them as Glue job
default arguments makes the job runnable without manually passing those
arguments at every invocation.

Step Functions policy comparison:

- `aws_iam_role_policy.ai_orchestration_state_machine[0]`
  - live policy allows Lambda invoke on the AI orchestration Lambda
  - live policy allows SNS publish to the failure topic
  - classification: semantic no-op / policy normalization

Remaining drift classification:

- `aws_lambda_function.ingest`
  - classification: expected package update, source-equivalent
  - guidance: safe only as an accepted redeploy of identical source
- `aws_lambda_function.ai_orchestration[0]`
  - classification: metadata-only drift
  - guidance: safe to apply if normalizing Terraform state
- `aws_s3_object.glue_script`
  - classification: metadata-only/tag drift
  - guidance: safe if accepting object tag/version metadata update
- `aws_glue_job.raw_to_parquet`
  - classification: operating configuration drift
  - guidance: recommended after accepting default argument behavior
- `aws_iam_role_policy.ai_orchestration_state_machine[0]`
  - classification: semantic no-op / policy normalization
  - guidance: safe to apply if normalizing IAM policy state

No Terraform apply was run during this baseline step.

## Classified Executable Drift Apply

Completed on 2026-05-12.

Decision:

- apply accepted classified drift only
- include Glue job default arguments
- include Glue script object metadata/tags
- include AI orchestration Lambda metadata normalization
- include Step Functions policy normalization
- defer `aws_lambda_function.ingest`

Targeted plan:

```bash
terraform plan -no-color \
  -target=aws_glue_job.raw_to_parquet \
  -target=aws_s3_object.glue_script \
  -target='aws_lambda_function.ai_orchestration[0]' \
  -target='aws_iam_role_policy.ai_orchestration_state_machine[0]' \
  -out=tfplan-step4b-accepted-drift
```

Targeted plan result:

```text
Plan: 0 to add, 4 to change, 0 to destroy.
```

Applied:

```bash
terraform apply tfplan-step4b-accepted-drift
```

Apply result:

```text
Apply complete! Resources: 0 added, 3 changed, 0 destroyed.
```

Terraform planned the Step Functions policy normalization, but it resolved as a
semantic no-op during apply.

Verified live state:

```text
Glue job DefaultArguments include RAW_PATH, CURATED_PATH, metrics, and logs.
Glue script object has standard Terraform tags.
AI orchestration Lambda package hash remains unchanged.
Ingestion Lambda LastModified remains 2026-05-05T14:37:34.000+0000.
Daily ingestion schedule remains DISABLED.
AI orchestration schedule remains DISABLED.
```

Remaining full plan after this targeted apply:

```text
Plan: 0 to add, 1 to change, 0 to destroy.
```

Remaining deferred drift:

- `aws_lambda_function.ingest`

This remains deferred because applying it would redeploy the ingestion Lambda,
even though the downloaded live source and local source were source-equivalent.

## Ingestion Lambda Drift Decision

Completed on 2026-05-12.

Decision:

- document `aws_lambda_function.ingest` drift
- defer ingestion Lambda redeploy
- keep the current deployed ingestion Lambda unchanged

Reasoning:

- Live and local `ingest_elexon.py` source comparison produced `0` diff lines.
- The live package hash still differs from the Terraform-built package hash.
- Applying Terraform would therefore perform a real Lambda code update, even
  though the source is equivalent.
- A redeploy is not required for the current Phase 9 hardening objective.

Accepted residual plan:

```text
Plan: 0 to add, 1 to change, 0 to destroy.
```

Redeploy criteria:

- intentional source change to `lambda/ingest_elexon.py`
- full post-apply ingestion validation window is available
- final closeout requires a fully clean Terraform plan
- the documented drift starts making future Terraform plans harder to review

Required validation if redeployed later:

- targeted plan includes only `aws_lambda_function.ingest`
- EventBridge schedules remain disabled
- Lambda invoke succeeds
- raw S3 keys land for expected datasets
- final Terraform plan is reviewed

## Reproducibility Posture Check

Completed on 2026-05-12.

Purpose:

- confirm Phase 8 resources remain reproducible from Terraform
- confirm the remaining Terraform plan is limited to documented residual drift
- confirm schedules remain disabled

Commands:

```bash
terraform validate
terraform state list | sort > /tmp/phase9-step5/state-list.txt
terraform output -json > /tmp/phase9-step5/outputs.json
terraform plan -no-color > /tmp/phase9-step5/plan-step5-reproducibility.txt
```

Terraform proof:

```text
terraform validate = success
state address count = 44
plan = Plan: 0 to add, 1 to change, 0 to destroy.
```

Phase 8 Terraform state includes:

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

Output proof:

```text
ai_orchestration_lambda_function_name=energy-market-news-ai-orchestration
ai_orchestration_state_machine_arn=arn:aws:states:eu-west-2:464975959576:stateMachine:energy-market-ai-insight-orchestration
ai_orchestration_failure_topic_arn=arn:aws:sns:eu-west-2:464975959576:energy-market-ai-orchestration-failures
dashboard_bucket_name=energy-market-dashboard-public-464975959576-20260511
data_bucket_name=energy-market-lake-464975959576-20260405
glue_job_name=energy-market-etl-raw-to-parquet
athena_workgroup_name=energy-market-workgroup
```

Live readback:

```text
State machine energy-market-ai-insight-orchestration = ACTIVE
AI orchestration Lambda CodeSha256 = ElgyDWfVG22HqYn8vx9hieJDenug/+AnmwINSjzB++g=
SNS failure topic exists
Dashboard bucket versioning = Enabled
energy-market-daily-ingestion = DISABLED
energy-market-ai-orchestration-schedule = DISABLED
```

Conclusion:

- Phase 8 resources are reproducible from Terraform.
- The only remaining Terraform drift is the intentionally documented ingestion
  Lambda package redeploy.
- No CloudWatch alarms were added in this step because the plan is not fully
  clean; alarms should be a separate decision.

## Phase 9 Closeout

Completed on 2026-05-12.

Decision:

- close Phase 9 with documented residual drift
- keep `aws_lambda_function.ingest` redeploy deferred
- defer CloudWatch alarms to a later focused state
- keep both EventBridge schedules disabled

Final accepted plan state:

```text
Plan: 0 to add, 1 to change, 0 to destroy.
```

The remaining plan item is:

```text
aws_lambda_function.ingest
```

Closeout summary:

- Terraform import and backend adoption are complete.
- Governance drift has been applied.
- Accepted executable drift has been applied.
- Phase 8 resources are reproducible from Terraform.
- Historical S3 data, Athena results, and old dashboard evidence remain outside
  Terraform portability scope.
- Alarms remain a follow-up task, not part of this closeout.

## Data Portability Note

Terraform can recreate infrastructure in a future clean account, but it should
not be expected to move stale historical S3 data, old Athena results, or old
dashboard evidence. Those are intentionally treated as non-portable run
artifacts unless a separate backup/restore process is designed.
