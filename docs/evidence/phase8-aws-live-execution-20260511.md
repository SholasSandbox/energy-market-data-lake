# Phase 8 AWS Live Execution Evidence - 2026-05-11

## State Transition

Moved from deployable AWS skeleton to deployed AWS orchestration proof.

Target state:

- Phase 8 Terraform resources exist in AWS.
- A manual Step Functions execution succeeds.
- Curated AI orchestration artifacts land in S3.
- Public dashboard snapshot is published only after validation passes.
- A controlled failed run writes to `failed/` and leaves the previous public
  dashboard snapshot unchanged.

## Deployment Context

- AWS account: `464975959576`
- Region: `eu-west-2`
- Terraform backend bucket:
  `energy-market-terraform-state-464975959576-eu-west-2`
- Terraform state key:
  `energy-market-data-lake/phase8-ai-orchestration/terraform.tfstate`
- Data lake bucket:
  `energy-market-lake-464975959576-20260405`
- Dashboard/static bucket:
  `energy-market-dashboard-public-464975959576-20260511`
- Lambda:
  `energy-market-news-ai-orchestration`
- State machine:
  `arn:aws:states:eu-west-2:464975959576:stateMachine:energy-market-ai-insight-orchestration`
- Failure topic:
  `arn:aws:sns:eu-west-2:464975959576:energy-market-ai-orchestration-failures`

## Terraform Apply

Used targeted Terraform apply for the Phase 8 resources only, because the older
ingestion, Glue, and Athena resources were created manually and are not fully
imported into this state yet.

Terraform apply result:

```text
Apply complete! Resources: 17 added, 0 changed, 0 destroyed.
```

Terraform-managed Phase 8 resource set includes:

- dashboard/static S3 bucket, encryption, versioning, and public access block
- orchestration Lambda and log group
- Lambda execution role and S3 policy
- Step Functions execution role and policy
- Step Functions state machine
- SNS failure topic
- disabled EventBridge schedule and target role

## Successful Execution

Execution ARN:

```text
arn:aws:states:eu-west-2:464975959576:execution:energy-market-ai-insight-orchestration:phase8-proof-final-20260511T114812Z
```

Execution result:

```text
status=SUCCEEDED
run_id=ai-insight-20260511T114815Z-927685a3
status=dashboard_snapshot_published
article_count=18
insight_count=1
risk_level=low
```

Curated artifacts written:

```text
curated/source=ai_orchestration/dataset=energy_input/run_id=ai-insight-20260511T114815Z-927685a3/payload.json
curated/source=ai_orchestration/dataset=news_summary/run_id=ai-insight-20260511T114815Z-927685a3/payload.json
curated/source=ai_orchestration/dataset=ai_input_bundle/run_id=ai-insight-20260511T114815Z-927685a3/payload.json
curated/source=ai_orchestration/dataset=ai_insight/run_id=ai-insight-20260511T114815Z-927685a3/payload.json
```

Dashboard artifacts written:

```text
dashboard_snapshot_v1.json
snapshots/run_id=ai-insight-20260511T114815Z-927685a3/dashboard_snapshot_v1.json
```

Public latest snapshot:

```text
ETag="695d45951c8a5d237e5f3c5acdb15446"
VersionId=BwXaGB_3FAkHuT5r30GDPpzN2ek1V5Xz
Size=9920
```

## Controlled Failure Execution

Failure method:

- Backed up `dashboard/dashboard-data.json`.
- Temporarily removed the input key from the data lake bucket.
- Started a manual Step Functions execution.
- Restored the input key immediately after the failed execution completed.

Execution ARN:

```text
arn:aws:states:eu-west-2:464975959576:execution:energy-market-ai-insight-orchestration:phase8-forced-failure-20260511T114347Z
```

Execution result:

```text
status=FAILED
error=AIInsightOrchestrationFailed
cause=A Phase 8 orchestration step failed.
```

Failed record:

```text
failed/workflow=ai_insight/component=exportenergyinput/run_id=ai-insight-20260511T114347Z-3f741c36/payload.json
```

Failed record summary:

```json
{
  "workflow": "ai_insight",
  "run_id": "ai-insight-20260511T114347Z-3f741c36",
  "component": "exportenergyinput",
  "schema_name": "unknown",
  "status": "validation_failed",
  "reason": "NoSuchKey: dashboard/dashboard-data.json was temporarily removed."
}
```

Public snapshot preservation proof:

```text
snapshot_before_etag="0aab906f08bd90723c580e702bac99a8"
snapshot_after_etag="0aab906f08bd90723c580e702bac99a8"
snapshot_before_version=rrpoBum0D_XCtHNoKoHvST3oDi7.Mssk
snapshot_after_version=rrpoBum0D_XCtHNoKoHvST3oDi7.Mssk
public snapshot unchanged after failed run
```

## Notes

- The first two live attempts failed because the Lambda zip lacked a compatible
  `typing_extensions` dependency for `jsonschema`/`referencing` under Python
  3.11. The Lambda package build script now pins `referencing==0.36.2` and
  `typing-extensions==4.15.0`.
- EventBridge schedule remains disabled. Manual execution is the current safe
  operating mode until the next validation pass decides whether to enable it.
- The failure proof used a missing input object, not a malformed model output.
  It proves Step Functions failure routing, failed-zone write behavior, and
  public snapshot preservation in AWS.
