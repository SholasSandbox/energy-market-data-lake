# Phase 8 Operational Runbook

<!-- markdownlint-disable MD013 -->

Use this runbook to rerun, inspect, and demo the Phase 8 AWS AI insight
orchestration without enabling the schedule.

## Current Operating State

Current state:

- Phase 8 is deployed in AWS.
- Manual Step Functions execution is the approved operating mode.
- EventBridge scheduling exists but remains disabled.
- Dashboard publishing writes only after validation gates pass.
- Failed runs write to `failed/` and leave the previous public snapshot intact.

Target state for each operational run:

- one Step Functions execution reaches `SUCCEEDED`
- S3 contains run-scoped curated artifacts for the new `run_id`
- the dashboard bucket contains the latest approved snapshot and an immutable
  run-scoped copy
- CloudWatch execution history can explain the run

Deferred by design:

- Bedrock `InvokeModel`
- OpenClaw managed runtime
- EventBridge scheduled automation
- public website hosting or CloudFront

Historical note: this was the Phase 8 operating boundary. Bedrock invocation,
the EventBridge schedule, CloudFront delivery, and an AWS Budget were later
implemented and verified. ADR 0007 rejects OpenClaw/ECS for the current target,
defers LangGraph and multi-agent orchestration, and continues to reject raw
model text publication. Use the Phase 17AU evidence and current README for
present-state claims; do not execute this historical runbook as if it were the
current operating procedure.

## Environment

```bash
export AWS_REGION=eu-west-2
export DATA_BUCKET=energy-market-lake-464975959576-20260405
export DASHBOARD_BUCKET=energy-market-dashboard-public-464975959576-20260511
export AI_ORCHESTRATION_FUNCTION_NAME=energy-market-news-ai-orchestration
export AI_ORCHESTRATION_STATE_MACHINE_ARN="arn:aws:states:eu-west-2:464975959576:stateMachine:energy-market-ai-insight-orchestration"
export AI_ORCHESTRATION_FAILURE_TOPIC_ARN="arn:aws:sns:eu-west-2:464975959576:energy-market-ai-orchestration-failures"
export DASHBOARD_DATA_KEY=dashboard/dashboard-data.json
```

## Preflight

Confirm the account and region before any run:

```bash
aws sts get-caller-identity --region "${AWS_REGION}"
aws configure get region
```

Confirm the Lambda and state machine exist:

```bash
aws lambda get-function-configuration \
  --function-name "${AI_ORCHESTRATION_FUNCTION_NAME}" \
  --region "${AWS_REGION}" \
  --query '{name:FunctionName,runtime:Runtime,timeout:Timeout,memory:MemorySize}'

aws stepfunctions describe-state-machine \
  --state-machine-arn "${AI_ORCHESTRATION_STATE_MACHINE_ARN}" \
  --region "${AWS_REGION}" \
  --query '{name:name,status:status,type:type}'
```

Confirm the schedule is still disabled:

```bash
aws events describe-rule \
  --name energy-market-ai-orchestration-schedule \
  --region "${AWS_REGION}" \
  --query '{name:Name,state:State,schedule:ScheduleExpression}'
```

Expected schedule state:

```text
DISABLED
```

Confirm the input dashboard data exists:

```bash
aws s3api head-object \
  --bucket "${DATA_BUCKET}" \
  --key "${DASHBOARD_DATA_KEY}" \
  --region "${AWS_REGION}" \
  --query '{etag:ETag,size:ContentLength,last_modified:LastModified}'
```

Capture the current public snapshot before running:

```bash
aws s3api head-object \
  --bucket "${DASHBOARD_BUCKET}" \
  --key dashboard_snapshot_v1.json \
  --region "${AWS_REGION}" \
  --query '{etag:ETag,version:VersionId,size:ContentLength,last_modified:LastModified}'
```

## Start A Manual Execution

```bash
export EXECUTION_NAME="phase8-manual-$(date -u +%Y%m%dT%H%M%SZ)"

aws stepfunctions start-execution \
  --state-machine-arn "${AI_ORCHESTRATION_STATE_MACHINE_ARN}" \
  --name "${EXECUTION_NAME}" \
  --input '{}' \
  --region "${AWS_REGION}" \
  --query executionArn \
  --output text | tee /tmp/phase8-execution-arn.txt

export EXECUTION_ARN="$(cat /tmp/phase8-execution-arn.txt)"
```

Poll until the execution reaches a terminal state:

```bash
while true; do
  STATUS="$(aws stepfunctions describe-execution \
    --execution-arn "${EXECUTION_ARN}" \
    --region "${AWS_REGION}" \
    --query status \
    --output text)"
  echo "${STATUS}"
  case "${STATUS}" in
    SUCCEEDED|FAILED|TIMED_OUT|ABORTED) break ;;
    *) sleep 10 ;;
  esac
done
```

Read the final execution output:

```bash
aws stepfunctions describe-execution \
  --execution-arn "${EXECUTION_ARN}" \
  --region "${AWS_REGION}" \
  --query output \
  --output text | jq . | tee /tmp/phase8-output.json

export RUN_ID="$(jq -r '.run_id' /tmp/phase8-output.json)"
echo "${RUN_ID}"
```

Expected state:

```text
SUCCEEDED
```

Expected output fields:

- `workflow=ai_insight`
- `status=dashboard_snapshot_published`
- `run_id=ai-insight-...`
- `summary.article_count`
- `summary.insight_count`
- `summary.risk_level`

## Verify S3 Artifacts

Check the private curated artifacts:

```bash
for DATASET in energy_input news_summary ai_input_bundle ai_insight; do
  aws s3api head-object \
    --bucket "${DATA_BUCKET}" \
    --key "curated/source=ai_orchestration/dataset=${DATASET}/run_id=${RUN_ID}/payload.json" \
    --region "${AWS_REGION}" \
    --query '{dataset:`'"${DATASET}"'`,etag:ETag,size:ContentLength,last_modified:LastModified}'
done
```

Check the public-safe dashboard artifacts:

```bash
aws s3api head-object \
  --bucket "${DASHBOARD_BUCKET}" \
  --key dashboard_snapshot_v1.json \
  --region "${AWS_REGION}" \
  --query '{etag:ETag,version:VersionId,size:ContentLength,last_modified:LastModified}'

aws s3api head-object \
  --bucket "${DASHBOARD_BUCKET}" \
  --key "snapshots/run_id=${RUN_ID}/dashboard_snapshot_v1.json" \
  --region "${AWS_REGION}" \
  --query '{etag:ETag,version:VersionId,size:ContentLength,last_modified:LastModified}'
```

Sample one artifact:

```bash
aws s3 cp \
  "s3://${DATA_BUCKET}/curated/source=ai_orchestration/dataset=ai_insight/run_id=${RUN_ID}/payload.json" \
  - | jq '{schema_version, generated_at, insight_count: (.insights | length)}'
```

## Inspect Execution History And Logs

```bash
aws stepfunctions get-execution-history \
  --execution-arn "${EXECUTION_ARN}" \
  --region "${AWS_REGION}" \
  --max-results 25 \
  --query 'events[].{id:id,type:type,timestamp:timestamp}'

aws logs tail "/aws/lambda/${AI_ORCHESTRATION_FUNCTION_NAME}" \
  --since 30m \
  --region "${AWS_REGION}"
```

## Failure Drill

Use this only for a controlled proof window. The drill temporarily removes the
input object, proves failure routing, and restores the object immediately.

Capture the current public snapshot:

```bash
aws s3api head-object \
  --bucket "${DASHBOARD_BUCKET}" \
  --key dashboard_snapshot_v1.json \
  --region "${AWS_REGION}" \
  --query '{etag:ETag,version:VersionId}' \
  --output json | tee /tmp/phase8-snapshot-before.json
```

Back up and temporarily remove the input object:

```bash
aws s3 cp \
  "s3://${DATA_BUCKET}/${DASHBOARD_DATA_KEY}" \
  /tmp/phase8-dashboard-data-backup.json \
  --region "${AWS_REGION}"

aws s3 rm \
  "s3://${DATA_BUCKET}/${DASHBOARD_DATA_KEY}" \
  --region "${AWS_REGION}"
```

Start a forced-failure execution:

```bash
export FAILURE_EXECUTION_NAME="phase8-forced-failure-$(date -u +%Y%m%dT%H%M%SZ)"

aws stepfunctions start-execution \
  --state-machine-arn "${AI_ORCHESTRATION_STATE_MACHINE_ARN}" \
  --name "${FAILURE_EXECUTION_NAME}" \
  --input '{}' \
  --region "${AWS_REGION}" \
  --query executionArn \
  --output text | tee /tmp/phase8-failure-execution-arn.txt

export FAILURE_EXECUTION_ARN="$(cat /tmp/phase8-failure-execution-arn.txt)"
```

Restore the input object:

```bash
aws s3 cp \
  /tmp/phase8-dashboard-data-backup.json \
  "s3://${DATA_BUCKET}/${DASHBOARD_DATA_KEY}" \
  --region "${AWS_REGION}"
```

Poll the failure execution:

```bash
while true; do
  STATUS="$(aws stepfunctions describe-execution \
    --execution-arn "${FAILURE_EXECUTION_ARN}" \
    --region "${AWS_REGION}" \
    --query status \
    --output text)"
  echo "${STATUS}"
  case "${STATUS}" in
    SUCCEEDED|FAILED|TIMED_OUT|ABORTED) break ;;
    *) sleep 10 ;;
  esac
done
```

Expected state:

```text
FAILED
```

Confirm a failed record exists:

```bash
aws s3api list-objects-v2 \
  --bucket "${DATA_BUCKET}" \
  --prefix "failed/workflow=ai_insight/" \
  --region "${AWS_REGION}" \
  --query 'reverse(sort_by(Contents,&LastModified))[0].Key' \
  --output text | tee /tmp/phase8-latest-failed-key.txt

aws s3 cp \
  "s3://${DATA_BUCKET}/$(cat /tmp/phase8-latest-failed-key.txt)" \
  - \
  --region "${AWS_REGION}" | jq '{workflow,run_id,component,schema_name,status,reason}'
```

Confirm the public snapshot did not change:

```bash
aws s3api head-object \
  --bucket "${DASHBOARD_BUCKET}" \
  --key dashboard_snapshot_v1.json \
  --region "${AWS_REGION}" \
  --query '{etag:ETag,version:VersionId}' \
  --output json | tee /tmp/phase8-snapshot-after.json

diff /tmp/phase8-snapshot-before.json /tmp/phase8-snapshot-after.json
```

Expected result:

```text
no diff output
```

## Disable Schedule

If the schedule is accidentally enabled before the next design decision, turn it
off:

```bash
aws events disable-rule \
  --name energy-market-ai-orchestration-schedule \
  --region "${AWS_REGION}"
```

## Rebuild And Terraform Notes

Build the deterministic Lambda package:

```bash
./scripts/build_phase8_lambda_package.sh
```

Validate Terraform locally:

```bash
cd infra/terraform/lakehouse
terraform fmt -check
terraform init -backend=false
terraform validate
```

For the live Phase 8 resources, use the S3 backend documented in
`infra/terraform/lakehouse/README.md` and `docs/setup.md`. The 2026-05-11 proof
used a targeted Phase 8 apply because older lakehouse resources were not yet
fully imported into Terraform state.

## Demo Talk Track

```text
This is not a model-quality demo. It is an AI orchestration control demo:
Step Functions coordinates the deterministic insight workflow, Lambda writes
run-scoped S3 artifacts, schema validation gates the publish step, bad runs are
quarantined, and the public dashboard snapshot is updated only after the run is
valid.
```

Keep the demo under two minutes:

1. Show the state machine execution succeeded.
2. Show the `run_id`.
3. Show the four curated S3 artifacts.
4. Show latest plus immutable dashboard snapshot.
5. Show the failed-run evidence that preserves the previous snapshot.

## Completion Evidence

The first live proof is captured here:

```text
docs/evidence/phase8-aws-live-execution-20260511.md
```
