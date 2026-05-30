# Phase 17Y Managed Workflow Smoke Execution

Date: 2026-05-30

## Scope

Phase 17Y ran one explicitly approved manual Step Functions managed workflow
smoke execution.

Guardrails:

- one manual Step Functions execution maximum
- no manual retry
- no Terraform apply
- no EventBridge schedule enablement
- rollback snapshot metadata captured before execution
- execution ARN, history, generated run ID, S3 artifacts, dashboard impact, and
  post-run schedule state captured

## Execution

Execution name:

- `phase17y-managed-workflow-smoke-20260530T205941Z`

Execution ARN:

- `arn:aws:states:eu-west-2:464975959576:execution:energy-market-ai-insight-orchestration:phase17y-managed-workflow-smoke-20260530T205941Z`

Evidence:

- `docs/evidence/phase17y-managed-workflow-smoke-start-execution-20260530.json`
- `docs/evidence/phase17y-managed-workflow-smoke-describe-execution-20260530.json`
- `docs/evidence/phase17y-managed-workflow-smoke-execution-history-20260530.json`
- `docs/evidence/phase17y-managed-workflow-smoke-failure-summary-20260530.txt`
- `docs/evidence/phase17y-managed-workflow-smoke-root-cause-summary-20260530.txt`

Result:

- execution status: `FAILED`
- manual retries: `0`
- generated run ID: `ai-insight-20260530T205944Z-df1fdb6a`
- failure state: `MergeAiInsightManaged`
- sanitized failure reason: deployed Lambda handler did not recognize
  `MergeAiInsightManaged`
- Bedrock was not invoked because the Lambda failed before the managed action
  handler could call Bedrock
- estimated Bedrock cost: `$0.00`

## Artifact Evidence

Generated run ID evidence:

- `docs/evidence/phase17y-managed-workflow-smoke-generated-run-id-20260530.txt`

S3 artifact evidence:

- `docs/evidence/phase17y-managed-workflow-smoke-s3-artifacts-20260530.json`
- `docs/evidence/phase17y-managed-workflow-smoke-artifact-summary-20260530.txt`
- `docs/evidence/phase17y-managed-workflow-smoke-failed-artifacts-20260530.json`
- `docs/evidence/phase17y-managed-workflow-smoke-dashboard-run-snapshot-objects-20260530.json`

Result:

- `energy_input`, `news_summary`, and `ai_input_bundle` artifacts were written
  for the run
- no `ai_insight` artifact was written
- no run-scoped dashboard snapshot was written
- no failed payload record was written because the unknown-action error occurs
  before the Lambda dispatcher enters the failed-record wrapper
- the state machine still routed to `WorkflowFailed` and SNS failure publish
  succeeded

## Dashboard Impact

Rollback and dashboard evidence:

- `docs/evidence/phase17y-pre-smoke-latest-snapshot-head-20260530.json`
- `docs/evidence/phase17y-pre-smoke-immutable-snapshot-head-20260530.json`
- `docs/evidence/phase17y-pre-smoke-dashboard-http-check-20260530.txt`
- `docs/evidence/phase17y-pre-smoke-dashboard-sha256-20260530.txt`
- `docs/evidence/phase17y-managed-workflow-smoke-post-latest-snapshot-head-20260530.json`
- `docs/evidence/phase17y-managed-workflow-smoke-post-dashboard-http-check-20260530.txt`
- `docs/evidence/phase17y-managed-workflow-smoke-post-dashboard-sha256-20260530.txt`
- `docs/evidence/phase17y-managed-workflow-smoke-dashboard-impact-summary-20260530.txt`

Result:

- live dashboard latest snapshot version did not change
- live dashboard latest snapshot ETag did not change
- live dashboard snapshot SHA256 stayed
  `d180b4a2bda131fae6088a650301f40b696ba67929ffcea78be42731adb3a741`
- CloudFront still returns `200` for `dashboard_snapshot_v1.json`
- no rollback was required

## Schedule And Infrastructure

Evidence:

- `docs/evidence/phase17y-pre-smoke-schedule-state-20260530.json`
- `docs/evidence/phase17y-managed-workflow-smoke-post-schedule-state-20260530.json`
- `docs/evidence/phase17y-pre-smoke-terraform-nochange-20260530.txt`
- `docs/evidence/phase17y-pre-smoke-state-summary-20260530.txt`
- `docs/evidence/phase17y-managed-workflow-smoke-lambda-config-with-code-20260530.json`

Result:

- EventBridge schedule remained `DISABLED`
- no Terraform apply was run
- no infrastructure mutation was performed during the smoke execution

## Red-Green Evidence

Red:

- Phase 17W deployed managed workflow routing, but no manual managed workflow
  smoke had run.

Green:

- Phase 17Y proved the deployed state machine reaches the managed merge state
  and fails safely before Bedrock when the live Lambda package lacks the
  managed action handler.

Regression:

- no retry was run
- no Bedrock cost was incurred
- no dashboard publish occurred
- schedule remains disabled

## Boundary

Phase 17Y execution is complete.

Next recommended state:

- Phase 17Z: Lambda package refresh preflight
- rebuild and deploy the AI orchestration Lambda package containing
  `MergeAiInsightManaged` before any second managed workflow smoke
- keep schedules disabled
- do not retry the managed workflow until package refresh evidence is reviewed
