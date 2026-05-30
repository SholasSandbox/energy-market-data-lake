# Phase 17X Managed Workflow Smoke Decision

Date: 2026-05-30

## Scope

Phase 17X reviewed whether the managed Step Functions workflow should be run
once as a manual smoke execution after Phase 17W deployed managed routing.

This was a decision-only slice:

- no Bedrock invocation
- no Step Functions execution
- no Terraform apply
- no IAM mutation
- no Lambda deployment
- no Step Functions deployment
- no EventBridge schedule enablement
- no DNS, ACM, alarms, budgets, S3 write, CloudFront invalidation, or dashboard
  publish

## Current State

Read-only evidence:

- `docs/evidence/phase17x-managed-workflow-smoke-decision-lambda-config-20260530.json`
- `docs/evidence/phase17x-managed-workflow-smoke-decision-state-machine-20260530.json`
- `docs/evidence/phase17x-managed-workflow-smoke-decision-schedule-state-20260530.json`
- `docs/evidence/phase17x-managed-workflow-smoke-decision-recent-executions-20260530.json`
- `docs/evidence/phase17x-managed-workflow-smoke-decision-dashboard-http-check-20260530.txt`
- `docs/evidence/phase17x-managed-workflow-smoke-decision-latest-snapshot-head-20260530.json`
- `docs/evidence/phase17x-managed-workflow-smoke-decision-immutable-snapshot-head-20260530.json`
- `docs/evidence/phase17x-managed-workflow-smoke-decision-terraform-nochange-20260530.txt`

Confirmed:

- Lambda is active
- Lambda `AI_ORCHESTRATION_MODE` is `managed`
- Lambda `BEDROCK_MODEL_ID` is `mistral.ministral-3-8b-instruct`
- Step Functions state machine is `ACTIVE`
- `CreateAiInputBundle` routes to `MergeAiInsightManaged`
- `MergeAiInsightManaged` routes to `PublishDashboardSnapshot`
- EventBridge schedule remains `DISABLED`
- the most recent listed executions are the May 11 Phase 8 executions
- current live `dashboard_snapshot_v1.json` returns `200`
- latest and immutable managed snapshot object metadata was captured for
  rollback review
- Terraform is no-op only when managed mode, CloudFront preservation, and
  schedule-disabled variables are passed together

## Decision

Managed workflow smoke execution is a **go-candidate**, not automatic.

Execution remains blocked until explicit approval in a separate substate.

This is not a harmless read-only smoke. A successful run would invoke Bedrock
and then write a new latest public dashboard snapshot because the deployed state
machine ends at `PublishDashboardSnapshot`.

## Required Guardrails For Execution

Any future smoke execution must:

- run one manual Step Functions execution only
- use no manual retry
- keep EventBridge schedule disabled
- keep Terraform apply out of scope
- capture latest snapshot object metadata before start
- capture the immutable managed snapshot rollback reference before start
- capture execution ARN, execution history, final output, generated run ID, and
  produced S3 artifact keys
- capture whether `dashboard_snapshot_v1.json` changed
- estimate Bedrock cost from available invocation metadata
- stop after the result is reviewed

The current state machine does not pass a caller-supplied internal `run_id`
into `InitializeRun`. The manual execution can have a controlled execution
name, but the workflow run ID must be recovered from the execution output or
history after the run.

## Future Execution Shape

The future execution boundary should start with a command shaped like this, but
it was not run in Phase 17X:

```bash
RUN_NAME="phase17y-managed-workflow-smoke-$(date -u +%Y%m%dT%H%M%SZ)"
STATE_MACHINE_ARN="arn:aws:states:eu-west-2:464975959576:"\
"stateMachine:energy-market-ai-insight-orchestration"

aws stepfunctions start-execution \
  --region eu-west-2 \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --name "$RUN_NAME" \
  --input '{}'
```

After execution, capture:

- `describe-execution`
- `get-execution-history`
- generated run ID
- lake `ai_insight`, `dashboard_snapshot`, failed, and audit artifact state
- dashboard latest object metadata
- CloudFront HTTP check
- schedule state

## Red-Green Evidence

Red:

- managed workflow routing is deployed, but a manual run is publish-capable and
  had not yet been reviewed as its own boundary.

Green:

- Phase 17X records that smoke execution is a go-candidate only after explicit
  approval, one-run discipline, and rollback evidence capture.

Regression:

- no workflow execution was started
- no Bedrock invocation was made
- schedule remains disabled
- dashboard snapshot remains reachable
- Terraform no-change proof is preserved for managed mode with CloudFront and
  schedules protected

## Boundary

Phase 17X is complete as a decision slice.

Next recommended state:

- Phase 17Y: controlled managed workflow smoke execution
- explicit approval required
- one manual execution maximum
- no schedule enablement
- no Terraform apply
