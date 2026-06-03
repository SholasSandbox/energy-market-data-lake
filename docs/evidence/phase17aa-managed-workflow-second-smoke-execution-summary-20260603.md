# Phase 17AA Managed Workflow Second-Smoke Execution

Date: 2026-06-03

## Scope

Phase 17AA execution ran one explicitly approved managed workflow smoke after
Phase 17Z refreshed the deployed AI orchestration Lambda package.

Guardrails:

- one manual Step Functions execution maximum
- no manual retry
- no Terraform apply
- no EventBridge schedule enablement
- rollback snapshot metadata captured before execution
- execution ARN, history, output, generated run ID, S3 artifacts, dashboard
  impact, estimated model cost, and post-run schedule state captured

## Pre-Execution Evidence

Evidence:

- `docs/evidence/phase17aa-second-smoke-execution-aws-identity-sanitized-20260603.txt`
- `docs/evidence/phase17aa-second-smoke-execution-pre-lambda-config-20260603.json`
- `docs/evidence/phase17aa-second-smoke-execution-pre-state-machine-20260603.json`
- `docs/evidence/phase17aa-second-smoke-execution-pre-schedule-state-20260603.json`
- `docs/evidence/phase17aa-second-smoke-execution-pre-recent-executions-20260603.json`
- `docs/evidence/phase17aa-second-smoke-execution-pre-latest-snapshot-head-20260603.json`
- `docs/evidence/phase17aa-second-smoke-execution-pre-immutable-snapshot-head-20260603.json`
- `docs/evidence/phase17aa-second-smoke-execution-pre-dashboard-http-check-20260603.txt`
- `docs/evidence/phase17aa-second-smoke-execution-pre-terraform-nochange-20260603.txt`

Result:

- Lambda was active and still on the Phase 17Z refreshed code hash
- Step Functions routed to `MergeAiInsightManaged`, then
  `PublishDashboardSnapshot`
- EventBridge schedule was `DISABLED`
- Terraform reported `No changes`
- latest dashboard snapshot version before execution was
  `qYxpit3hmGzpSByvhG07nrOG4kBrz1qn`
- latest dashboard snapshot SHA-256 before execution was
  `d180b4a2bda131fae6088a650301f40b696ba67929ffcea78be42731adb3a741`

## Execution

Execution name:

- `phase17aa-managed-workflow-second-smoke-20260603T010742Z`

Execution evidence:

- `docs/evidence/phase17aa-second-smoke-start-execution-20260603.json`
- `docs/evidence/phase17aa-second-smoke-describe-execution-20260603.json`
- `docs/evidence/phase17aa-second-smoke-execution-history-20260603.json`
- `docs/evidence/phase17aa-second-smoke-generated-run-id-20260603.txt`
- `docs/evidence/phase17aa-second-smoke-output-summary-20260603.json`

Result:

- execution status: `SUCCEEDED`
- generated run ID: `ai-insight-20260603T010744Z-4d89a62a`
- workflow status: `dashboard_snapshot_published`
- manual retries: `0`
- redrive count: `0`
- Bedrock managed merge succeeded through Mistral
- `ai_provider`: `bedrock`
- `bedrock_provider`: `mistral`
- `bedrock_model_id`: `mistral.ministral-3-8b-instruct`
- `risk_level`: `watch`
- `insight_count`: `1`

## Artifact And Validation Evidence

Evidence:

- `docs/evidence/phase17aa-second-smoke-s3-artifacts-20260603.json`
- `docs/evidence/phase17aa-second-smoke-artifact-summary-20260603.txt`
- `docs/evidence/phase17aa-second-smoke-schema-validation-summary-20260603.txt`
- `docs/evidence/phase17aa-second-smoke-failed-artifacts-20260603.json`

Result:

- `energy_input` artifact was written
- `news_summary` artifact was written
- `ai_input_bundle` artifact was written
- `ai_insight` artifact was written
- latest dashboard snapshot was written
- immutable run dashboard snapshot was written at
  `snapshots/run_id=ai-insight-20260603T010744Z-4d89a62a/dashboard_snapshot_v1.json`
- generated `ai_insight_v1` validates
- generated `dashboard_snapshot_v1` validates
- no failed artifact was written for the run
- raw AI payloads and raw model output were not committed

## Dashboard Impact

Evidence:

- `docs/evidence/phase17aa-second-smoke-dashboard-impact-summary-20260603.txt`
- `docs/evidence/phase17aa-second-smoke-post-latest-snapshot-head-20260603.json`
- `docs/evidence/phase17aa-second-smoke-post-immutable-snapshot-head-20260603.json`
- `docs/evidence/phase17aa-second-smoke-post-dashboard-http-check-20260603.txt`
- `docs/evidence/phase17aa-second-smoke-post-immutable-dashboard-http-check-20260603.txt`

Result:

- dashboard publish occurred
- latest snapshot ETag changed from `"c341541722f25da0ab5dddf6fe9a2f21"` to
  `"78dc3e2733a818b8c876fc156ad905eb"`
- latest snapshot version changed from
  `qYxpit3hmGzpSByvhG07nrOG4kBrz1qn` to
  `b9PUPbupwFRcRCIHTcMwFhylWsuDCkSv`
- latest CloudFront snapshot SHA-256 changed from
  `d180b4a2bda131fae6088a650301f40b696ba67929ffcea78be42731adb3a741` to
  `d4806fbbd0a2045ad1bc79c511601ad5f342ebe8a12fe276448cec1b6fb1d515`
- latest and immutable CloudFront snapshot paths both returned `200`
- no CloudFront invalidation was requested

## Cost And Guardrails

Evidence:

- `docs/evidence/phase17aa-second-smoke-cost-summary-20260603.txt`
- `docs/evidence/phase17aa-second-smoke-post-lambda-config-20260603.json`
- `docs/evidence/phase17aa-second-smoke-post-schedule-state-20260603.json`
- `docs/evidence/phase17aa-second-smoke-post-recent-executions-20260603.json`
- `docs/evidence/phase17aa-second-smoke-post-terraform-nochange-20260603.txt`

Result:

- Bedrock was invoked through the managed Mistral workflow path
- workflow output does not emit token usage
- estimated direct model cost: `$0.00132618`
- practical budget cap: `$0.10`
- EventBridge schedule remains `DISABLED`
- Lambda remains on the Phase 17Z refreshed code hash
- post-run Terraform reports `No changes`
- recent execution evidence shows exactly one new Phase 17AA smoke execution

## Red-Green Evidence

Red:

- Phase 17Y proved managed workflow routing reached `MergeAiInsightManaged`,
  but failed because the deployed Lambda package was stale.

Green:

- Phase 17AA execution proved the refreshed deployed Lambda package can run the
  managed Bedrock/Mistral merge and publish a valid dashboard snapshot through
  the managed Step Functions workflow.

Regression:

- one execution only
- no manual retry
- schedules remain disabled
- Terraform remains no-change
- generated artifacts validate
- failed artifacts are empty for the run

## Boundary

Phase 17AA execution is complete.

Next recommended state:

- Phase 17AB: managed workflow post-smoke demo verification
- keep the next slice read-only
- verify hosted dashboard behavior after the workflow-published snapshot
- keep schedule enablement as a later explicit decision boundary
