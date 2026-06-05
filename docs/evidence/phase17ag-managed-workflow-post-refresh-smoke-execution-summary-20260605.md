# Phase 17AG Managed Workflow Post-Refresh Smoke Execution

<!-- markdownlint-disable MD013 -->

Date: 2026-06-05

## Scope

Phase 17AG ran one explicitly approved managed workflow post-refresh smoke after
Phase 17AE refreshed the deployed AI orchestration Lambda package with the
Phase 17AC source-label sanitizer.

Guardrails:

- one manual Step Functions execution maximum
- no manual retry
- no Terraform apply
- no EventBridge schedule enablement
- rollback snapshot metadata captured before execution
- execution ARN, history, output, generated run ID, S3 artifacts, dashboard
  impact, estimated model cost, source-label proof, and post-run schedule state
  captured

## Pre-Execution Evidence

Evidence:

- `docs/evidence/phase17ag-smoke-execution-aws-identity-sanitized-20260605.txt`
- `docs/evidence/phase17ag-smoke-execution-pre-lambda-config-20260605.json`
- `docs/evidence/phase17ag-smoke-execution-pre-state-machine-20260605.json`
- `docs/evidence/phase17ag-smoke-execution-pre-state-machine-routing-20260605.json`
- `docs/evidence/phase17ag-smoke-execution-pre-schedule-state-20260605.json`
- `docs/evidence/phase17ag-smoke-execution-pre-recent-executions-20260605.json`
- `docs/evidence/phase17ag-smoke-execution-pre-latest-snapshot-head-20260605.json`
- `docs/evidence/phase17ag-smoke-execution-pre-immutable-snapshot-head-20260605.json`
- `docs/evidence/phase17ag-smoke-execution-pre-dashboard-http-check-20260605.txt`
- `docs/evidence/phase17ag-smoke-execution-pre-terraform-nochange-20260605.txt`

Result:

- Lambda was active and on the Phase 17AE source-label sanitizer package hash
- Step Functions routed to `MergeAiInsightManaged`, then
  `PublishDashboardSnapshot`
- EventBridge schedule was `DISABLED`
- Terraform reported `No changes`
- latest dashboard snapshot version before execution was
  `b9PUPbupwFRcRCIHTcMwFhylWsuDCkSv`
- rollback immutable snapshot was
  `snapshots/run_id=ai-insight-20260603T010744Z-4d89a62a/dashboard_snapshot_v1.json`
- latest CloudFront snapshot SHA-256 before execution was
  `d4806fbbd0a2045ad1bc79c511601ad5f342ebe8a12fe276448cec1b6fb1d515`

## Execution

Execution name:

- `phase17ag-managed-workflow-post-refresh-smoke-20260605T213352Z`

Execution evidence:

- `docs/evidence/phase17ag-smoke-start-execution-20260605.json`
- `docs/evidence/phase17ag-smoke-describe-execution-20260605.json`
- `docs/evidence/phase17ag-smoke-execution-history-20260605.json`
- `docs/evidence/phase17ag-smoke-terminal-status-20260605.txt`
- `docs/evidence/phase17ag-smoke-generated-run-id-20260605.txt`
- `docs/evidence/phase17ag-smoke-output-summary-20260605.json`

Result:

- execution status: `SUCCEEDED`
- generated run ID: `ai-insight-20260605T213354Z-88068c72`
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

- `docs/evidence/phase17ag-smoke-s3-artifacts-20260605.json`
- `docs/evidence/phase17ag-smoke-artifact-summary-20260605.txt`
- `docs/evidence/phase17ag-smoke-schema-validation-summary-20260605.txt`
- `docs/evidence/phase17ag-smoke-source-label-summary-20260605.txt`
- `docs/evidence/phase17ag-smoke-failed-artifacts-20260605.json`

Result:

- `energy_input` artifact was written
- `news_summary` artifact was written
- `ai_input_bundle` artifact was written
- `ai_insight` artifact was written
- latest dashboard snapshot object was written in S3
- immutable run dashboard snapshot was written at
  `snapshots/run_id=ai-insight-20260605T213354Z-88068c72/dashboard_snapshot_v1.json`
- generated `energy_input_v1`, `news_summary_v1`, `ai_input_bundle_v1`,
  `ai_insight_v1`, and `dashboard_snapshot_v1` artifacts validate
- source-label validation found `0` violations
- generated dashboard sources are public-safe:
  - `dashboard-data.json`
  - `https://www.energyvoice.com/renewables-energy-transition/grid-retail/598797/centrica-smart-meter/`
- no failed artifact was written for the run
- raw payloads and raw model output were not committed

## Dashboard Impact

Evidence:

- `docs/evidence/phase17ag-smoke-dashboard-impact-summary-20260605.txt`
- `docs/evidence/phase17ag-smoke-post-latest-snapshot-head-20260605.json`
- `docs/evidence/phase17ag-smoke-post-immutable-snapshot-head-20260605.json`
- `docs/evidence/phase17ag-smoke-post-dashboard-http-check-20260605.txt`
- `docs/evidence/phase17ag-smoke-post-dashboard-http-recheck-20260605.txt`
- `docs/evidence/phase17ag-smoke-post-immutable-dashboard-http-check-20260605.txt`

Result:

- dashboard publish occurred at the S3 object layer
- latest S3 snapshot ETag changed from
  `"78dc3e2733a818b8c876fc156ad905eb"` to
  `"2c239d74ed726990ec7322b7fe6228c9"`
- latest S3 snapshot version changed from
  `b9PUPbupwFRcRCIHTcMwFhylWsuDCkSv` to
  `KByeeyWC.YWMJOzJ6OGYvlIn8xN7Et2f`
- latest S3 snapshot SHA-256 after execution is
  `4c4871a2ff09f11ed097e4c03f637b34812d3893a1d2dbb97b0584cc7001d4c0`
- immutable Phase 17AG CloudFront snapshot path returned `200` and served the
  new snapshot SHA-256
  `4c4871a2ff09f11ed097e4c03f637b34812d3893a1d2dbb97b0584cc7001d4c0`
- normal CloudFront latest path still served the cached Phase 17AA snapshot at
  the initial post-run check and recheck
- no CloudFront invalidation was requested

## Cost And Guardrails

Evidence:

- `docs/evidence/phase17ag-smoke-cost-summary-20260605.txt`
- `docs/evidence/phase17ag-smoke-post-lambda-config-20260605.json`
- `docs/evidence/phase17ag-smoke-post-schedule-state-20260605.json`
- `docs/evidence/phase17ag-smoke-post-recent-executions-20260605.json`
- `docs/evidence/phase17ag-smoke-post-terraform-nochange-20260605.txt`

Result:

- Bedrock was invoked through the managed Mistral workflow path
- workflow output does not emit token usage
- estimated direct model cost: `$0.00132618`
- practical budget cap: `$0.10`
- EventBridge schedule remains `DISABLED`
- Lambda remains on the Phase 17AE refreshed code hash
- post-run Terraform reports `No changes`
- recent execution evidence shows exactly one new Phase 17AG smoke execution

## Red-Green Evidence

Red:

- Phase 17AB found source-label public-surface drift in the workflow-published
  snapshot, and Phase 17AD/17AE showed the sanitizer was not deployed until the
  Lambda package refresh.

Green:

- Phase 17AG execution proved the refreshed deployed Lambda package can run the
  managed Bedrock/Mistral merge, publish a valid dashboard snapshot, and produce
  public-safe dashboard source labels.

Regression:

- one execution only
- no manual retry
- schedules remain disabled
- Terraform remains no-change
- generated artifacts validate
- failed artifacts are empty for the run

## Boundary

Phase 17AG execution is complete.

Next recommended state:

- Phase 17AH: managed workflow post-smoke dashboard cache verification
- keep the next slice read-only
- verify when the normal CloudFront latest path refreshes to the Phase 17AG
  snapshot, or make a separate invalidation decision
- keep schedule enablement as a later explicit decision boundary
