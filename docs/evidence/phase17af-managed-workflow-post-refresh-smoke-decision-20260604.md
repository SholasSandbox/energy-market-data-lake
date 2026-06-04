# Phase 17AF Managed Workflow Post-Refresh Smoke Decision

<!-- markdownlint-disable MD013 -->

Date: 2026-06-04

## Scope

Phase 17AF reviewed whether one controlled managed workflow smoke is justified
after Phase 17AE execution refreshed the deployed AI orchestration Lambda
package with the Phase 17AC source-label sanitizer.

This was a decision-only slice:

- no Step Functions execution
- no Bedrock invocation
- no Terraform apply
- no IAM mutation
- no Lambda deploy
- no Step Functions deploy
- no EventBridge schedule enablement
- no S3 write or CloudFront invalidation
- no static-site rebuild or dashboard publish

## Read-Only Evidence

Lambda evidence:

- `docs/evidence/phase17af-smoke-decision-lambda-config-20260604.json`

Result:

- Lambda is `Active`
- Lambda remains in managed mode
- `BEDROCK_MODEL_ID` remains `mistral.ministral-3-8b-instruct`
- Lambda `CodeSha256` is
  `V/PZH22YFXzyYarXT+dglN/JJ0CasL0G1zFqbVFk1Zc=`
- the live code hash matches the Phase 17AE refreshed sanitizer package hash

Workflow evidence:

- `docs/evidence/phase17af-smoke-decision-state-machine-20260604.json`
- `docs/evidence/phase17af-smoke-decision-state-machine-routing-20260604.json`
- `docs/evidence/phase17af-smoke-decision-recent-executions-20260604.json`

Result:

- Step Functions state machine is `ACTIVE`
- `CreateAiInputBundle` routes to `MergeAiInsightManaged`
- `MergeAiInsightManaged` routes to `PublishDashboardSnapshot`
- `PublishDashboardSnapshot` remains the terminal success state
- recent execution evidence shows no new managed workflow run after the Phase
  17AA successful smoke

Schedule evidence:

- `docs/evidence/phase17af-smoke-decision-schedule-state-20260604.json`

Result:

- EventBridge schedule remains `DISABLED`

Dashboard and rollback evidence:

- `docs/evidence/phase17af-smoke-decision-dashboard-http-check-20260604.txt`
- `docs/evidence/phase17af-smoke-decision-current-run-id-20260604.txt`
- `docs/evidence/phase17af-smoke-decision-latest-snapshot-head-20260604.json`
- `docs/evidence/phase17af-smoke-decision-immutable-snapshot-head-20260604.json`

Result:

- CloudFront `dashboard_snapshot_v1.json` returns `200`
- latest snapshot ETag remains `"78dc3e2733a818b8c876fc156ad905eb"`
- current dashboard snapshot SHA-256 remains
  `d4806fbbd0a2045ad1bc79c511601ad5f342ebe8a12fe276448cec1b6fb1d515`
- the public dashboard snapshot schema does not expose a top-level run ID
- the current immutable Phase 17AA managed workflow snapshot remains available
  at
  `snapshots/run_id=ai-insight-20260603T010744Z-4d89a62a/dashboard_snapshot_v1.json`

Terraform evidence:

- `docs/evidence/phase17af-smoke-decision-terraform-nochange-20260604.txt`

Result:

- Terraform reports `No changes`
- no infrastructure apply is needed before a post-refresh smoke decision

## Decision

Phase 17AF is a **go-candidate** for one controlled managed workflow
post-refresh smoke, not automatic execution.

Execution remains blocked until explicit approval in a separate execution
substate.

If approved, the execution boundary must use these guardrails:

- one manual Step Functions execution maximum
- no manual retry
- keep EventBridge schedule disabled
- keep Terraform apply out of scope
- capture latest snapshot object metadata before start
- capture the immutable Phase 17AA managed workflow snapshot rollback reference
  before start
- capture execution ARN, execution history, final output, generated run ID, S3
  artifacts, and dashboard impact
- verify source labels in the produced dashboard snapshot are public-safe
- verify whether Bedrock was invoked and estimate model cost
- expect one managed Mistral call if the workflow reaches
  `MergeAiInsightManaged`
- keep the practical cost cap at `$0.10`
- confirm EventBridge schedule remains disabled after the run
- stop after the result is reviewed

## Cost And Risk

Expected direct Bedrock model cost is low if the run reaches one Mistral call.
Recent controlled Mistral calls were approximately `$0.0013` each. The smoke
also has nominal Step Functions, Lambda, S3, and CloudFront read/write costs.

Main risk:

- a successful smoke is publish-capable and can overwrite the latest public
  `dashboard_snapshot_v1.json`

Mitigation:

- capture latest and immutable rollback metadata before execution
- run once only
- verify dashboard object version, ETag, SHA-256, source labels, and CloudFront
  response after execution
- keep the immutable Phase 17AA managed workflow snapshot as rollback evidence

## Red-Green Evidence

Red:

- Phase 17AB found source-label public-surface drift in the managed workflow
  snapshot, and Phase 17AD/17AE showed the deployed Lambda was stale relative
  to the local sanitizer.

Green:

- Phase 17AE execution refreshed the deployed Lambda package, and Phase 17AF
  confirms the live code hash now matches the package containing
  `source_label_context`.

Regression:

- no workflow smoke was run
- no Bedrock invocation occurred
- no dashboard publish occurred
- EventBridge schedule remains disabled
- Terraform remains no-change

## Boundary

Phase 17AF decision is complete.

Next recommended state:

- Phase 17AG: controlled managed workflow post-refresh smoke execution
- run only after explicit approval
- preserve one-run discipline, no manual retry, rollback-first evidence,
  source-label verification, and schedule-disabled proof
