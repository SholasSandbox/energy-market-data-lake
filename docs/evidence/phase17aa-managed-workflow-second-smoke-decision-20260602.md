# Phase 17AA Managed Workflow Second-Smoke Decision

Date: 2026-06-02

## Scope

Phase 17AA reviewed whether one controlled second managed workflow smoke is
justified after Phase 17Z execution refreshed the deployed AI orchestration
Lambda package.

Guardrails:

- no Step Functions execution
- no Bedrock invocation
- no Terraform apply
- no Lambda deploy
- no EventBridge schedule enablement
- no S3 write or CloudFront invalidation
- no dashboard publish

## Read-Only Evidence

Lambda evidence:

- `docs/evidence/phase17aa-second-smoke-decision-lambda-config-20260602.json`

Result:

- Lambda is `Active`
- Lambda remains in managed mode
- `BEDROCK_MODEL_ID` remains `mistral.ministral-3-8b-instruct`
- Lambda `CodeSha256` is `Eeeg+InzSBuAUcrQPN9glMbw3hSWLBPkspiH0Ly2puE=`
- the live code hash matches the Phase 17Z refreshed package hash

Workflow evidence:

- `docs/evidence/phase17aa-second-smoke-decision-state-machine-20260602.json`
- `docs/evidence/phase17aa-second-smoke-decision-recent-executions-20260602.json`

Result:

- Step Functions state machine is `ACTIVE`
- `CreateAiInputBundle` routes to `MergeAiInsightManaged`
- `MergeAiInsightManaged` routes to `PublishDashboardSnapshot`
- `PublishDashboardSnapshot` is the terminal success state
- recent execution evidence shows no new managed workflow run after the Phase
  17Y failed smoke

Schedule evidence:

- `docs/evidence/phase17aa-second-smoke-decision-schedule-state-20260602.json`

Result:

- EventBridge schedule remains `DISABLED`

Dashboard and rollback evidence:

- `docs/evidence/phase17aa-second-smoke-decision-dashboard-http-check-20260602.txt`
- `docs/evidence/phase17aa-second-smoke-decision-latest-snapshot-head-20260602.json`
- `docs/evidence/phase17aa-second-smoke-decision-immutable-snapshot-head-20260602.json`

Result:

- CloudFront `dashboard_snapshot_v1.json` returns `200`
- latest snapshot ETag remains `"c341541722f25da0ab5dddf6fe9a2f21"`
- latest snapshot version remains `qYxpit3hmGzpSByvhG07nrOG4kBrz1qn`
- immutable managed AI snapshot remains available at
  `snapshots/run_id=managed-ai-phase17p-20260528T213401Z/dashboard_snapshot_v1.json`
- dashboard snapshot SHA-256 remains
  `d180b4a2bda131fae6088a650301f40b696ba67929ffcea78be42731adb3a741`

Terraform evidence:

- `docs/evidence/phase17aa-second-smoke-decision-terraform-nochange-20260602.txt`

Result:

- Terraform reports `No changes`
- no infrastructure apply is needed before a second smoke decision

## Decision

Phase 17AA is a go-candidate for one controlled second managed workflow smoke,
not automatic execution.

Execution remains blocked until explicit approval in a separate execution
substate.

If approved, the execution boundary must use these guardrails:

- one manual Step Functions execution maximum
- no manual retry
- capture rollback snapshot metadata before execution
- capture execution ARN, history, output, generated run ID, S3 artifacts, and
  dashboard impact
- verify whether Bedrock was invoked and estimate model cost
- expect one managed Mistral call if the workflow reaches
  `MergeAiInsightManaged`
- keep the practical cost cap at `$0.10`
- confirm EventBridge schedule remains disabled after the run
- do not run Terraform apply
- do not enable schedules

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
- verify dashboard object version, ETag, SHA-256, and CloudFront response after
  execution
- keep the immutable Phase 17S managed AI snapshot as rollback evidence

## Red-Green Evidence

Red:

- Phase 17Y proved managed workflow routing reached `MergeAiInsightManaged`,
  but failed because the deployed Lambda package was stale.

Green:

- Phase 17Z execution refreshed the deployed Lambda package, and Phase 17AA
  confirms the live code hash now matches the refreshed package.

Regression:

- no second workflow smoke was run
- no Bedrock invocation occurred
- no dashboard publish occurred
- EventBridge schedule remains disabled
- Terraform remains no-change

## Boundary

Phase 17AA decision is complete.

Next recommended state:

- Phase 17AA execution substate: one controlled second managed workflow smoke
- run only after explicit approval
- preserve one-run discipline, no manual retry, rollback-first evidence, and
  schedule-disabled proof
