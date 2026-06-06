# Phase 17AM Managed Workflow Schedule Enablement Preflight

<!-- markdownlint-disable MD013 -->

Date: 2026-06-06

## Boundary

Phase 17AM reviewed whether the proven manual managed workflow is ready to move
toward EventBridge schedule enablement.

This was a decision-only, no-apply preflight.

No Bedrock invocation, Step Functions execution, Terraform apply, IAM mutation,
Lambda deploy, Step Functions deploy, EventBridge schedule enablement, DNS,
ACM, alarm, budget, S3 write, CloudFront invalidation, static-site rebuild, or
dashboard publish was performed.

The fence for this slice allowed read-only AWS checks, dashboard HTTP/JSON
checks, and Terraform plan evidence only.

## Evidence

- `docs/evidence/phase17am-schedule-preflight-aws-identity-sanitized-20260606.txt`
- `docs/evidence/phase17am-schedule-preflight-lambda-config-sanitized-20260606.json`
- `docs/evidence/phase17am-schedule-preflight-state-machine-sanitized-20260606.json`
- `docs/evidence/phase17am-schedule-preflight-schedule-state-20260606.json`
- `docs/evidence/phase17am-schedule-preflight-schedule-targets-20260606.json`
- `docs/evidence/phase17am-schedule-preflight-failure-subscriptions-sanitized-20260606.json`
- `docs/evidence/phase17am-schedule-preflight-recent-executions-20260606.json`
- `docs/evidence/phase17am-schedule-preflight-dashboard-http-check-20260606.txt`
- `docs/evidence/phase17am-schedule-preflight-dashboard-json-check-20260606.json`
- `docs/evidence/phase17am-schedule-preflight-current-terraform-nochange-20260606.txt`
- `docs/evidence/phase17am-schedule-preflight-enable-candidate-plan-20260606.txt`
- `docs/evidence/phase17am-schedule-preflight-cost-frequency-summary-20260606.txt`
- `docs/evidence/phase17am-schedule-preflight-readiness-summary-20260606.txt`

## Current State

- Lambda `energy-market-news-ai-orchestration` is `Active` with
  `LastUpdateStatus: Successful`.
- Lambda environment remains in managed mode with:
  - `AI_ORCHESTRATION_MODE=managed`
  - `BEDROCK_PROVIDER=mistral`
  - `BEDROCK_MODEL_ID=mistral.ministral-3-8b-instruct`
  - `BEDROCK_MAX_TOKENS=1600`
  - `BEDROCK_TEMPERATURE=0.2`
- Step Functions remains `ACTIVE` and routes through `MergeAiInsightManaged`,
  then `PublishDashboardSnapshot`.
- latest Step Functions execution remains
  `phase17ag-managed-workflow-post-refresh-smoke-20260605T213352Z`, status
  `SUCCEEDED`.
- EventBridge rule `energy-market-ai-orchestration-schedule` remains
  `DISABLED`.
- EventBridge target points at the managed workflow state machine through the
  orchestration events role.
- hosted `dashboard_snapshot_v1.json` returns `200`.
- dashboard snapshot remains `dashboard_snapshot_v1` with metadata status
  `watch`.
- dashboard snapshot SHA-256 remains
  `4c4871a2ff09f11ed097e4c03f637b34812d3893a1d2dbb97b0584cc7001d4c0`.
- dashboard source labels remain public-safe with `0` private references.
- current preserved Terraform plan reports `No changes`.

## Schedule Candidate

Candidate schedule:

- expression: `cron(30 7 * * ? *)`
- interpreted operating posture: once daily at 07:30 UTC
- manual retries in this phase: `0`

Terraform candidate plan with the live CloudFront and managed workflow flags
preserved showed exactly one in-place change:

- `aws_cloudwatch_event_rule.ai_orchestration_schedule[0]`
- `state = "DISABLED" -> "ENABLED"`
- `Plan: 0 to add, 1 to change, 0 to destroy`

No Terraform apply was run.

## Cost And Alerting

Cost estimate:

- direct model cost estimate per run: `$0.00132618`
- estimate basis: Phase 17AG and Phase 17AA controlled managed workflow smoke
  cost summaries
- once-daily 30-day direct model estimate: `$0.03978540`
- once-daily 31-day direct model estimate: `$0.04111158`

This estimate excludes nominal Step Functions, Lambda, S3, CloudFront, and log
costs. More importantly, scheduled operation would make the model cost
recurring instead of manually bounded.

Failure notification finding:

- the failure SNS topic exists in the state-machine failure path
- current SNS subscription evidence shows `0` subscriptions
- therefore there is no evidenced human alert receiver for scheduled failures

## Decision

Decision: **no-go for immediate schedule enablement**.

Rationale:

- the managed workflow is deployed and manually proven
- the dashboard snapshot remains healthy and public-safe
- the candidate Terraform plan shape is narrow and no-destroy
- estimated direct model cost is low for a once-daily cadence
- but the failure notification topic has no subscriptions
- recurring automation should not be enabled without an evidenced alert path,
  explicit stop criteria, and a rollback/disable runbook

## Alternatives

Keep schedule disabled:

- preserves the proven manual operating state
- avoids recurring model calls without notification controls
- selected for this phase

Enable the schedule from the candidate plan:

- rejected for this phase
- Terraform plan shape is clean, but alerting and stop-control evidence is not
  ready

Add notification and stop-control preflight next:

- preferred next boundary
- keeps schedule enablement separate from notification/rollback readiness

## Red-Green Evidence

Red:

- Phase 17AL explicitly deferred schedule enablement until cost, alerting,
  rollback, freshness, plan shape, and stop criteria were reviewed.

Green:

- Phase 17AM proves the current manual posture is stable and the schedule
  enablement Terraform candidate is technically narrow.

Regression:

- no workflow execution occurred
- no Bedrock invocation occurred
- no S3 write occurred
- no Terraform apply occurred
- no schedule enablement occurred
- no CloudFront invalidation occurred
- schedules remain disabled
- dashboard snapshot remains public-safe

## Next Boundary

Recommended next slice: **Phase 17AN managed workflow failure notification and
stop-control preflight**, decision-only/no-apply.

Phase 17AN should decide whether to add or verify a failure notification
receiver, document the schedule-disable rollback command, define stop criteria,
and decide whether a later schedule enablement execution boundary is justified.

## Proof Commands

```bash
npx markdownlint-cli2 README.md PLANS.md docs/demo-walkthrough.md \
  docs/git-state-command-reference.md \
  docs/phase-17-managed-ai-refresh-preflight.md \
  docs/evidence/phase17am-managed-workflow-schedule-enablement-preflight-20260606.md

python3 -m json.tool \
  docs/evidence/phase17am-schedule-preflight-lambda-config-sanitized-20260606.json

python3 -m json.tool \
  docs/evidence/phase17am-schedule-preflight-state-machine-sanitized-20260606.json

python3 -m json.tool \
  docs/evidence/phase17am-schedule-preflight-schedule-state-20260606.json

python3 -m json.tool \
  docs/evidence/phase17am-schedule-preflight-schedule-targets-20260606.json

python3 -m json.tool \
  docs/evidence/phase17am-schedule-preflight-failure-subscriptions-sanitized-20260606.json

python3 -m json.tool \
  docs/evidence/phase17am-schedule-preflight-recent-executions-20260606.json

python3 -m json.tool \
  docs/evidence/phase17am-schedule-preflight-dashboard-json-check-20260606.json

.venv/bin/python scripts/check_phase17a_managed_ai_adapter.py

.venv/bin/python scripts/check_phase17r_dashboard_source_links.py

.venv/bin/python scripts/check_phase17ac_source_label_sanitization.py

.venv/bin/python scripts/validate_contracts.py --include-evidence \
  --check-failures

terraform -chdir=infra/terraform/lakehouse validate

git diff --check
```
