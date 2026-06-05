# Phase 17AL Managed Workflow Operating Decision

<!-- markdownlint-disable MD013 -->

Date: 2026-06-06

## Boundary

Phase 17AL decided the operating posture after Phase 17AK verified the hosted
dashboard demo following cache resolution.

No Bedrock invocation, Step Functions execution, Terraform apply, IAM mutation,
Lambda deploy, Step Functions deploy, EventBridge schedule enablement, DNS,
ACM, alarm, budget, S3 write, CloudFront invalidation, static-site rebuild, or
dashboard publish was performed.

## Evidence

- `docs/evidence/phase17al-operating-decision-aws-identity-sanitized-20260606.txt`
- `docs/evidence/phase17al-operating-decision-lambda-config-sanitized-20260606.json`
- `docs/evidence/phase17al-operating-decision-state-machine-sanitized-20260606.json`
- `docs/evidence/phase17al-operating-decision-cloudfront-distribution-20260606.json`
- `docs/evidence/phase17al-operating-decision-latest-snapshot-head-20260606.json`
- `docs/evidence/phase17al-operating-decision-immutable-snapshot-head-20260606.json`
- `docs/evidence/phase17al-operating-decision-snapshot-http-check-20260606.txt`
- `docs/evidence/phase17al-operating-decision-dashboard-json-check-20260606.json`
- `docs/evidence/phase17al-operating-decision-schedule-state-20260606.json`
- `docs/evidence/phase17al-operating-decision-recent-executions-20260606.json`
- `docs/evidence/phase17al-operating-decision-recent-invalidations-20260606.json`
- `docs/evidence/phase17al-operating-decision-terraform-nochange-20260606.txt`
- `docs/evidence/phase17al-operating-decision-summary-20260606.txt`

## Current State

- Lambda `energy-market-news-ai-orchestration` is `Active` with
  `LastUpdateStatus: Successful`.
- Lambda environment is in managed mode with:
  - `AI_ORCHESTRATION_MODE=managed`
  - `BEDROCK_PROVIDER=mistral`
  - `BEDROCK_MODEL_ID=mistral.ministral-3-8b-instruct`
  - `BEDROCK_MAX_TOKENS=1600`
- Step Functions definition routes through `MergeAiInsightManaged`, then
  `PublishDashboardSnapshot`.
- latest Step Functions execution remains
  `phase17ag-managed-workflow-post-refresh-smoke-20260605T213352Z`, status
  `SUCCEEDED`.
- EventBridge schedule remains `DISABLED`.
- latest and immutable Phase 17AG snapshot paths both return `200`.
- latest and immutable snapshot paths both serve SHA-256
  `4c4871a2ff09f11ed097e4c03f637b34812d3893a1d2dbb97b0584cc7001d4c0`.
- latest and immutable snapshot payloads match.
- public snapshot source labels remain public-safe with `0` private references.
- latest CloudFront invalidation remains Phase 17AJ invalidation
  `I3IV0NIU4E4H7RQCPW0WGCKFTG`, status `Completed`.
- safe root Terraform plan with CloudFront and managed workflow flags preserved
  reports `No changes`.

## Decision

Decision: **keep the managed workflow as a manual-only proven path for now**.

Immediate schedule enablement is **no-go** in this phase.

Rationale:

- the managed workflow has now run successfully through the deployed Lambda and
  Step Functions path
- the hosted dashboard demo is healthy after cache resolution
- source labels are public-safe
- Terraform is no-change with the live preservation flags
- the EventBridge schedule remains disabled by design
- schedule enablement needs its own operating preflight for cost posture,
  alerting/rollback checks, frequency, freshness expectations, and demo impact

## Alternatives

Keep manual-only proven path:

- preserves the proven state
- avoids unbounded scheduled Bedrock calls
- keeps demos reproducible with explicit evidence
- selected for this phase

Move directly to schedule enablement:

- rejected for this phase
- would turn a manually proven path into recurring automation without a fresh
  schedule-specific preflight

Pause for portfolio/demo polish:

- acceptable as a product direction
- does not replace the need for a schedule enablement preflight if automation
  is later desired

## Red-Green Evidence

Red:

- earlier Phase 17 slices showed that live managed workflow deployment,
  package refresh, source-label hardening, and CloudFront cache behavior each
  needed separate proof before any operating automation.

Green:

- Phase 17AL confirms the managed workflow is deployed, manually proven,
  dashboard-visible, public-safe, and stable with schedules disabled.

Regression:

- no workflow execution occurred
- no Bedrock invocation occurred
- no S3 write occurred
- no Terraform apply occurred
- no schedule enablement occurred
- no CloudFront invalidation occurred
- schedules remain disabled
- Terraform remains no-change with the live preservation flags

## Next Boundary

Recommended next slice: **Phase 17AM managed workflow schedule enablement
preflight**, decision-only/no-apply.

Phase 17AM should decide whether schedule enablement is ready by reviewing
frequency, estimated model cost, failure notification path, rollback posture,
freshness expectations, no-destroy Terraform plan shape, and explicit stop
criteria. It should not enable schedules or run the workflow unless a later
execution phase explicitly approves those actions.

## Proof Commands

```bash
npx markdownlint-cli2 README.md PLANS.md docs/demo-walkthrough.md \
  docs/phase-17-managed-ai-refresh-preflight.md \
  docs/evidence/phase17al-managed-workflow-operating-decision-20260606.md

python3 -m json.tool \
  docs/evidence/phase17al-operating-decision-lambda-config-sanitized-20260606.json

python3 -m json.tool \
  docs/evidence/phase17al-operating-decision-state-machine-sanitized-20260606.json

python3 -m json.tool \
  docs/evidence/phase17al-operating-decision-cloudfront-distribution-20260606.json

python3 -m json.tool \
  docs/evidence/phase17al-operating-decision-dashboard-json-check-20260606.json

python3 -m json.tool \
  docs/evidence/phase17al-operating-decision-schedule-state-20260606.json

python3 -m json.tool \
  docs/evidence/phase17al-operating-decision-recent-executions-20260606.json

python3 -m json.tool \
  docs/evidence/phase17al-operating-decision-recent-invalidations-20260606.json

.venv/bin/python scripts/check_phase17a_managed_ai_adapter.py

.venv/bin/python scripts/check_phase17r_dashboard_source_links.py

.venv/bin/python scripts/check_phase17ac_source_label_sanitization.py

.venv/bin/python scripts/validate_contracts.py --include-evidence \
  --check-failures

terraform -chdir=infra/terraform/lakehouse plan -no-color \
  -var 'create_dashboard_bucket=true' \
  -var 'dashboard_cloudfront_enabled=true' \
  -var 'ai_orchestration_managed_ai_enabled=true'

git diff --check
```
