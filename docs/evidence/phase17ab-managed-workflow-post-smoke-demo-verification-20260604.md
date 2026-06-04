# Phase 17AB Managed Workflow Post-Smoke Demo Verification

<!-- markdownlint-disable MD013 -->

Date: 2026-06-04

## Boundary

Phase 17AB performed read-only hosted dashboard verification after the Phase
17AA managed workflow smoke published a new dashboard snapshot.

No Bedrock invocation, Step Functions execution, Terraform apply, IAM mutation,
Lambda deploy, Step Functions deploy, EventBridge schedule enablement, DNS,
ACM, alarm, budget, S3 write, CloudFront invalidation, static-site rebuild, or
dashboard publish was performed.

## Evidence

- `docs/evidence/phase17ab-managed-workflow-post-smoke-demo-http-check-20260604.txt`
- `docs/evidence/phase17ab-managed-workflow-post-smoke-demo-json-check-20260604.txt`
- `docs/evidence/phase17ab-managed-workflow-post-smoke-schedule-state-20260604.json`
- `docs/evidence/phase17ab-managed-workflow-post-smoke-recent-executions-20260604.json`

## Result

- CloudFront returned `200` for `/`, `/index.html`,
  `/dashboard-data.json`, latest `dashboard_snapshot_v1.json`, and the Phase
  17AA immutable run snapshot.
- latest and immutable snapshot paths both returned SHA-256
  `d4806fbbd0a2045ad1bc79c511601ad5f342ebe8a12fe276448cec1b6fb1d515`.
- latest and immutable snapshot paths validate against
  `dashboard_snapshot_v1`.
- latest and immutable snapshot payloads match each other.
- the public snapshot still reports `metadata.status: watch`.
- the primary insight is
  `UK Grid Expansion and Renewable Integration Risks Amid Policy Shifts`.
- recent execution evidence still shows the Phase 17AA execution as the latest
  Step Functions run; Phase 17AB did not start a new execution.
- EventBridge schedule remains `DISABLED`.

## Finding

Phase 17AB found one demo/public-surface hardening gap:

- the first source URL remains dashboard-safe as `dashboard-data.json`
- the first source label still carries private lake S3 context from the managed
  workflow snapshot
- the evidence file redacts the private lake reference rather than committing
  the account-specific path

This is not a schema failure and it does not make the source link target
unsafe, but it is public demo polish drift. The next implementation slice
should sanitize managed workflow source labels before any schedule enablement
or repeated managed workflow run.

## Red-Green Evidence

Red:

- Phase 17AA proved the managed workflow can publish the dashboard snapshot,
  but the hosted demo still needed read-only proof after cache propagation.

Green:

- Phase 17AB confirms the hosted dashboard routes, latest workflow snapshot,
  immutable workflow snapshot, and schema validation are healthy.

Regression:

- no workflow execution occurred
- no dashboard mutation occurred
- schedules remain disabled
- the source URL fallback remains public-safe
- the source label sanitization gap is captured as the next boundary

## Next Boundary

Recommended next slice: **Phase 17AC managed workflow source-label
sanitization**, local/preflight first.

Phase 17AC should keep Bedrock invocation, Step Functions execution, Terraform
apply, schedule enablement, S3 writes, CloudFront invalidation, and dashboard
publish out of scope until the source-label fix is proven locally.

## Proof Commands

```bash
npx markdownlint-cli2 README.md PLANS.md docs/demo-walkthrough.md \
  docs/phase-17-managed-ai-refresh-preflight.md \
  docs/evidence/phase17ab-managed-workflow-post-smoke-demo-verification-20260604.md

python3 -m json.tool \
  docs/evidence/phase17ab-managed-workflow-post-smoke-schedule-state-20260604.json

python3 -m json.tool \
  docs/evidence/phase17ab-managed-workflow-post-smoke-recent-executions-20260604.json

.venv/bin/python scripts/check_phase17a_managed_ai_adapter.py

.venv/bin/python scripts/check_phase17r_dashboard_source_links.py

.venv/bin/python scripts/validate_contracts.py --include-evidence \
  --check-failures

git diff --check
```
