# Phase 17AK Managed Workflow Post-Cache Demo Verification

<!-- markdownlint-disable MD013 -->

Date: 2026-06-05

## Boundary

Phase 17AK performed read-only hosted dashboard verification after Phase 17AJ
resolved the normal CloudFront latest snapshot cache.

No Bedrock invocation, Step Functions execution, Terraform apply, IAM mutation,
Lambda deploy, Step Functions deploy, EventBridge schedule enablement, DNS,
ACM, alarm, budget, S3 write, CloudFront invalidation, static-site rebuild, or
dashboard publish was performed.

## Evidence

- `docs/evidence/phase17ak-post-cache-demo-aws-identity-sanitized-20260605.txt`
- `docs/evidence/phase17ak-post-cache-demo-routes-http-check-20260605.txt`
- `docs/evidence/phase17ak-post-cache-demo-snapshot-http-check-20260605.txt`
- `docs/evidence/phase17ak-post-cache-demo-json-check-20260605.json`
- `docs/evidence/phase17ak-post-cache-demo-source-label-summary-20260605.txt`
- `docs/evidence/phase17ak-post-cache-demo-latest-snapshot-head-20260605.json`
- `docs/evidence/phase17ak-post-cache-demo-immutable-snapshot-head-20260605.json`
- `docs/evidence/phase17ak-post-cache-demo-recent-invalidations-20260605.json`
- `docs/evidence/phase17ak-post-cache-demo-schedule-state-20260605.json`
- `docs/evidence/phase17ak-post-cache-demo-recent-executions-20260605.json`
- `docs/evidence/phase17ak-post-cache-demo-terraform-nochange-20260605.txt`
- `docs/evidence/phase17ak-post-cache-demo-verification-summary-20260605.txt`

## Result

- hosted routes `/`, `/index.html`, `/dashboard-data.json`,
  `/dashboard_snapshot_v1.json`, and the Phase 17AG immutable snapshot path
  return `200`.
- normal CloudFront latest snapshot path serves SHA-256
  `4c4871a2ff09f11ed097e4c03f637b34812d3893a1d2dbb97b0584cc7001d4c0`.
- immutable Phase 17AG CloudFront snapshot path serves the same SHA-256
  `4c4871a2ff09f11ed097e4c03f637b34812d3893a1d2dbb97b0584cc7001d4c0`.
- latest and immutable snapshot payloads match.
- latest snapshot reports `schema_version: dashboard_snapshot_v1`.
- latest snapshot reports `metadata.status: watch`.
- primary insight title is
  `UK Demand Surge Risks Grid Stability Amid Smart Meter Rollout`.
- source-label summary found `0` private references.
- public snapshot sources are:
  - `dashboard-data.json`
  - `https://www.energyvoice.com/renewables-energy-transition/grid-retail/598797/centrica-smart-meter/`
- recent invalidation evidence shows the latest invalidation remains Phase
  17AJ invalidation `I3IV0NIU4E4H7RQCPW0WGCKFTG`, status `Completed`.
- recent Step Functions evidence still shows Phase 17AG as the latest
  execution; Phase 17AK did not start a workflow run.
- EventBridge schedule remains `DISABLED`.
- safe root Terraform plan with CloudFront and managed workflow flags preserved
  reports `No changes`.

## Red-Green Evidence

Red:

- Phase 17AH and Phase 17AI showed the normal CloudFront latest snapshot path
  was still cached on the Phase 17AA payload after the Phase 17AG workflow
  publish.

Green:

- Phase 17AK verifies that the hosted demo now serves the Phase 17AG snapshot
  through both normal latest and immutable snapshot paths after Phase 17AJ
  cache resolution.

Regression:

- no workflow execution occurred
- no Bedrock invocation occurred
- no S3 write occurred
- no Terraform apply occurred
- no schedule enablement occurred
- no CloudFront invalidation occurred
- source labels are public-safe
- schedules remain disabled
- Terraform remains no-change with the live preservation flags

## Next Boundary

Recommended next slice: **Phase 17AL managed workflow operating decision**,
decision-only.

Phase 17AL should decide whether the managed workflow is ready to remain a
manual-only proven path, move toward schedule enablement preflight, or pause
for portfolio/demo polish. Keep schedule enablement, workflow execution,
Bedrock invocation, Terraform apply, S3 writes, static-site publish, and
CloudFront invalidation out of scope until explicitly approved.

## Proof Commands

```bash
npx markdownlint-cli2 README.md PLANS.md docs/demo-walkthrough.md \
  docs/phase-17-managed-ai-refresh-preflight.md \
  docs/evidence/phase17ak-managed-workflow-post-cache-demo-verification-20260605.md

python3 -m json.tool \
  docs/evidence/phase17ak-post-cache-demo-json-check-20260605.json

python3 -m json.tool \
  docs/evidence/phase17ak-post-cache-demo-latest-snapshot-head-20260605.json

python3 -m json.tool \
  docs/evidence/phase17ak-post-cache-demo-immutable-snapshot-head-20260605.json

python3 -m json.tool \
  docs/evidence/phase17ak-post-cache-demo-recent-invalidations-20260605.json

python3 -m json.tool \
  docs/evidence/phase17ak-post-cache-demo-schedule-state-20260605.json

python3 -m json.tool \
  docs/evidence/phase17ak-post-cache-demo-recent-executions-20260605.json

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
