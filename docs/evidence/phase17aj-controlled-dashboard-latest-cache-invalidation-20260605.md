# Phase 17AJ Controlled Dashboard Latest Cache Invalidation

<!-- markdownlint-disable MD013 -->

Date: 2026-06-05

## Boundary

Phase 17AJ executed the explicitly approved cache-resolution action from Phase
17AI: one CloudFront invalidation for `/dashboard_snapshot_v1.json` only.

No Bedrock invocation, Step Functions execution, Terraform apply, IAM mutation,
Lambda deploy, Step Functions deploy, EventBridge schedule enablement, DNS,
ACM, alarm, budget, S3 write, static-site rebuild, broad CloudFront
invalidation, or dashboard publish was performed.

## Pre-Invalidation Evidence

- `docs/evidence/phase17aj-cache-invalidation-aws-identity-sanitized-20260605.txt`
- `docs/evidence/phase17aj-pre-invalidation-cloudfront-distribution-20260605.json`
- `docs/evidence/phase17aj-pre-invalidation-latest-snapshot-head-20260605.json`
- `docs/evidence/phase17aj-pre-invalidation-immutable-snapshot-head-20260605.json`
- `docs/evidence/phase17aj-pre-invalidation-latest-http-check-20260605.txt`
- `docs/evidence/phase17aj-pre-invalidation-immutable-http-check-20260605.txt`
- `docs/evidence/phase17aj-pre-invalidation-dashboard-routes-http-check-20260605.txt`
- `docs/evidence/phase17aj-pre-invalidation-schedule-state-20260605.json`
- `docs/evidence/phase17aj-pre-invalidation-recent-executions-20260605.json`

Pre-state result:

- latest S3 snapshot was already the Phase 17AG object with version
  `KByeeyWC.YWMJOzJ6OGYvlIn8xN7Et2f`.
- normal CloudFront latest path still served cached SHA-256
  `d4806fbbd0a2045ad1bc79c511601ad5f342ebe8a12fe276448cec1b6fb1d515`.
- normal CloudFront latest response still carried older version
  `b9PUPbupwFRcRCIHTcMwFhylWsuDCkSv`.
- immutable Phase 17AG CloudFront path served SHA-256
  `4c4871a2ff09f11ed097e4c03f637b34812d3893a1d2dbb97b0584cc7001d4c0`.
- EventBridge schedule was `DISABLED`.
- recent Step Functions evidence showed Phase 17AG remained the latest
  execution.

## Invalidation Evidence

- `docs/evidence/phase17aj-cache-invalidation-create-20260605.json`
- `docs/evidence/phase17aj-cache-invalidation-id-20260605.txt`
- `docs/evidence/phase17aj-cache-invalidation-status-20260605.json`

Result:

- invalidation ID: `I3IV0NIU4E4H7RQCPW0WGCKFTG`
- invalidation status: `Completed`
- invalidation path count: `1`
- invalidation path: `/dashboard_snapshot_v1.json`

No `/*`, static dashboard assets, immutable snapshot paths, S3 prefixes, or
additional paths were invalidated.

## Post-Invalidation Evidence

- `docs/evidence/phase17aj-post-invalidation-latest-http-check-20260605.txt`
- `docs/evidence/phase17aj-post-invalidation-immutable-http-check-20260605.txt`
- `docs/evidence/phase17aj-post-invalidation-latest-snapshot-head-20260605.json`
- `docs/evidence/phase17aj-post-invalidation-schedule-state-20260605.json`
- `docs/evidence/phase17aj-post-invalidation-recent-executions-20260605.json`
- `docs/evidence/phase17aj-post-invalidation-terraform-nochange-20260605.txt`
- `docs/evidence/phase17aj-cache-invalidation-summary-20260605.txt`

Post-state result:

- normal CloudFront latest path returns `200` and now serves SHA-256
  `4c4871a2ff09f11ed097e4c03f637b34812d3893a1d2dbb97b0584cc7001d4c0`.
- normal CloudFront latest response now carries Phase 17AG version
  `KByeeyWC.YWMJOzJ6OGYvlIn8xN7Et2f`.
- immutable Phase 17AG CloudFront path still returns `200` and serves SHA-256
  `4c4871a2ff09f11ed097e4c03f637b34812d3893a1d2dbb97b0584cc7001d4c0`.
- EventBridge schedule remains `DISABLED`.
- recent Step Functions evidence still shows Phase 17AG as the latest
  execution; Phase 17AJ did not start a workflow run.
- safe root Terraform plan with CloudFront and managed workflow flags preserved
  reports `No changes`.

## Red-Green Evidence

Red:

- Phase 17AI confirmed normal CloudFront latest was still serving the cached
  Phase 17AA snapshot even though S3 latest and the immutable Phase 17AG path
  were correct.

Green:

- Phase 17AJ invalidated exactly `/dashboard_snapshot_v1.json`, waited for
  completion, and verified normal CloudFront latest now serves the Phase 17AG
  snapshot.

Regression:

- no workflow execution occurred
- no Bedrock invocation occurred
- no S3 write occurred
- no Terraform apply occurred
- no schedule enablement occurred
- no broad invalidation occurred
- schedules remain disabled
- Terraform remains no-change with the live preservation flags

## Next Boundary

Recommended next slice: **Phase 17AK managed workflow post-cache demo
verification**, read-only.

Phase 17AK should verify the hosted dashboard demo path now that normal latest
and immutable Phase 17AG snapshot paths both serve the same validated snapshot.
Keep Step Functions execution, Bedrock invocation, Terraform apply, schedule
enablement, S3 writes, static-site publish, and CloudFront invalidation out of
scope.

## Proof Commands

```bash
npx markdownlint-cli2 README.md PLANS.md docs/demo-walkthrough.md \
  docs/phase-17-managed-ai-refresh-preflight.md \
  docs/evidence/phase17aj-controlled-dashboard-latest-cache-invalidation-20260605.md

python3 -m json.tool \
  docs/evidence/phase17aj-cache-invalidation-create-20260605.json

python3 -m json.tool \
  docs/evidence/phase17aj-cache-invalidation-status-20260605.json

python3 -m json.tool \
  docs/evidence/phase17aj-post-invalidation-latest-snapshot-head-20260605.json

python3 -m json.tool \
  docs/evidence/phase17aj-post-invalidation-schedule-state-20260605.json

python3 -m json.tool \
  docs/evidence/phase17aj-post-invalidation-recent-executions-20260605.json

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
