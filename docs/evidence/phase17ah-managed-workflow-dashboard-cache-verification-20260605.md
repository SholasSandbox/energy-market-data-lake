# Phase 17AH Managed Workflow Dashboard Cache Verification

<!-- markdownlint-disable MD013 -->

Date: 2026-06-05

## Boundary

Phase 17AH performed read-only dashboard cache verification after the Phase
17AG managed workflow smoke published a new dashboard snapshot.

No Bedrock invocation, Step Functions execution, Terraform apply, IAM mutation,
Lambda deploy, Step Functions deploy, EventBridge schedule enablement, DNS,
ACM, alarm, budget, S3 write, CloudFront invalidation, static-site rebuild, or
dashboard publish was performed.

## Evidence

- `docs/evidence/phase17ah-cache-verification-aws-identity-sanitized-20260605.txt`
- `docs/evidence/phase17ah-cache-verification-cloudfront-distribution-20260605.json`
- `docs/evidence/phase17ah-cache-verification-latest-snapshot-head-20260605.json`
- `docs/evidence/phase17ah-cache-verification-immutable-snapshot-head-20260605.json`
- `docs/evidence/phase17ah-cache-verification-latest-http-check-20260605.txt`
- `docs/evidence/phase17ah-cache-verification-immutable-http-check-20260605.txt`
- `docs/evidence/phase17ah-cache-verification-dashboard-routes-http-check-20260605.txt`
- `docs/evidence/phase17ah-cache-verification-summary-20260605.txt`
- `docs/evidence/phase17ah-cache-verification-schedule-state-20260605.json`
- `docs/evidence/phase17ah-cache-verification-recent-executions-20260605.json`
- `docs/evidence/phase17ah-cache-verification-terraform-nochange-20260605.txt`

## Result

- S3 latest snapshot remains the Phase 17AG object:
  `VersionId` `KByeeyWC.YWMJOzJ6OGYvlIn8xN7Et2f`, ETag
  `"2c239d74ed726990ec7322b7fe6228c9"`, and length `9246`.
- S3 immutable Phase 17AG snapshot remains present at
  `snapshots/run_id=ai-insight-20260605T213354Z-88068c72/dashboard_snapshot_v1.json`.
- immutable Phase 17AG CloudFront snapshot path returns `200` and serves SHA-256
  `4c4871a2ff09f11ed097e4c03f637b34812d3893a1d2dbb97b0584cc7001d4c0`.
- normal CloudFront latest path returns `200`, but still serves cached SHA-256
  `d4806fbbd0a2045ad1bc79c511601ad5f342ebe8a12fe276448cec1b6fb1d515`.
- normal CloudFront latest response still carries the older S3 version
  `b9PUPbupwFRcRCIHTcMwFhylWsuDCkSv` and ETag
  `"78dc3e2733a818b8c876fc156ad905eb"`.
- CloudFront distribution `E2H9BGRGYAHKPN` is `Deployed` and enabled.
- hosted routes `/`, `/index.html`, and `/dashboard-data.json` return `200`.
- recent Step Functions evidence still shows Phase 17AG as the latest
  execution; Phase 17AH did not start a new execution.
- EventBridge schedule remains `DISABLED`.
- safe root Terraform plan with CloudFront and managed workflow flags preserved
  reports `No changes`.

## Finding

Phase 17AH confirms that the Phase 17AG snapshot is published and healthy at
the S3 object layer and immutable CloudFront path, but the normal CloudFront
latest path has not refreshed yet.

This is cache state, not a schema or workflow failure. The read-only boundary
was preserved and no invalidation was requested.

## Red-Green Evidence

Red:

- Phase 17AG published a new latest S3 snapshot, but normal CloudFront latest
  still served the Phase 17AA cached object during the post-run checks.

Green:

- Phase 17AH confirms the immutable Phase 17AG path is healthy and the normal
  latest path behavior is now explicitly evidenced as stale cache state.

Regression:

- no workflow execution occurred
- no dashboard mutation occurred
- no CloudFront invalidation occurred
- schedules remain disabled
- Terraform remains no-change when live CloudFront and managed workflow flags
  are preserved

## Next Boundary

Recommended next slice: **Phase 17AI managed workflow dashboard cache
resolution decision**, not automatic invalidation.

Phase 17AI should decide whether to wait and recheck the normal latest path or
request one controlled CloudFront invalidation. Keep Step Functions execution,
Bedrock invocation, Terraform apply, schedule enablement, S3 writes, and static
site publish out of scope.

## Proof Commands

```bash
npx markdownlint-cli2 README.md PLANS.md docs/demo-walkthrough.md \
  docs/phase-17-managed-ai-refresh-preflight.md \
  docs/evidence/phase17ah-managed-workflow-dashboard-cache-verification-20260605.md

python3 -m json.tool \
  docs/evidence/phase17ah-cache-verification-cloudfront-distribution-20260605.json

python3 -m json.tool \
  docs/evidence/phase17ah-cache-verification-latest-snapshot-head-20260605.json

python3 -m json.tool \
  docs/evidence/phase17ah-cache-verification-immutable-snapshot-head-20260605.json

python3 -m json.tool \
  docs/evidence/phase17ah-cache-verification-schedule-state-20260605.json

python3 -m json.tool \
  docs/evidence/phase17ah-cache-verification-recent-executions-20260605.json

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
