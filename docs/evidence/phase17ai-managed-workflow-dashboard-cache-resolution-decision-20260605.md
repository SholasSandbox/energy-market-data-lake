# Phase 17AI Managed Workflow Dashboard Cache Resolution Decision

<!-- markdownlint-disable MD013 -->

Date: 2026-06-05

## Boundary

Phase 17AI decided how to resolve the stale normal CloudFront latest snapshot
path observed in Phase 17AH.

No Bedrock invocation, Step Functions execution, Terraform apply, IAM mutation,
Lambda deploy, Step Functions deploy, EventBridge schedule enablement, DNS,
ACM, alarm, budget, S3 write, CloudFront invalidation, static-site rebuild, or
dashboard publish was performed.

## Evidence

- `docs/evidence/phase17ai-cache-resolution-aws-identity-sanitized-20260605.txt`
- `docs/evidence/phase17ai-cache-resolution-cloudfront-distribution-20260605.json`
- `docs/evidence/phase17ai-cache-resolution-cloudfront-cache-policy-20260605.json`
- `docs/evidence/phase17ai-cache-resolution-latest-snapshot-head-20260605.json`
- `docs/evidence/phase17ai-cache-resolution-immutable-snapshot-head-20260605.json`
- `docs/evidence/phase17ai-cache-resolution-latest-http-recheck-20260605.txt`
- `docs/evidence/phase17ai-cache-resolution-immutable-http-recheck-20260605.txt`
- `docs/evidence/phase17ai-cache-resolution-decision-summary-20260605.txt`
- `docs/evidence/phase17ai-cache-resolution-schedule-state-20260605.json`
- `docs/evidence/phase17ai-cache-resolution-recent-executions-20260605.json`
- `docs/evidence/phase17ai-cache-resolution-terraform-nochange-20260605.txt`

## Current State

- S3 latest snapshot remains the Phase 17AG object:
  `VersionId` `KByeeyWC.YWMJOzJ6OGYvlIn8xN7Et2f`, ETag
  `"2c239d74ed726990ec7322b7fe6228c9"`, and length `9246`.
- immutable Phase 17AG CloudFront snapshot path returns `200` and serves SHA-256
  `4c4871a2ff09f11ed097e4c03f637b34812d3893a1d2dbb97b0584cc7001d4c0`.
- normal CloudFront latest path returns `200`, but still serves cached SHA-256
  `d4806fbbd0a2045ad1bc79c511601ad5f342ebe8a12fe276448cec1b6fb1d515`.
- normal CloudFront latest response still carries older version
  `b9PUPbupwFRcRCIHTcMwFhylWsuDCkSv`, ETag
  `"78dc3e2733a818b8c876fc156ad905eb"`, and length `10154`.
- latest response is a CloudFront cache hit with `age: 2151`.
- CloudFront default behavior uses `Managed-CachingOptimized`, with
  `DefaultTTL` `86400` and `MaxTTL` `31536000`.
- EventBridge schedule remains `DISABLED`.
- safe root Terraform plan with CloudFront and managed workflow flags preserved
  reports `No changes`.

## Decision

Recommendation: **go-candidate for one controlled CloudFront invalidation, not
automatic execution**.

If approved in a later execution substate, invalidate only:

```text
/dashboard_snapshot_v1.json
```

Do not invalidate `/*`, static dashboard assets, immutable snapshot paths, or
S3 prefixes. Do not run Step Functions, invoke Bedrock, write S3 objects,
publish dashboard assets, apply Terraform, or enable schedules as part of the
cache-resolution execution.

Why this is preferred over wait-only:

- S3 latest already points to the validated Phase 17AG snapshot.
- immutable Phase 17AG path proves the object is publicly reachable.
- the stale normal latest response is a CloudFront cache hit, not an origin or
  schema failure.
- the active managed cache policy can keep the stale object for the default
  `86400` second TTL.
- a single-path invalidation is narrower and more deterministic than waiting
  for an unspecified demo-ready moment.

## Alternatives

Wait and recheck:

- safest from a mutation standpoint
- leaves the normal latest dashboard snapshot stale for the public demo path
- may take up to the managed cache policy TTL to converge

Controlled invalidation:

- mutates only CloudFront cache state
- does not change S3, Terraform, Step Functions, Lambda, IAM, schedules, or
  dashboard assets
- should make the normal latest path read the already-published Phase 17AG S3
  object after propagation

## Red-Green Evidence

Red:

- Phase 17AH confirmed the normal CloudFront latest path was still serving the
  cached Phase 17AA snapshot after Phase 17AG published the new S3 latest
  object.

Green:

- Phase 17AI identifies the stale response as cache state and narrows the next
  possible mutation to a single CloudFront object path.

Regression:

- no workflow execution occurred
- no dashboard mutation occurred
- no CloudFront invalidation occurred
- schedules remain disabled
- Terraform remains no-change with the live preservation flags

## Next Boundary

Recommended next slice: **Phase 17AJ controlled dashboard latest cache
invalidation execution**, only after explicit approval.

Phase 17AJ should capture pre-invalidation latest and immutable metadata, create
one invalidation for `/dashboard_snapshot_v1.json`, wait for completion, verify
normal latest CloudFront SHA-256 matches
`4c4871a2ff09f11ed097e4c03f637b34812d3893a1d2dbb97b0584cc7001d4c0`, and
confirm schedules remain disabled.

## Proof Commands

```bash
npx markdownlint-cli2 README.md PLANS.md docs/demo-walkthrough.md \
  docs/phase-17-managed-ai-refresh-preflight.md \
  docs/evidence/phase17ai-managed-workflow-dashboard-cache-resolution-decision-20260605.md

python3 -m json.tool \
  docs/evidence/phase17ai-cache-resolution-cloudfront-distribution-20260605.json

python3 -m json.tool \
  docs/evidence/phase17ai-cache-resolution-cloudfront-cache-policy-20260605.json

python3 -m json.tool \
  docs/evidence/phase17ai-cache-resolution-latest-snapshot-head-20260605.json

python3 -m json.tool \
  docs/evidence/phase17ai-cache-resolution-immutable-snapshot-head-20260605.json

python3 -m json.tool \
  docs/evidence/phase17ai-cache-resolution-schedule-state-20260605.json

python3 -m json.tool \
  docs/evidence/phase17ai-cache-resolution-recent-executions-20260605.json

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
