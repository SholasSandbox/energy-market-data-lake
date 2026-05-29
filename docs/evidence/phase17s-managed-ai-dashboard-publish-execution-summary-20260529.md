# Phase 17S Managed AI Dashboard Publish Execution Summary

<!-- markdownlint-disable MD013 -->

Date: 2026-05-29

## Boundary

Phase 17S execution published the approved Phase 17R managed AI dashboard
candidate to the live CloudFront-backed dashboard snapshot path.

No Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
EventBridge schedule enablement, DNS, ACM, alarm, budget, static-site rebuild,
or managed workflow deployment was performed.

## Red-Green Evidence

Red:

- Before publish, the live `dashboard_snapshot_v1.json` still served the Phase
  16 restored deterministic snapshot.
- Phase 17S decision required explicit approval before any S3 write or
  CloudFront invalidation.

Green:

- The approved Phase 17R candidate was uploaded as the latest
  `dashboard_snapshot_v1.json`.
- The same payload was uploaded as an immutable snapshot at
  `snapshots/run_id=managed-ai-phase17p-20260528T213401Z/dashboard_snapshot_v1.json`.
- CloudFront invalidation `I9MCXBX6M0BCO1HN0BWCKZO5H9` completed.
- CloudFront now serves the approved candidate at both latest and immutable
  paths.

Regression:

- Local managed AI adapter proof remains green.
- Local Phase 17R dashboard source-link proof remains green.
- The published CloudFront payload SHA256 matches the approved Phase 17R
  candidate SHA256.
- Managed handler/state-machine deployment remains blocked.

## Published Payload

- Candidate:
  `docs/evidence/phase17r-managed-ai-dashboard-source-link-candidate-20260529.json`
- Candidate SHA256:
  `d180b4a2bda131fae6088a650301f40b696ba67929ffcea78be42731adb3a741`
- Latest key: `dashboard_snapshot_v1.json`
- Immutable key:
  `snapshots/run_id=managed-ai-phase17p-20260528T213401Z/dashboard_snapshot_v1.json`

## Verification

- Latest CloudFront URL:
  `https://d28yo76if4k3l1.cloudfront.net/dashboard_snapshot_v1.json`
- Immutable CloudFront URL:
  `https://d28yo76if4k3l1.cloudfront.net/snapshots/run_id=managed-ai-phase17p-20260528T213401Z/dashboard_snapshot_v1.json`
- Both paths returned `HTTP/2 200`.
- Both paths returned `schema_version: dashboard_snapshot_v1`.
- Both paths returned SHA256:
  `d180b4a2bda131fae6088a650301f40b696ba67929ffcea78be42731adb3a741`
- Latest cache control: `no-cache`
- Immutable cache control: `public,max-age=31536000,immutable`

## Evidence Files

- `docs/evidence/phase17s-execution-aws-identity-sanitized-20260529.txt`
- `docs/evidence/phase17s-execution-prepublish-live-dashboard-snapshot-http-check-20260529.txt`
- `docs/evidence/phase17s-dashboard-publish-latest-s3-cp-20260529.txt`
- `docs/evidence/phase17s-dashboard-publish-immutable-s3-cp-20260529.txt`
- `docs/evidence/phase17s-dashboard-publish-latest-head-20260529.json`
- `docs/evidence/phase17s-dashboard-publish-immutable-head-20260529.json`
- `docs/evidence/phase17s-dashboard-publish-cloudfront-invalidation-20260529.json`
- `docs/evidence/phase17s-dashboard-publish-cloudfront-invalidation-status-20260529.json`
- `docs/evidence/phase17s-dashboard-publish-cloudfront-http-check-20260529.txt`

## Rollback Path

Restore the Phase 16 deterministic snapshot to the latest path and invalidate
only the latest snapshot:

```bash
aws s3 cp docs/evidence/phase16-dashboard-snapshot-v1-restored-20260522.json \
  s3://energy-market-dashboard-public-464975959576-20260511/dashboard_snapshot_v1.json \
  --content-type application/json \
  --cache-control no-cache

aws cloudfront create-invalidation \
  --distribution-id E2H9BGRGYAHKPN \
  --paths "/dashboard_snapshot_v1.json"
```

The immutable managed AI snapshot should remain as audit evidence unless a
separate cleanup phase explicitly removes it.

## Next Boundary

Recommended next slice: **Phase 17T: managed AI dashboard post-publish demo
verification**.

Phase 17T should be read-only: verify the hosted dashboard experience, update
demo notes, and keep Bedrock invocation, Terraform, IAM, schedules, DNS, ACM,
alarms, budgets, and managed workflow deployment out of scope.
