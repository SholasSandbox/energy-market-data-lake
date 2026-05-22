# Phase 16 Live AI Dashboard Snapshot Restore Summary

Date: 2026-05-22

## Decision

Phase 16 restored the live AI dashboard snapshot by rebuilding the public-safe
`dashboard_snapshot_v1.json` from the successful Phase 8 curated artifacts for
run `ai-insight-20260511T114815Z-927685a3`.

This was safer than rerunning orchestration because it did not invoke managed
AI, change schedules, run Terraform, alter DNS/ACM, or touch the working
CloudFront-hosted React dashboard assets.

## Objects Restored

- `s3://energy-market-dashboard-public-464975959576-20260511/dashboard_snapshot_v1.json`
- `s3://energy-market-dashboard-public-464975959576-20260511/snapshots/run_id=ai-insight-20260511T114815Z-927685a3/dashboard_snapshot_v1.json`

## Verification

- Restored payload validates as `dashboard_snapshot_v1`.
- Latest snapshot object has `Content-Type: application/json` and
  `Cache-Control: no-cache`.
- Immutable snapshot object has `Content-Type: application/json` and
  `Cache-Control: public,max-age=31536000,immutable`.
- CloudFront invalidation `IDR5CTSS7OUOG7MA72SDXNSR6J` completed.
- CloudFront returned `200 OK` for:
  - `/`
  - `/index.html`
  - `/dashboard-data.json`
  - `/dashboard_snapshot_v1.sample.json`
  - `/dashboard_snapshot_v1.json`
  - `/snapshots/run_id=ai-insight-20260511T114815Z-927685a3/dashboard_snapshot_v1.json`

## Evidence Files

- `docs/evidence/phase16-dashboard-snapshot-v1-restored-20260522.json`
- `docs/evidence/phase16-dashboard-snapshot-latest-head-20260522.json`
- `docs/evidence/phase16-dashboard-snapshot-immutable-head-20260522.json`
- `docs/evidence/phase16-cloudfront-snapshot-invalidation-20260522.json`
- `docs/evidence/phase16-cloudfront-snapshot-invalidation-status-20260522.json`
- `docs/evidence/phase16-cloudfront-snapshot-http-check-20260522.txt`
- `docs/evidence/phase16-cloudfront-snapshot-http-json-check-20260522.txt`

## Rollback

If the restored snapshot needs to be removed without affecting the hosted React
dashboard, delete only these keys and invalidate the same CloudFront paths:

```bash
aws s3 rm s3://energy-market-dashboard-public-464975959576-20260511/dashboard_snapshot_v1.json
aws s3 rm s3://energy-market-dashboard-public-464975959576-20260511/snapshots/run_id=ai-insight-20260511T114815Z-927685a3/dashboard_snapshot_v1.json

aws cloudfront create-invalidation \
  --distribution-id E2H9BGRGYAHKPN \
  --paths \
    "/dashboard_snapshot_v1.json" \
    "/snapshots/run_id=ai-insight-20260511T114815Z-927685a3/dashboard_snapshot_v1.json"
```

Do not run `scripts/publish_dashboard_static_site.sh --apply` as a rollback
step unless the goal is to republish the static React build.
