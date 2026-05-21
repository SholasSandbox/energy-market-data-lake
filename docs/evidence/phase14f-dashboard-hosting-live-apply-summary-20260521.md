# Phase 14F Dashboard Hosting Live Apply Summary

## State

- Branch: `feature/phase14f-dashboard-hosting-live-apply`
- Approval: explicit user approval granted before apply
- Scope: dashboard CloudFront/S3 hosting only
- DNS, ACM, alarms, schedules, and managed AI invocation: not changed

## Apply Result

Evidence:

- `docs/evidence/phase14f-dashboard-hosting-apply-20260521.txt`
- `docs/evidence/phase14f-dashboard-hosting-post-apply-outputs-20260521.json`
- `docs/evidence/phase14f-cloudfront-distribution-20260521.json`
- `docs/evidence/phase14f-dashboard-bucket-policy-20260521.json`

Result:

```text
Apply complete! Resources: 4 added, 0 changed, 0 destroyed.
```

Created resources:

- CloudFront distribution: `E2H9BGRGYAHKPN`
- CloudFront domain: `d28yo76if4k3l1.cloudfront.net`
- Origin Access Control: `E3TCE5PD0QBXWX`
- Response headers policy: `7c4e1883-9862-4feb-9504-a01908d6d6f6`
- Dashboard bucket policy scoped to the CloudFront distribution

## Publish Result

Evidence:

- `docs/evidence/phase14f-dashboard-hosting-publish-20260521.md`
- `docs/evidence/phase14f-dashboard-hosting-publish-output-20260521.txt`
- `docs/evidence/phase14f-cloudfront-invalidation-20260521.json`
- `docs/evidence/phase14f-dashboard-bucket-objects-20260521.txt`

Result:

- Dashboard build passed.
- Contract validation passed.
- Static files were synced to S3.
- CloudFront invalidation `I5Y1IB9UV28LPLKATYRN0ELGM1` completed.

## HTTP Verification

Evidence:

- `docs/evidence/phase14f-cloudfront-http-headers-20260521.txt`

Results:

```text
/index.html: 200 OK
/dashboard-data.json: 200 OK
/dashboard_snapshot_v1.sample.json: 200 OK
```

Security headers include HSTS, `DENY` frame options, `nosniff`, and
`strict-origin-when-cross-origin`.

## Post-Apply Plan

Evidence:

- `docs/evidence/phase14f-dashboard-hosting-post-apply-nochange-plan-20260521.txt`

Result:

```text
No changes. Your infrastructure matches the configuration.
```

## Publish Script Hardening

The initial static-site publish removed older AI snapshot keys from the
dashboard bucket because the root S3 sync used `--delete`:

- `dashboard_snapshot_v1.json`
- `snapshots/run_id=ai-insight-20260511T114236Z-cf2343fc/dashboard_snapshot_v1.json`
- `snapshots/run_id=ai-insight-20260511T114815Z-927685a3/dashboard_snapshot_v1.json`

The live React dashboard proof is healthy because the app currently loads
`dashboard_snapshot_v1.sample.json`. The publish script was then hardened so
future static-site publishes preserve `dashboard_snapshot_v1.json` and
`snapshots/*`.

Plan-only hardening evidence:

- `docs/evidence/phase14f-dashboard-hosting-publish-preserve-snapshots-plan-20260521.md`
- `docs/evidence/phase14f-dashboard-hosting-publish-preserve-snapshots-plan-output-20260521.txt`

## Decision

Phase 14F is complete. The public-safe dashboard is now CloudFront-hosted and
verified.

## Next Boundary

Decide whether to repopulate the live AI `dashboard_snapshot_v1.json` through a
Phase 8 publish rerun or controlled snapshot restore. Keep DNS, ACM, alarms,
schedules, and managed AI invocation deferred until a dedicated phase targets
those operating boundaries.
