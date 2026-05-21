# Phase 15 CloudFront Demo Hardening Summary

## State

- Branch: `docs/phase15-cloudfront-demo-hardening`
- Start point: clean `main` after Phase 14F
- Scope: documentation, demo, and runbook hardening only
- Terraform apply: not run
- Live AI `dashboard_snapshot_v1.json` restore: intentionally deferred

## Hosted Dashboard

Live dashboard URL:

```text
https://d28yo76if4k3l1.cloudfront.net/
```

Verification evidence:

```text
docs/evidence/phase15-cloudfront-demo-http-check-20260521.txt
```

Verified paths:

```text
/: 200 OK
/index.html: 200 OK
/dashboard-data.json: 200 OK
/dashboard_snapshot_v1.sample.json: 200 OK
```

## Documentation Updated

- `docs/demo-walkthrough.md`
- `PLANS.md`
- `README.md`

## Demo Message

The dashboard is now CloudFront-hosted through a private S3 origin with Origin
Access Control. Private raw, curated, failed, and audit data remain behind the
lakehouse boundary. The public static dashboard serves only approved dashboard
assets and public-safe JSON.

## Known Follow-Up

The live AI `dashboard_snapshot_v1.json` restore remains deferred. The hosted
React demo currently verifies `dashboard_snapshot_v1.sample.json`, and the
publish script has already been hardened to preserve the live AI snapshot keys
on future static-site publishes.

## Deferred Boundaries

- DNS
- ACM certificate
- CloudWatch alarms
- EventBridge schedule automation
- Managed AI invocation through Bedrock or OpenClaw runtime
