# Dashboard Hosting Publish Evidence

<!-- markdownlint-disable MD013 -->

Generated at: 2026-05-18T08:36:06Z
Mode: plan-only
Git branch: feature/phase13-dashboard-hosting-runbook-proof
Git commit: 810b387

## Inputs

- Dashboard dist: `/Users/shola/Workspace/cloud-projects/energy-market-data-lake/dashboard-ui/dist`
- Dashboard bucket: `energy-market-dashboard-public-464975959576-20260511`
- CloudFront distribution ID: `not configured`
- CloudFront domain: `not configured`
- AWS region: `eu-west-2`
- Asset count: 5

## Commands

```bash
npm --prefix dashboard-ui run build
.venv/bin/python scripts/validate_contracts.py --include-evidence --check-failures
aws s3 sync dashboard-ui/dist/ s3://energy-market-dashboard-public-464975959576-20260511/ \
  --delete \
  --exclude "assets/*" \
  --cache-control "no-cache"
aws s3 sync dashboard-ui/dist/assets/ s3://energy-market-dashboard-public-464975959576-20260511/assets/ \
  --delete \
  --cache-control "public,max-age=31536000,immutable"
```

## Result

- Plan-only mode. No AWS write commands were executed.
