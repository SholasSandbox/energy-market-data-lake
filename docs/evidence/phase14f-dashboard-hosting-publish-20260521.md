# Dashboard Hosting Publish Evidence

<!-- markdownlint-disable MD013 -->

Generated at: 2026-05-21T14:45:38Z
Mode: apply
Git branch: feature/phase14e-dashboard-hosting-apply-candidate
Git commit: 2a5ab2b

## Inputs

- Dashboard dist: `/Users/shola/Workspace/cloud-projects/energy-market-data-lake/dashboard-ui/dist`
- Dashboard bucket: `energy-market-dashboard-public-464975959576-20260511`
- CloudFront distribution ID: `E2H9BGRGYAHKPN`
- CloudFront domain: `d28yo76if4k3l1.cloudfront.net`
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
aws cloudfront create-invalidation \
  --distribution-id E2H9BGRGYAHKPN \
  --paths "/index.html" "/assets/*" "/dashboard-data.json" "/dashboard_snapshot_v1.sample.json"
```

## Result

- AWS publish commands were executed.
