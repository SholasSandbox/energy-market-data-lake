# Phase 12: Dashboard Hosting Foundation

Use this checklist to move from local React dashboard proof toward a
CloudFront-fronted public dashboard delivery path while preserving the private
lakehouse boundary.

## Goal

Add Terraform support for optional static dashboard delivery:

```text
approved dashboard build artifacts -> private dashboard S3 bucket ->
CloudFront Origin Access Control -> HTTPS public dashboard endpoint
```

This phase creates the infrastructure foundation. It does not apply live AWS
changes or enable broader operating automation by itself.

## Branch

```text
feature/phase12-dashboard-hosting-foundation
```

## Current State

- Phase 10 dashboard overview is complete.
- Phase 11 deterministic filters are complete.
- Phase 8 publishes approved public-safe dashboard snapshots to a separate
  dashboard bucket.
- Terraform can optionally create the dashboard bucket, but it did not yet
  define CloudFront delivery for that bucket.

## Target State

- Terraform has an optional CloudFront distribution for the Terraform-managed
  dashboard bucket.
- CloudFront uses Origin Access Control so the dashboard bucket can remain
  private.
- Static delivery remains disabled by default.
- Operators have clear variables, outputs, and proof commands before any live
  apply.

## Scope Boundary

In scope:

- CloudFront distribution for the dashboard static bucket.
- Origin Access Control for private S3 access.
- S3 bucket policy allowing only the CloudFront distribution to read objects.
- Security response headers for the public dashboard surface.
- Terraform variables, outputs, tfvars example, and docs.

Out of scope:

- Applying Terraform to AWS in this chat.
- DNS, ACM certificates, or custom domains.
- CloudWatch alarms and budgets.
- Enabling EventBridge schedules.
- Bedrock or OpenClaw managed model invocation.
- Changing the dashboard React behavior.

## Implementation Checklist

### 1. Terraform Foundation

- [x] Add `dashboard_cloudfront_enabled`.
- [x] Add `dashboard_cloudfront_price_class`.
- [x] Add CloudFront Origin Access Control.
- [x] Add CloudFront distribution for the dashboard bucket.
- [x] Add security response headers policy.
- [x] Add S3 bucket policy for CloudFront-only reads.
- [x] Add CloudFront outputs.

### 2. Documentation

- [x] Update Terraform README with enablement notes.
- [x] Update `terraform.tfvars.example`.
- [x] Update platform planning docs.
- [x] Keep live hosting clearly separate from local dashboard proof.

### 3. Verification

- [x] Run Terraform formatting.
- [x] Run Terraform validation.
- [x] Run React dashboard build.
- [x] Run contract validation.
- [x] Run Markdown lint.
- [x] Run `git diff --check`.

Verification notes:

- `terraform fmt -check -recursive infra/terraform/lakehouse`
- `terraform validate`
- `npm --prefix dashboard-ui run build`
- `.venv/bin/python scripts/validate_contracts.py --include-evidence --check-failures`
- `npx markdownlint-cli2 README.md PLANS.md docs/target-operating-model.md`
- `npx markdownlint-cli2 docs/phase-12-dashboard-hosting-foundation.md infra/terraform/lakehouse/README.md`
- `git diff --check`

## Enablement Notes

The CloudFront distribution is intentionally off by default. To plan it for a
Terraform-managed dashboard bucket:

```hcl
create_dashboard_bucket          = true
dashboard_bucket_name            = "energy-market-dashboard-public-<unique-suffix>"
dashboard_cloudfront_enabled     = true
dashboard_cloudfront_price_class = "PriceClass_100"
```

Then validate and plan from the Terraform root:

```bash
cd infra/terraform/lakehouse
terraform fmt -check
terraform validate
terraform plan
```

After a live apply, publish the React build artifacts and approved public JSON
to the dashboard bucket, then invalidate CloudFront paths that changed:

```bash
DISTRIBUTION_ID="$(terraform output -raw dashboard_cloudfront_distribution_id)"

aws cloudfront create-invalidation \
  --distribution-id "${DISTRIBUTION_ID}" \
  --paths "/index.html" "/assets/*" "/dashboard-data.json" "/dashboard_snapshot_v1.sample.json"
```

## Definition Of Done

- Terraform can describe the private S3 plus CloudFront delivery boundary.
- Static hosting remains opt-in.
- The public dashboard surface reads only approved dashboard assets.
- The implementation can be validated without changing live AWS state.
