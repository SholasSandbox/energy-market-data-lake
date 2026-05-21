# Phase 14D Lambda Reconciliation Apply Summary

## State

- Branch: `feature/phase14d-lambda-reconciliation-apply`
- Approval: explicit user approval granted before apply
- Scope: ingestion Lambda reconciliation only
- Dashboard hosting apply: not run

## Pre-Apply Guardrail

The saved Terraform plan showed only the ingestion Lambda update:

```text
Plan: 0 to add, 1 to change, 0 to destroy.
```

Rollback package captured locally before apply:

```text
infra/terraform/lakehouse/.terraform/rollback/ingest-elexon-before-phase14d-20260521T141212Z.zip
```

Rollback package hash:

```text
LpuQEhsU45t3ne5cbEvumah4ljmMPwo8FaxzhW30Z/Y=
```

The rollback ZIP is intentionally not committed.

## Apply Result

Evidence:

```text
docs/evidence/phase14d-lambda-reconcile-apply-20260521.txt
```

Result:

```text
Apply complete! Resources: 0 added, 1 changed, 0 destroyed.
```

## Post-Apply Verification

Post-apply Lambda code hash:

```text
O+87gZ8+OMKKUwvzsXhA2sCVrAbDOwymkLU7MYS/Goc=
```

Post-apply tags:

```json
{
  "Environment": "dev",
  "ManagedBy": "terraform",
  "Project": "energy-market",
  "Workload": "energy-market-data-lake"
}
```

Lambda smoke invoke:

```text
StatusCode: 200
Handler status: ok
Warnings: []
```

S3 smoke object checked:

```text
raw/source=elexon/dataset=atl/date=2026-05-03/payload.json
```

Post-apply Terraform plan:

```text
No changes. Your infrastructure matches the configuration.
```

## Decision

Phase 14D is complete. The ingestion Lambda drift is reconciled.

The next safe state is to re-run the dashboard hosting plan with
`dashboard_cloudfront_enabled=true` and proceed only if the root plan is limited
to CloudFront distribution, OAC, response headers policy, and dashboard S3
bucket policy.

## Rollback Path

If ingestion regresses after this boundary:

1. Use the local rollback ZIP with `aws lambda update-function-code`.
2. Re-run the sanitized Lambda configuration check.
3. Re-run the one-day Lambda smoke invoke.
4. Keep dashboard hosting apply blocked until the post-rollback Terraform plan
   and ingestion smoke evidence are clean again.
