# Phase 14C Lambda Reconciliation Decision Evidence

## State

- Branch: `feature/phase14c-lambda-reconciliation-decision`
- Start point: clean `main` after Phase 14B
- Apply status: no `terraform apply` was run
- Dashboard hosting status: still blocked

## Inputs Reviewed

- `PLANS.md`
- `docs/target-operating-model.md`
- `docs/phase-14-dashboard-hosting-live-apply-evidence.md`
- `docs/evidence/phase14b-dashboard-hosting-refreshfalse-plan-20260520.txt`
- `infra/terraform/lakehouse/lambda.tf`
- Terraform state for `aws_lambda_function.ingest`
- Sanitized live AWS Lambda configuration

## Evidence Files

- `docs/evidence/phase14c-root-lambda-reconcile-plan-20260520.txt`
- `docs/evidence/phase14c-ingest-lambda-current-config-sanitized-20260520.json`
- `docs/evidence/phase14c-ingest-lambda-current-tags-20260520.json`

## Findings

The normal root Terraform plan, with dashboard CloudFront still disabled,
contains only the ingestion Lambda reconciliation:

```text
Plan: 0 to add, 1 to change, 0 to destroy.
```

The plan updates `aws_lambda_function.ingest` in place by adding Terraform-owned
package metadata, `source_code_hash`, `publish = false`, and standard tags.

Live deployed Lambda package hash:

```text
LpuQEhsU45t3ne5cbEvumah4ljmMPwo8FaxzhW30Z/Y=
```

Terraform-built local package hash:

```text
O+87gZ8+OMKKUwvzsXhA2sCVrAbDOwymkLU7MYS/Goc=
```

Extracted deployed source file hash and local source file hash both match:

```text
525ef7109341258906f3ed6b6fbc0ce829666cb8cbfd06c53df78e46caed4997
```

Live Lambda tags are currently empty:

```json
{"Tags": {}}
```

## Decision

Do not apply during Phase 14C.

The safest next state is a controlled Lambda-only reconciliation apply before
dashboard hosting live apply. This keeps the target operating model intact:
Terraform remains the ownership boundary, ingestion drift is reconciled before
public delivery changes, and dashboard hosting is not mixed with an unrelated
Lambda mutation.

## Rejected Options

- Broad `ignore_changes` for Lambda code or environment values: rejected because
  it would hide future ingestion drift.
- Preserve the deployed ZIP as the Terraform-owned package: rejected for now
  because extracted source matches the repo source and Terraform already points
  at `lambda/ingest_elexon.py`.
- Targeted dashboard apply while Lambda drift remains: rejected except as an
  explicit break-glass path.

## Next State Boundary

Phase 14D should:

1. Start from clean `main`.
2. Capture the current deployed Lambda ZIP into a local ignored rollback file
   without printing the pre-signed `Code.Location` URL.
3. Save a normal root Terraform plan with CloudFront disabled.
4. Apply only if the saved plan still shows exactly
   `Plan: 0 to add, 1 to change, 0 to destroy`.
5. Verify Lambda hash, tags, sanitized configuration keys, and ingestion smoke
   evidence.
6. Re-run the dashboard hosting plan only after Lambda drift is resolved.

## Rollback Path

If the Lambda reconciliation causes an ingestion regression:

1. Restore the locally saved rollback ZIP with `aws lambda update-function-code`.
2. Re-run the sanitized Lambda configuration check.
3. Re-run the ingestion smoke check.
4. Keep dashboard hosting apply blocked until the Lambda state is stable again.
