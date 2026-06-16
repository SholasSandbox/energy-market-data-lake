# ADR 0003: S3 Versioning And Bucket Tagging

- Status: Accepted and implemented
- Date: 2026-06-15
- Decision owners: Energy Data Lakehouse repository owner
- Related tracker milestone: June-July lakehouse readiness closure

## Context

Read-only verification on 2026-06-14 found that the live data bucket had
SSE-S3, full S3 Block Public Access, and an enabled raw-data lifecycle rule. It
did not have versioning or bucket tags.

The bucket is referenced by the lakehouse Terraform root with
`create_data_bucket = false`, so the current live bucket controls are not
managed by Terraform. Any direct AWS change must therefore be documented and
kept aligned with the Terraform-created target.

## Decision

Enable S3 Versioning on the live lakehouse data bucket as a controlled change.
Apply lifecycle protection before enabling versioning:

- preserve the existing `raw/` transitions to Standard-IA after 30 days and
  Glacier after 90 days;
- preserve expiration of current `raw/` objects after 180 days;
- expire noncurrent versions across the bucket after 30 days;
- remove expired object delete markers; and
- abort incomplete multipart uploads after 7 days.

Add these bucket tags:

| Tag | Value |
| --- | --- |
| `Project` | `energy-market` |
| `Workload` | `energy-market-data-lake` |
| `Environment` | `dev` |
| `Purpose` | `lakehouse-data` |
| `Owner` | `Shola` |
| `ManagedBy` | `manual` |
| `DataClassification` | `public` |
| `CostCenter` | `sap-c02-lab` |

`ManagedBy=manual` is accurate while Terraform references but does not manage
the existing bucket. Change it to `terraform` only after an intentional import
or ownership transfer.

## Rationale

Versioning provides recovery from accidental overwrite and deletion, which is
more valuable than leaving the current data and evidence path unprotected.
The 30-day noncurrent-version expiry limits the storage growth introduced by
versioning while leaving a practical recovery window.

Tags establish ownership, environment, workload, data classification, and cost
attribution. User-defined cost-allocation tags still require activation in AWS
Billing before they appear in cost reporting.

## Consequences

- New object writes receive version IDs.
- Overwrites and deletes retain recoverable noncurrent versions for 30 days.
- Current raw objects continue to follow the existing 30/90/180-day lifecycle.
- Storage cost can increase during the recovery window.
- Tags improve inventory and cost-governance evidence but do not themselves
  enforce access control.
- The direct live configuration remains externally managed until the bucket is
  imported into Terraform or another owner is documented.

## Rollback

Suspending versioning affects future writes but does not remove existing
versions. A rollback must retain the lifecycle cleanup rules and must not bulk
delete versions without a separate reviewed plan.

Bucket tags can be replaced or removed, but removal would reduce governance
evidence and requires explicit approval.

## Follow-Up

- Read-back evidence was captured in
  `docs/evidence/s3-versioning-tagging-apply-20260615.md`.
- Cost Allocation Tag activation was approved and attempted on 2026-06-16, but
  AWS blocked the action because the workload account is a linked account
  without Billing tag-administration access. See
  `docs/evidence/cost-allocation-tag-activation-preflight-20260616.md`.
- Decide whether to import the existing bucket and child controls into the
  lakehouse Terraform root.
- Monitor storage growth before extending the 30-day noncurrent-version window.

## References

- `docs/evidence/s3-data-bucket-posture-20260614.md`
- `docs/adr/0001-shared-s3-data-bucket.md`
- `infra/terraform/lakehouse/s3.tf`
