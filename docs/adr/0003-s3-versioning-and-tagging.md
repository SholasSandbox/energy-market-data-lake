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
| `Owner` | `[redacted-owner]` |
| `ManagedBy` | `manual` |
| `DataClassification` | `public` |
| `CostCenter` | `sap-c02-lab` |

`ManagedBy=manual` is accurate while Terraform references but does not manage
the existing bucket. Change it to `terraform` only after an intentional import
or ownership transfer.

## Alternatives Considered

| Option | Decision | Why |
|---|---|---|
| Enable versioning with lifecycle protection and approved governance tags | Accepted | Provides overwrite/delete recovery, cost controls for noncurrent versions, and ownership/cost attribution evidence. |
| Leave versioning disabled | Rejected | Keeps storage cost lower, but leaves the raw/curated evidence path exposed to accidental overwrite or deletion. |
| Enable versioning without noncurrent-version cleanup | Rejected | Improves recovery but creates uncontrolled storage growth, which conflicts with the lab cost-control posture. |
| Use Object Lock or immutable retention | Rejected for now | Stronger protection, but unnecessary for public lab data and materially more complex to govern and reverse. |
| Apply tags without enabling Billing Cost Allocation Tags | Rejected as incomplete | Useful for inventory, but insufficient for cost reporting until selected tag keys are activated in AWS Billing. |
| Import the bucket and manage all controls in Terraform before any live change | Deferred | Desirable for ownership clarity, but would broaden the closure task into import/reconciliation work and delay the approved recovery/tagging remediation. |
| Use different tag keys such as personal `Owner` or high-cardinality generated tags for Billing | Rejected for Billing activation | Noisier and less useful for cost allocation than stable low-cardinality governance keys. |

## Rationale

Versioning provides recovery from accidental overwrite and deletion, which is
more valuable than leaving the current data and evidence path unprotected.
The 30-day noncurrent-version expiry limits the storage growth introduced by
versioning while leaving a practical recovery window.

Tags establish ownership, environment, workload, data classification, and cost
attribution. The selected low-cardinality user-defined cost-allocation tags were
activated in AWS Billing from the Organizations management account on
2026-06-17.

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
- Cost Allocation Tag activation was initially blocked from the workload
  account on 2026-06-16, then completed from the Organizations management
  account on 2026-06-17. See
  `docs/evidence/cost-allocation-tag-activation-preflight-20260616.md` and
  `docs/evidence/cost-allocation-tag-activation-20260617.md`.
- Decide whether to import the existing bucket and child controls into the
  lakehouse Terraform root.
- Monitor storage growth before extending the 30-day noncurrent-version window.

## Revisit Conditions

Revisit this ADR if storage growth from noncurrent versions becomes material,
recovery requirements exceed 30 days, the bucket is imported into Terraform,
data classification changes, Object Lock becomes a real audit requirement, or
the account-level tagging standard changes during the governance phase.

## References

- `docs/evidence/s3-data-bucket-posture-20260614.md`
- `docs/adr/0001-shared-s3-data-bucket.md`
- `infra/terraform/lakehouse/s3.tf`
