# S3 Versioning And Tagging Apply Evidence - 2026-06-15

## Scope

Controlled update of the live Energy Data Lakehouse data bucket following the
accepted ADR 0003 decision and explicit user approval.

- Bucket: `energy-market-lake-464975959576-20260405`
- Region: `eu-west-2`
- Applied and verified at: `2026-06-15T12:58:10Z`
- AWS account: `464975959576`
- AWS principal: `IAMUser1`

## Preflight

Immediately before the change:

- bucket versioning was not enabled;
- the existing `raw-lifecycle` rule was enabled;
- the bucket had no tag set;
- Terraform configuration validation passed; and
- AWS CLI lifecycle and tagging payload validation passed.

## Applied Sequence

The lifecycle safeguards were applied and read back before versioning was
enabled.

1. Preserve the existing `raw-lifecycle` rule.
2. Add 30-day noncurrent-version expiration across the bucket.
3. Add expired delete-marker cleanup.
4. Add 7-day incomplete multipart-upload cleanup.
5. Enable bucket versioning.
6. Add the approved governance and cost-attribution tags.

No object copy, deletion, encryption change, public-access change, or Terraform
import was performed.

## Verified Live State

### Versioning

```json
{
  "Status": "Enabled"
}
```

### Lifecycle Rules

- `raw-lifecycle`: move `raw/` objects to Standard-IA at 30 days and Glacier at
  90 days, then expire them at 180 days.
- `noncurrent-version-retention`: expire noncurrent versions after 30 days.
- `expired-delete-marker-cleanup`: remove expired object delete markers.
- `incomplete-multipart-upload-cleanup`: abort incomplete multipart uploads
  after 7 days.

The S3 transition minimum remains `all_storage_classes_128K`.

### Bucket Tags

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

`ManagedBy=manual` reflects that Terraform still references but does not manage
the existing data bucket.

### Unchanged Controls

- Default encryption remains SSE-S3 (`AES256`).
- SSE-C remains blocked by the bucket encryption configuration.
- All four S3 Block Public Access controls remain enabled.
- The bucket remains in `eu-west-2`.

## Validation

- `terraform -chdir=infra/terraform/lakehouse fmt -check`
- `terraform -chdir=infra/terraform/lakehouse validate`
- `aws s3api get-bucket-versioning`
- `aws s3api get-bucket-lifecycle-configuration`
- `aws s3api get-bucket-tagging`
- `aws s3api get-bucket-encryption`
- `aws s3api get-public-access-block`
- `aws s3api head-bucket`

## Remaining Governance Actions

- Activate selected user-defined cost-allocation tags in AWS Billing through a
  separately approved account-governance action.
- Decide whether to import the existing bucket and child controls into the
  lakehouse Terraform root.
- Monitor noncurrent-version storage growth and revisit the 30-day retention
  period if recovery needs or cost evidence justify a change.
