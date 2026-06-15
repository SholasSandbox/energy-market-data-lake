# S3 Data Bucket Posture Verification - 2026-06-14

## Scope

Read-only verification of the live Energy Data Lakehouse data bucket configured
by `infra/terraform/lakehouse/terraform.tfvars`.

- Bucket: `energy-market-lake-464975959576-20260405`
- Region: `eu-west-2`
- Observed at: `2026-06-14T22:53:50Z`
- AWS identity: authenticated IAM user in account `464975959576`
- Change boundary: no create, update, delete, import, plan, or apply action

`head-bucket` and `get-bucket-location` confirmed that the configured bucket was
accessible in `eu-west-2` before the posture calls were made.

## Results

### Versioning

Command:

```bash
aws s3api get-bucket-versioning \
  --bucket energy-market-lake-464975959576-20260405 \
  --region eu-west-2
```

Result: the API returned no `Status` or `MFADelete` fields. Bucket versioning is
not enabled. This differs from the Terraform-created bucket path, which enables
versioning when `create_data_bucket = true`.

### Default Encryption

Command:

```bash
aws s3api get-bucket-encryption \
  --bucket energy-market-lake-464975959576-20260405 \
  --region eu-west-2
```

Result:

- default encryption algorithm: `AES256` (SSE-S3)
- S3 Bucket Key: disabled, which is expected to have no benefit for SSE-S3
- blocked encryption type: SSE-C

The live default is confirmed. ADR 0002 subsequently retained SSE-S3 for the
current platform and defined the conditions for promotion to SSE-KMS.

### Public Access Block

Command:

```bash
aws s3api get-public-access-block \
  --bucket energy-market-lake-464975959576-20260405 \
  --region eu-west-2
```

Result: all four bucket-level controls are enabled:

- `BlockPublicAcls: true`
- `IgnorePublicAcls: true`
- `BlockPublicPolicy: true`
- `RestrictPublicBuckets: true`

### Lifecycle Configuration

Command:

```bash
aws s3api get-bucket-lifecycle-configuration \
  --bucket energy-market-lake-464975959576-20260405 \
  --region eu-west-2
```

Result: the enabled `raw-lifecycle` rule applies to `raw/` objects:

- transition to `STANDARD_IA` after 30 days
- transition to `GLACIER` after 90 days
- expire after 180 days
- default transition minimum: objects of at least 128 KB

No curated-zone lifecycle rule was returned.

### Tags

Command:

```bash
aws s3api get-bucket-tagging \
  --bucket energy-market-lake-464975959576-20260405 \
  --region eu-west-2
```

Result: AWS returned `NoSuchTagSet`. The live data bucket has no bucket tags.

## Assessment

| Control | Observed status | Closure assessment |
| --- | --- | --- |
| Versioning | Not enabled | Remediation decision required |
| Default encryption | SSE-S3 (`AES256`) | Verified; retained by ADR 0002 |
| Public Access Block | All four controls enabled | Verified |
| Raw lifecycle | Enabled: 30/90/180-day transitions and expiry | Verified |
| Bucket tags | No tag set | Remediation required |

This was the pre-change posture. On 2026-06-15, explicit approval was granted
and versioning, lifecycle protection, and bucket tags were applied. See
`docs/evidence/s3-versioning-tagging-apply-20260615.md`.

## SAP-C02 Relevance

- Domain 1: validates access controls, ownership boundaries, encryption, and
  governance evidence.
- Domain 2: demonstrates cost-aware S3 storage-class and lifecycle design.
- Domain 3: compares live posture with the desired infrastructure definition
  and converts drift into explicit remediation decisions.

## Follow-Up

1. Use `docs/adr/0002-encryption-and-kms-design.md` as the encryption decision.
2. Use `docs/adr/0003-s3-versioning-and-tagging.md` as the accepted versioning
   and tagging decision.
3. Decide whether Terraform should import and manage the existing bucket-level
   controls or continue to reference an externally managed bucket.
