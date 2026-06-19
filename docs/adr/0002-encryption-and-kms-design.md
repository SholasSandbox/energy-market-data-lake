# ADR 0002: Encryption Strategy And KMS Design

- Status: Accepted
- Date: 2026-06-14
- Decision owners: Energy Data Lakehouse repository owner
- Related tracker milestone: June-July lakehouse readiness closure

## Context

The Energy Data Lakehouse currently processes public energy-market and news
data in one AWS workload account. The live data bucket uses SSE-S3 (`AES256`),
the Athena workgroup uses `SSE_S3`, the dashboard bucket uses SSE-S3, and
CloudWatch Logs encrypts log data at rest using its default service-managed
encryption.

The platform is a cost-controlled portfolio and lab environment. Its current
AWS Budget is intentionally small, while an AWS KMS customer managed key has a
recurring key charge and can generate request charges. Introducing a key
without a security, compliance, ownership, or cross-account requirement would
add cost and an availability dependency without materially improving the
confidentiality of the current public datasets.

SSE-S3 and SSE-KMS both encrypt S3 data at rest. SSE-KMS becomes preferable
when the architecture needs customer-controlled key policy, independent key
lifecycle, detailed KMS usage auditing, cross-account encrypted-data access,
or a compliance control that specifically requires a customer managed key.

## Decision

Retain the current encryption methods for the active portfolio/lab platform:

- S3 `raw/`: SSE-S3 for public source data with low operational overhead.
- S3 `curated/`: SSE-S3 for derived public market data with no separate
  compliance boundary.
- S3 `audit/` and `failed/`: SSE-S3 for private operational records that do not
  contain regulated data.
- S3 `scripts/`: SSE-S3, with access protected through IAM and integrity
  controls.
- Athena query results: SSE-S3 enforced by the workgroup and consistent with
  the current single-account access model.
- Dashboard bucket: SSE-S3 for approved public-safe CloudFront artifacts.
- CloudWatch Logs: default CloudWatch encryption because no customer managed
  key requirement exists.

Do not create a customer managed KMS key or change live encryption settings as
part of this decision. Implementation requires both a promotion trigger and
explicit approval for AWS changes.

## Alternatives Considered

| Option | Decision | Why |
|---|---|---|
| Retain SSE-S3 for lakehouse data, Athena results, dashboard artifacts, and default CloudWatch Logs encryption | Accepted | Fits the current public-data classification, single-account lab scope, and small budget while preserving encrypted-at-rest coverage. |
| Promote all S3 and Athena storage to one customer managed SSE-KMS key now | Rejected for now | Adds key-policy, availability, cost, and lockout risk without a current sensitive-data, compliance, cross-account, or independent key-administration requirement. |
| Use separate customer managed keys for raw, curated, Athena results, logs, and dashboard data now | Rejected | Provides finer separation but creates disproportionate operational overhead and cost for the current one-owner public-data platform. |
| Use a future dedicated log key for centralized logging | Deferred | Correct target when central logging exists, but premature until the security/log archive boundary is designed. |
| Use multi-Region KMS keys, CloudHSM custom key stores, imported key material, or external key stores | Rejected | These options solve specialized resilience, sovereignty, or external-control requirements that the current workload does not have. |
| Use SSE-KMS for the public dashboard bucket | Rejected for now | Adds CloudFront/key-policy complexity to artifacts intended to be public-safe; restricted data should trigger a dashboard-boundary review first. |

## SSE-KMS Promotion Triggers

Promote the private lakehouse boundary to SSE-KMS when at least one of these
requirements becomes real:

- sensitive, personal, commercially restricted, or regulated data is ingested;
- encrypted objects must be shared across AWS accounts;
- a compliance requirement mandates customer-controlled keys or separation of
  key administrators from data users;
- security operations require KMS-level audit evidence for decrypt and data-key
  operations;
- a security or logging account becomes responsible for key administration;
- contractual controls require independent disablement, rotation, or deletion
  authority; or
- threat modelling concludes that key-policy enforcement materially reduces a
  documented risk.

Do not promote solely because SSE-KMS sounds more production-like. The design
must satisfy a named requirement and include the additional IAM, monitoring,
cost, recovery, and service-availability consequences.

## Target KMS Design

### Lakehouse Data Key

If promoted, create one single-Region symmetric customer managed encryption key
in `eu-west-2` for the private lakehouse data boundary.

```text
alias/energy-market-lakehouse-dev
```

Initial scope:

- `raw/`
- `curated/`
- `audit/`
- `failed/`
- `scripts/`
- `athena-results/`

Use the full key ARN in service configuration. Enable automatic annual key
rotation and S3 Bucket Keys. The S3 Bucket Key reduces KMS request traffic and
cost, but it changes the S3 encryption context from the object ARN to the
bucket ARN. Prefix isolation must therefore remain enforced through S3 bucket
and IAM policies rather than relying on the KMS encryption context.

Do not use a multi-Region key, imported key material, CloudHSM custom key store,
or external key store for the current workload. Those options add cost and
operational responsibility without a requirement.

### Log Key

Do not share the lakehouse data key with a future centralized logging boundary.
If CloudWatch Logs or a central log archive later requires a customer managed
key, create a separate key owned by the security or logging account:

```text
alias/energy-market-logs-<environment>
```

Restrict CloudWatch Logs use with `kms:ViaService` and the
`kms:EncryptionContext:aws:logs:arn` condition for the intended account and log
groups. Existing log groups stay on default encryption until that governance
requirement exists.

### Dashboard Boundary

Keep the public-safe dashboard bucket on SSE-S3. SSE-KMS would add key-policy
and CloudFront integration complexity to content that is intentionally
publishable. Revisit only if the dashboard starts containing restricted data,
which should first trigger a review of whether that data belongs on the public
delivery path at all.

## Ownership And Separation Of Duties

For the current single-account target:

- key owner: workload account;
- key administrators: a named platform/security administration role;
- key users: only workload roles that must encrypt or decrypt lakehouse data;
- data users do not receive key administration permissions; and
- application roles do not receive `kms:DisableKey`, `kms:ScheduleKeyDeletion`,
  `kms:PutKeyPolicy`, `kms:CreateGrant`, or alias-management permissions.

In a multi-account target, key administration moves to the security or data
platform ownership boundary. Cross-account use requires both the key policy in
the owning account and IAM permission in the consuming account.

## Key Policy Design

Every key must have a key policy. The design uses these policy layers:

1. Allow the workload account principal to delegate approved permissions
   through IAM policies, preventing the key from becoming unmanageable.
2. Grant key administration only to the named administration role.
3. Grant cryptographic use only to named workload roles and only through the
   required AWS service and Region.
4. Keep grants disabled for application roles unless a later AWS integration
   explicitly requires them; then require `kms:GrantIsForAWSResource`.

Cryptographic permissions are limited to the selected key ARN:

- `kms:Encrypt`
- `kms:Decrypt`
- `kms:ReEncrypt*`
- `kms:GenerateDataKey*`
- `kms:DescribeKey`

For S3 use, constrain permissions with:

```text
kms:ViaService = s3.eu-west-2.amazonaws.com
kms:EncryptionContext:aws:s3:arn =
  arn:aws:s3:::energy-market-lake-464975959576-20260405
```

The bucket ARN is the expected encryption context when S3 Bucket Keys are
enabled. S3 permissions remain separately scoped by prefix.

The non-deploying policy template is stored at
`docs/policies/kms-lakehouse-key-policy.example.json`. The named KMS
administrator must exist before the template could be used. The Athena query
role is defined, deployed, and live verified under ADR 0004. Any future SSE-KMS
implementation must also attach least-privilege IAM policies to the listed
roles; the key policy alone does not grant S3 data access.

## Workload Permission Matrix

- Ingestion Lambda role: write required `raw/` paths; generate data keys and
  encrypt through S3.
- Glue role: read required `raw/` and `scripts/` paths, write required
  `curated/` paths, decrypt source objects, and encrypt outputs.
- AI orchestration Lambda role: read and write named workflow prefixes; encrypt,
  decrypt, and generate data keys through S3.
- Athena analyst/query role: read curated paths, write bounded results, decrypt
  curated objects, and encrypt or decrypt query results.
- Step Functions and EventBridge roles: no direct S3 or KMS permissions unless
  a later implementation stores KMS-protected state directly.
- CloudFront: dashboard bucket access only and no lakehouse data-key permission.

The KMS design does not fix existing broad S3 permissions. Glue, ingestion,
orchestration, and analyst S3 policies must be prefix-scoped before or alongside
any SSE-KMS implementation.

## Rotation, Disablement, And Deletion

- Enable automatic annual rotation for a customer managed key.
- Rotation changes key material without changing the key ARN; prior material
  remains available to decrypt existing data.
- Alert on `DisableKey`, `ScheduleKeyDeletion`, `CancelKeyDeletion`,
  `PutKeyPolicy`, and unexpected grant changes through CloudTrail/EventBridge.
- Disable a key only through an approved incident or retirement runbook.
- Before deletion, inventory S3 objects, Athena results, logs, replicas,
  backups, and cross-account consumers that depend on the key.
- Use a 30-day pending deletion window and preserve an approved recovery path.
- Never delete a key to reduce ordinary monthly cost while dependent ciphertext
  still exists.

## Migration And Rollback

Changing bucket default encryption affects new writes; existing SSE-S3 objects
do not automatically become SSE-KMS objects. A future migration must therefore:

1. create and validate the key policy and workload IAM permissions;
2. enable S3 Bucket Keys and SSE-KMS for new objects;
3. update and test the Athena workgroup encryption configuration;
4. run representative Lambda, Glue, Athena, and orchestration tests;
5. inventory existing objects and decide whether re-encryption is justified;
6. re-encrypt existing objects only through an approved copy or Batch
   Operations plan with cost, versioning, and rollback controls; and
7. retain the key until every dependent object and result has expired or been
   migrated.

Rollback means restoring SSE-S3 for new writes while keeping the KMS key
enabled for reads of existing SSE-KMS objects. Disabling or deleting the key is
not a valid rollback.

## Cost And Availability

Customer managed keys have a recurring charge and KMS requests can incur usage
charges. S3 Bucket Keys can reduce S3-related KMS request traffic substantially,
but they do not remove the fixed key charge or the operational dependency on
the key policy and KMS availability.

The current platform budget and public-data classification do not justify that
cost and dependency. Cost must be estimated against current AWS pricing before
any implementation approval.

## SAP-C02 Relevance

This decision demonstrates:

- selecting an encryption method from data classification and governance
  requirements rather than using the most complex option by default;
- separating key administrators, key users, data access, and service roles;
- understanding cross-account KMS requirements and dual authorization;
- applying encryption context, `kms:ViaService`, rotation, audit, and deletion
  safeguards; and
- balancing security, availability, cost, and operational complexity.

It primarily supports Domain 1 organizational/security design, Domain 2 new
solution design, and Domain 3 improvement of an existing solution.

## Consequences

### Positive

- Maintains encrypted-at-rest coverage without increasing current AWS cost.
- Keeps the active lab simple and avoids accidental KMS lockout.
- Establishes explicit triggers and a deployable future KMS control model.
- Prevents a customer managed key from being shared indiscriminately across
  data and logging trust boundaries.

### Trade-Offs

- Current encryption does not provide customer-controlled disablement or
  KMS-level cryptographic audit events.
- Cross-account sharing of encrypted data will require a future customer
  managed key and policy changes.
- The design remains unimplemented until a trigger and explicit approval exist.

## Follow-Up Actions

- Versioning, lifecycle protection, and bucket tags were implemented on
  2026-06-15 under ADR 0003.
- Glue S3 access and dedicated Athena query access are now prefix-scoped in
  Terraform and live verified under
  `docs/adr/0004-glue-athena-access-boundaries.md`.
- If an SSE-KMS trigger occurs, add Terraform variables and resources, policy
  tests, a cost estimate, `terraform plan` evidence, and an apply/rollback
  runbook before requesting deployment approval.

## Revisit Conditions

Revisit this ADR when data classification changes, cross-account access becomes
real, a security/log archive account takes key ownership, compliance requires a
customer managed key, KMS audit evidence becomes necessary, or the cost model
changes enough that the operational benefit outweighs the added dependency.

## Design Artifacts

- `docs/policies/kms-lakehouse-key-policy.example.json`: customer managed key
  policy template for the current account, Region, bucket, and workload roles.
- `docs/evidence/s3-data-bucket-posture-20260614.md`: verified current SSE-S3
  posture and associated remediation findings.

## References

- [Amazon S3 SSE-KMS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html)
- [Amazon S3 Bucket Keys](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucket-key.html)
- [Athena query-result encryption](https://docs.aws.amazon.com/athena/latest/ug/encrypting-query-results-stored-in-s3.html)
- [CloudWatch Logs KMS encryption](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/encrypt-log-data-kms.html)
- [AWS KMS key policies](https://docs.aws.amazon.com/kms/latest/developerguide/key-policies.html)
- [AWS KMS key rotation](https://docs.aws.amazon.com/kms/latest/developerguide/rotate-keys.html)
- [AWS KMS pricing](https://aws.amazon.com/kms/pricing/)
