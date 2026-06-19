# ADR 0001: Shared S3 Data Bucket With Raw And Curated Zones

- Status: Accepted
- Date: 2026-06-13
- Decision owners: Energy Data Lakehouse repository owner
- Related tracker milestone: June-July lakehouse readiness closure

## Context

The implemented Energy Data Lakehouse uses one private S3 data bucket with
logical zones separated by prefixes:

```text
s3://<data-bucket>/
  raw/
  curated/
  athena-results/
  scripts/
  audit/
  failed/
  archive/
```

Lambda writes source payloads below `raw/`. Glue crawlers inspect `raw/` and
`curated/`, and the Glue ETL job reads raw objects and writes partitioned
Parquet below `curated/`. Athena query results and the Glue script also use
dedicated prefixes in the same data bucket. Public dashboard assets are kept
outside this boundary in a separate private dashboard bucket delivered through
CloudFront.

The active bucket name is supplied through `data_bucket_name`. The current
Terraform configuration sets `create_data_bucket = false`, so the lakehouse
root references the existing bucket but does not manage the bucket resource,
encryption configuration, versioning, public-access block, or lifecycle rules.

## Decision

Retain one shared private S3 data bucket for the current portfolio and lab
platform. Continue to separate raw and curated data through the top-level
`raw/` and `curated/` prefixes and use prefix-scoped IAM permissions, lifecycle
rules, Glue targets, and operational evidence.

Use this naming pattern for lakehouse data buckets:

```text
energy-market-<purpose>-<account-id>-<unique-suffix>
```

The existing `energy-market-lake-<account-id>-<date-suffix>` name conforms to
that pattern and is retained. Avoid names that imply separate raw or curated
buckets while this decision remains active.

The workload account owns the data bucket. Infrastructure ownership must also
be explicit:

- a Terraform-created or imported bucket is managed by the lakehouse Terraform
  root; or
- an existing bucket remains externally managed and Terraform only references
  it.

Do not claim Terraform ownership of bucket-level controls while
`create_data_bucket = false` and the bucket has not been imported. The current
referenced-bucket arrangement is accepted temporarily, subject to the posture
verification and ownership follow-up below.

## Alternatives Considered

| Option | Decision | Why |
|---|---|---|
| One shared private data bucket with `raw/` and `curated/` prefixes | Accepted | Matches the implemented platform, keeps cost and policy surface small, and is sufficient for one owner, one workload account, public data, and prefix-scoped IAM. |
| Separate raw and curated buckets in the same account | Rejected for now | Adds bucket policies, lifecycle rules, logging decisions, Terraform/import work, and migration effort without a current ownership, compliance, recovery, or security requirement that prefixes cannot express. |
| Separate ingestion and analytics accounts with separate buckets | Rejected for now | Stronger isolation, but materially larger than the current portfolio/lab scope and better reserved for a real multi-team or regulated boundary. |
| Rebuild or rename the existing bucket to match a new naming ideal | Rejected | Would create unnecessary migration and evidence churn while the existing bucket name already fits the accepted naming pattern. |
| Import the existing bucket into Terraform immediately | Deferred | Desirable for long-term ownership clarity, but bucket import and child-control reconciliation are separate operational changes and were not required to close the current architecture decision. |

## Why This Is Acceptable Now

- The platform has one owner, one workload account, and a small portfolio/lab
  operating scope.
- Prefixes already provide clear data lifecycle and processing boundaries.
- Lambda, Glue, Athena, and Step Functions can use one configured bucket name,
  keeping the current platform understandable and inexpensive.
- Prefix-scoped IAM and `s3:prefix` conditions can enforce least privilege
  without adding buckets solely for organization.
- Lifecycle rules can target `raw/` independently while frequently queried
  curated Parquet remains in an appropriate storage class.
- A single bucket reduces configuration, policy, logging, replication, and
  operational overhead during the SAP-C02 readiness programme.

This is a proportionate decision for the current platform, not a general rule
that one bucket is always preferable.

## When Separate Buckets Are Preferable

Use separate raw and curated buckets when a prefix boundary cannot adequately
express an ownership, security, compliance, resilience, or operational
requirement. Examples include:

- different AWS accounts own ingestion and analytics;
- raw data requires immutability, Object Lock, distinct retention, or a more
  restrictive deletion policy;
- zones require different KMS keys or independently administered key policies;
- regulatory, residency, classification, or contractual controls differ;
- independent teams need bucket-level administration without access to the
  other zone;
- replication, backup, event notification, access logging, or recovery
  requirements differ materially;
- reducing the blast radius of an incorrect bucket policy is more important
  than minimizing operational overhead; or
- data sharing and cross-account consumption require a distinct curated
  publication boundary.

Separate accounts plus separate buckets are generally stronger than adding
two buckets to the same account when the real requirement is organizational
isolation.

## Security And Governance Implications

The shared bucket must remain private. S3 Block Public Access, encryption at
rest, versioning, lifecycle controls, access logging or CloudTrail data-event
decisions, and cost tags apply at the bucket or account boundary and must be
verified explicitly.

IAM policies must distinguish zone access even though the bucket is shared:

- ingestion roles write only required `raw/` paths;
- Glue reads required raw paths and writes only required curated paths;
- analyst roles read curated data and use a bounded Athena results location;
- destructive actions are restricted to the smallest required prefixes; and
- `s3:ListBucket` permissions use prefix conditions where practical.

A shared bucket increases the effect of an overly broad bucket policy, IAM
policy, lifecycle rule, or deletion permission. Prefix separation therefore
does not replace least-privilege IAM, explicit ownership, encryption design,
audit controls, backup decisions, or evidence of the live bucket posture.

## SAP-C02 Relevance

This decision demonstrates the ability to choose a proportionate S3 design
rather than adding resources without a requirement. It supports:

- Domain 1 through ownership, least privilege, encryption, auditability, and
  account-boundary reasoning;
- Domain 2 through storage, data-lake, service-integration, and cost-aware
  architecture decisions; and
- Domain 3 through evaluation of an implemented design, blast radius,
  operational overhead, and conditions that justify modernization.

The exam-relevant principle is to select bucket and account boundaries from
security, ownership, compliance, resilience, and operating requirements, not
from raw/curated terminology alone.

## Consequences

### Positive

- Preserves the implemented and evidenced data flow.
- Keeps the current lab inexpensive and understandable.
- Avoids an unnecessary data migration or AWS change during readiness closure.
- Establishes a clear threshold for introducing separate buckets later.

### Trade-offs

- Bucket-level controls are shared across zones.
- Policy mistakes can affect both raw and curated data.
- Stronger organizational isolation will require separate buckets and likely
  separate accounts.
- The current externally managed bucket needs an explicit infrastructure
  ownership decision.

## Follow-Up Actions

- Live posture was verified on 2026-06-14 using read-only commands; see
  `docs/evidence/s3-data-bucket-posture-20260614.md`. Versioning, lifecycle
  protection, and bucket tags were then implemented on 2026-06-15 under ADR
  0003.
- Decide whether to import the existing bucket and its controls into the
  lakehouse Terraform root or document the external management owner.
- The SSE-S3 versus SSE-KMS decision and KMS target design were accepted on
  2026-06-14; see `docs/adr/0002-encryption-and-kms-design.md`.
- Restrict the Glue role to required prefixes and actions.
- Add a dedicated Athena analyst/query policy with bounded results and catalog
  permissions.
- Capture a current raw-to-curated-to-Athena evidence chain.

## Revisit Conditions

Revisit this ADR if account separation, regulation, team ownership, recovery,
data-sharing, Object Lock, cross-account sharing, KMS ownership, or replication
requirements change. Also revisit it before claiming a production landing-zone
posture, because the current single-bucket choice is intentionally scoped to
the portfolio/lab platform.

## Evidence

- `README.md` documents the shared `raw/` and `curated/` layout.
- `infra/terraform/lakehouse/locals.tf` derives both paths from one data bucket.
- `infra/terraform/lakehouse/glue.tf` targets both prefixes in that bucket.
- `infra/terraform/lakehouse/s3.tf` defines prefix-specific raw lifecycle
  behavior when Terraform creates the bucket.
- `infra/terraform/lakehouse/terraform.tfvars.example` documents the existing
  referenced-bucket mode.
- `docs/evidence/phase14d-lambda-reconciliation-apply-summary-20260521.md` and
  `docs/evidence/athena-gas-query-summary-20260506.md` provide implementation
  evidence for the raw and curated paths.
