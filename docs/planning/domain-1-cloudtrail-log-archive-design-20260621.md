# Domain 1 CloudTrail And Log Archive Design - 2026-06-21

<!-- markdownlint-disable MD013 -->

## Scope

This note records the repo-only design for the organization CloudTrail trail
and central log archive boundary that will support the later Domain 1
governance phase.

It aligns with:

- `docs/planning/sap-c02-readiness-tracker.md`, which keeps governance work in
  documentation/preparation mode until live changes are explicitly approved.
- `docs/planning/domain-1-governance-preflight-20260618.md`, which called for a
  detailed CloudTrail and log archive design before implementation.
- `docs/adr/0005-aws-organizations-governance-design.md`, which set the target
  control-plane and security/log archive direction.
- `docs/runbooks/domain-1-governance-live-readiness-runbook.md`, which
  packages the later per-change evidence, rollback, and validation boundary.

This note is an early design artifact created under explicit user approval
before the scheduled 2026-07-13 governance phase. It does not pull the tracker
phase forward, authorize AWS changes, or approve Terraform work.

## Status Note

The live organization state has since advanced beyond the original 2026-06-21
design baseline:

- `Security OU` now exists live;
- the dedicated `Security Log Archive` account `955659429518` now exists live;
- `account.amazonaws.com` trusted access and alternate contacts are now set for
  that account;
- the new account now sits in `Security OU`;
- the dedicated log-archive bucket and customer-managed KMS key boundary now
  also exist live;
- `cloudtrail.amazonaws.com` trusted access is now enabled and the
  management-account organization trail is now live.

This note still records the accepted design intent. The remaining open logging
work is now later retention refinement plus the downstream AWS Config and
GuardDuty decisions, not account-boundary creation, storage-boundary bring-up,
or organization-trail enablement.

The long-term architecture decision has also been tightened since the original
draft: keep the current `Security Log Archive` account as the storage-only
boundary, and later create a separate `Security Tooling` account for delegated
security-service operations after the current break-glass and root-SCP closure
is complete. OAM/cross-account observability belongs in that later tooling or
central monitoring boundary, not in the write-mostly log archive account.

## Confirmed Alignment

This note supports the tracker because it advances:

1. SAP-C02 Domain 1: organizational complexity, centralized logging, control
   planes, delegated administration, and audit protection.
2. The Energy Data Lakehouse case study as a governed multi-account AWS
   portfolio.
3. Near-term cloud architect positioning through concrete logging and
   evidence-retention decisions.
4. Later AWS Config, GuardDuty, and Security Hub design by clarifying the
   logging and archive boundary first.

## Current Organization Context

| Account | Current role | Design implication |
|---|---|---|
| `management-account-alias` | AWS Organizations management account | Owns organization control-plane decisions and should create the organization trail unless a later delegated-administrator design is explicitly adopted. |
| `lakehouse-workload-account` | Energy Data Lakehouse workload account | Produces workload events that should flow into the centralized organization trail. |
| `containers-lab.com` | Sandbox member account | Remains separate from lakehouse evidence, but its control-plane events should still be covered by the organization trail. |
| `Security Log Archive` | Dedicated member account in `Security OU` | Target owner for the dedicated log archive bucket, KMS key, and the long-term storage-only audit boundary. |

## Design Decisions

### 1. Trail ownership and control plane

Use one organization CloudTrail trail created from the management account as the
default design.

Rationale:

- AWS CloudTrail organization trails can be created by the management account or
  a delegated administrator, but the current repo governance model still treats
  the management account as the control plane.
- This keeps trail ownership aligned with Organizations administration while
  the `Security Log Archive` account remains storage-only and before any later
  CloudTrail delegated-administrator model is intentionally designed.

Revisit if:

- a later `Security Tooling` account is intentionally chosen as the CloudTrail
  delegated administrator account; or
- control-plane duties are intentionally separated further.

### 2. Trail scope

Use one multi-Region organization trail as the baseline design.

Baseline logging choices:

- include management events for all organization accounts;
- log both read and write management events;
- include global service events;
- enable log file integrity validation;
- deliver to a dedicated S3 bucket for CloudTrail logs only.

Do not make these part of the baseline trail yet:

- organization-wide S3 object-level data events;
- broad Lambda data events;
- CloudTrail Insights by default;
- CloudTrail Lake event data stores.

Those can be added later only where the study value, detection value, or audit
need outweighs the added cost and noise.

### 3. Log archive bucket ownership

Target a dedicated S3 bucket in the current `Security Log Archive` account
rather than a shared bucket in the management or workload account.

Design posture:

- one bucket dedicated to CloudTrail log delivery;
- no workload data mixed into that bucket;
- S3 Block Public Access enabled;
- versioning enabled;
- access kept private by default;
- CloudTrail service principal write access constrained with `aws:SourceArn`;
- no Requester Pays configuration.

This keeps audit evidence separate from workload operations and supports later
least-privilege read access through a security audit boundary.

### 4. Encryption posture

Target SSE-KMS with a customer-managed KMS key in the same Region as the log
archive bucket.

Rationale:

- central audit logs justify stricter control over decrypt permissions than the
  current public-data lakehouse baseline;
- KMS gives explicit control over which audit users and roles can decrypt logs;
- the current `Security Log Archive` account is the right ownership boundary
  for that key.

Design rules:

- the log archive KMS key should live in the same Region as the bucket;
- the key policy must allow CloudTrail to encrypt and approved audit principals
  to decrypt;
- read access requires both S3 read permissions and KMS decrypt permissions.

Fallback:

- if a temporary proof is ever needed before a dedicated log archive account
  exists, SSE-S3 is an acceptable interim implementation path, but it is not the
  target design for the centralized audit boundary.

### 5. Retention and delete protection

Use a retention-first design rather than an early-delete design.

Baseline posture:

- keep current-period logs directly accessible in S3;
- use lifecycle transitions for older logs if cost control becomes necessary;
- avoid automatic deletion until the governance phase explicitly approves a
  retention window and rollback posture.

Protected-delete design:

- restrict delete permissions to a very small break-glass boundary;
- use bucket versioning and least-privilege access as the baseline safeguard;
- add an SCP later to deny deleting the central log bucket once the bucket and
  OU targets exist;
- consider S3 Object Lock only if immutable retention is required strongly
  enough to justify the extra operational complexity.

Do not make MFA Delete the primary design choice:

- it is root-user-centric operationally; and
- AWS documents that S3 Lifecycle configuration is not supported on MFA-enabled
  buckets.

### 6. Access model

The default read model should favor separation of duties:

- organization administrators manage the trail configuration from the management
  account;
- the future security/log archive boundary owns log storage, encryption, and
  audit-read access;
- `SecurityAudit` is the preferred human read-only consumer shape;
- workload operators do not need broad write access to the log archive bucket;
- member accounts should not receive default broad access to organization log
  objects.

### 7. Relationship to later AWS Config and security services

This design unlocks later decisions cleanly:

- AWS Config can aggregate configuration and compliance data into a separate
  `Security Tooling` account after recorder scope and cost controls are chosen.
- GuardDuty can use that later `Security Tooling` account as the delegated
  security-operations home.
- Security Hub can be evaluated later as a standards and finding aggregation
  layer once the logging and GuardDuty boundaries are settled, and if adopted
  it should follow the same `Security Tooling` boundary rather than expand the
  log archive account.
- OAM can be evaluated later for cross-account operational visibility, but it
  should remain separate from the CloudTrail log archive because it answers
  live troubleshooting questions rather than audit-retention questions.

The sequence matters:

1. define the CloudTrail/log archive ownership and protection model;
2. define AWS Config recorder scope and aggregator account;
3. after the current break-glass/root-SCP blocker closes, create the separate
   `Security Tooling` account;
4. define AWS Config migration into that account;
5. define GuardDuty delegated-administrator and member coverage;
6. decide whether Security Hub adds enough value before the exam.

## Alternatives Considered

| Option | Decision | Why |
|---|---|---|
| Dedicated log archive bucket in the current `Security Log Archive` account, with later delegated security tooling split into a separate account | Accepted target design | Best matches centralized logging, storage hardening, and cleaner later separation of duties. |
| Keep CloudTrail logs in the management account long term | Rejected as target, acceptable only as temporary fallback | Simpler initially, but weaker segregation of duties and weaker future security-account narrative. |
| Keep archive storage and delegated security tooling permanently combined in one security account | Rejected as the preferred target | Smaller at first, but it mixes write-mostly audit storage with expanding operational permissions for Config, GuardDuty, and possible later Security Hub. |
| Use SSE-S3 as the permanent audit-log encryption model | Rejected as target | Lower operational overhead, but weaker control over decrypt permissions for a centralized audit boundary. |
| Enable organization-wide data events immediately | Rejected for baseline | Higher cost and noise than the current lab step needs; targeted later enablement is more deliberate. |
| Use MFA Delete as the main delete-protection design | Rejected | Root-centric operations and lifecycle incompatibility make it a poor default fit for an intentionally managed archive bucket. |
| Require S3 Object Lock from day one | Rejected for now | Strong immutability option, but more than this phase requires before retention and operational processes are finalized. |

## Practical Design Summary

```text
Management account
  └─ owns one multi-Region organization trail
       └─ writes CloudTrail logs and digest files
            to dedicated S3 bucket in current Log Archive boundary
                 ├─ Block Public Access
                 ├─ versioning
                 ├─ dedicated bucket policy with CloudTrail service principal
                 ├─ aws:SourceArn condition
                 └─ customer-managed KMS key in same Region

Security Log Archive account
  ├─ owns bucket and KMS key
  ├─ stays storage-first and write-mostly
  └─ exposes read-only audit access

Future Security Tooling account
  ├─ becomes the later home for Config aggregation
  ├─ becomes the later home for GuardDuty delegated administration
  ├─ becomes the later home for OAM / cross-account observability if adopted
  └─ hosts Security Hub only if it is later adopted
```

## Open Implementation Work

This note does not complete implementation. The following remain open:

- use `docs/runbooks/domain-1-governance-live-readiness-runbook.md` to package
  the exact prechange evidence, blast radius, rollback, validation, cost, and
  approval boundary before any live execution;
- choose final retention transition schedule;
- decide whether Object Lock is worth the extra operational burden;
- resolve the exact storage-step and organization-trail live approvals against
  the now-recorded bucket policy, KMS policy, and fresh read-only baseline;
- after the current break-glass/root-SCP blocker closes, create the separate
  `Security Tooling` account and keep `Security Log Archive` storage-only;
- define AWS Config recorder scope and migrate aggregation first into that later
  `Security Tooling` account;
- define GuardDuty delegated-administrator and member coverage after Config
  migration is settled;
- evaluate OAM as a separate cross-account observability pattern if the workload
  needs centralized CloudWatch metrics, logs, or traces;
- make a separate Security Hub adopt/defer decision.

## References

- Creating a trail for an organization:
  `https://docs.aws.amazon.com/awscloudtrail/latest/userguide/creating-trail-organization.html`
- Amazon S3 bucket policy for CloudTrail:
  `https://docs.aws.amazon.com/awscloudtrail/latest/userguide/create-s3-bucket-policy-for-cloudtrail.html`
- Security best practices in AWS CloudTrail:
  `https://docs.aws.amazon.com/awscloudtrail/latest/userguide/best-practices-security.html`
- Encrypting CloudTrail log files with AWS KMS keys:
  `https://docs.aws.amazon.com/awscloudtrail/latest/userguide/encrypting-cloudtrail-log-files-with-aws-kms.html`
- Validating CloudTrail log file integrity:
  `https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-log-file-validation-intro.html`
- Multi-account, multi-Region data aggregation for AWS Config:
  `https://docs.aws.amazon.com/config/latest/developerguide/aggregate-data.html`
- S3 Object Lock:
  `https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html`
- S3 MFA Delete:
  `https://docs.aws.amazon.com/AmazonS3/latest/userguide/MultiFactorAuthenticationDelete.html`
