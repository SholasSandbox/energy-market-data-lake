# Domain 2 Lakehouse Consolidation - 2026-06-17

## Scope

This note consolidates the June-July Energy Data Lakehouse evidence for
SAP-C02 Domain 2, Design for New Solutions. It closes the repository-side
lakehouse readiness pass before the larger IAM, Organizations, SCP, and central
governance phase begins.

This is a documentation and evidence consolidation only. It does not authorize
new AWS changes, dashboard expansion, AI orchestration work, or container work.

## Domain 2 Coverage Summary

| Area | Current repository posture | Evidence |
|---|---|---|
| Storage | One private S3 data bucket with `raw/`, `curated/`, `scripts/`, and `athena-results/` prefixes. Raw and curated zones are separated by prefix, IAM, Glue targets, and lifecycle rules rather than separate buckets. | `docs/adr/0001-shared-s3-data-bucket.md`; `docs/evidence/s3-data-bucket-posture-20260614.md` |
| Transformation | Glue crawlers and the Glue ETL job can read raw/source paths and write curated Parquet output under the restricted Glue role. | `docs/evidence/glue-athena-iam-live-verification-20260615.md`; `glue/etl_raw_to_parquet.py` |
| Query | Athena can query the curated dataset through the dedicated workgroup and query role. The verified query returned `row_count = 56`. | `docs/evidence/glue-athena-iam-live-verification-20260615.md`; `docs/evidence/athena-query-results-20260615.json` |
| Security | S3 Block Public Access is enabled, SSE-S3 is retained by decision, Glue access is prefix-scoped, and Athena query access is limited to curated data plus bounded result writes. | `docs/adr/0002-encryption-and-kms-design.md`; `docs/adr/0004-glue-athena-access-boundaries.md`; `docs/evidence/glue-athena-iam-live-verification-20260615.md` |
| Observability | Lambda, Glue, and Athena logging/metrics are configured or evidenced. The Glue/Athena verification records successful crawler, ETL, and query execution states. Managed workflow observation evidence remains a maintained baseline rather than a new feature path. | `docs/evidence/glue-athena-iam-live-verification-20260615.md`; `docs/evidence/phase17au-managed-workflow-scheduled-observation-20260612.md` |
| Cost | Raw lifecycle rules, noncurrent-version cleanup, incomplete multipart cleanup, bucket tags, selected AWS Billing Cost Allocation Tags, and the managed-workflow budget baseline are documented. | `docs/evidence/s3-versioning-tagging-apply-20260615.md`; `docs/evidence/cost-allocation-tag-activation-20260617.md`; `docs/evidence/phase17at-budget-guardrail-apply-summary-20260610.md` |
| Resilience decisions | S3 versioning and lifecycle cleanup are enabled for recovery and cost control. SSE-KMS promotion, key rotation, disablement, deletion, migration, and rollback are designed but not implemented because no current promotion trigger exists. Multi-Region DR, RTO/RPO, and networking resilience remain later SAP-C02 milestones. | `docs/adr/0002-encryption-and-kms-design.md`; `docs/adr/0003-s3-versioning-and-tagging.md`; `docs/evidence/s3-versioning-tagging-apply-20260615.md` |

## What Is Proven

- The current raw to Glue to curated Parquet to Athena path is live verified.
- The S3 data bucket posture has been read back and then remediated for
  versioning, lifecycle protection, and governance tags.
- The single-bucket raw/curated design is intentional and bounded by ADR 0001.
- SSE-S3 is the accepted current encryption model, with a documented SSE-KMS
  promotion threshold and target key policy.
- Glue no longer needs whole-bucket data access for the lakehouse flow.
- Athena has a dedicated query role that can read curated data and cannot list
  raw data.
- Selected Cost Allocation Tags are active from the AWS Organizations
  management account.

## What Is Not Claimed

- This pass does not claim a full enterprise governance design. OU design,
  SCPs, Identity Center permission sets, central logging, CloudTrail
  organization trail design, AWS Config aggregation, and break-glass access
  remain open for the scheduled governance phase.
- This pass does not claim a full disaster recovery design. RTO/RPO, backup
  strategy, multi-Region patterns, VPC endpoints, PrivateLink, Transit Gateway,
  Direct Connect, and Route 53 Resolver decisions remain later SAP-C02 work.
- This pass does not make the external S3 data bucket Terraform-managed. The
  bucket remains referenced by Terraform unless a later import/ownership
  decision is approved.
- This pass does not promote to SSE-KMS. SSE-KMS remains conditional on a
  documented trigger and explicit approval.
- This pass does not expand the dashboard, AI orchestration, or container
  tracks.

## Domain 2 Weak Areas To Carry Forward

| Weak area | Why it matters for SAP-C02 | Next artifact |
|---|---|---|
| Resilience and DR selection | Domain 2 questions often require choosing the simplest reliable design under RTO, RPO, cost, and operational constraints. | RTO/RPO table and DR pattern matrix in the September resilience milestone |
| Network access patterns | Data architecture questions often depend on choosing VPC endpoints, PrivateLink, peering, Transit Gateway, VPN, or Direct Connect correctly. | Compact carry-forward note in `docs/planning/domain-2-network-access-patterns-20260621.md`; fuller networking comparison matrix before the September milestone |
| Terraform ownership of existing bucket controls | The current live bucket controls are evidenced, but ownership is split between manual controls and referenced Terraform inputs. | Bucket import or external-owner decision before production claims |
| Practice-question calibration | The first two 20-question practice blocks and wrong-answer log are complete, but exam readiness still needs weak-area review and later timed scenario practice. | Carry-forward tracker review before the 2026-07-13 governance handoff |

## Four-Week Plan Update

| Week | Focus | Artifact |
|---|---|---|
| 2026-06-17 to 2026-06-23 | Close repository-side Domain 2 consolidation, record the first two 20-question practice blocks, and restart study cadence. | This consolidation note, tracker update, and wrong-answer log entries |
| 2026-06-24 to 2026-07-07 | Review weak areas from the completed blocks and keep study cadence moving. | Updated tracker notes, `docs/planning/domain-2-network-access-patterns-20260621.md`, and carry-forward review inputs |
| 2026-07-08 to 2026-07-12 | Decide whether Domain 2 is ready to hand off to the July 13 governance phase. | Tracker review with open risks and carry-forward items |

## Exit Assessment

The repository-side lakehouse closure is complete for June-July, and the first
two practice blocks plus separate Python/serverless tutorial evidence through
Lesson 33 are now recorded without reusing lakehouse evidence as tutorial
evidence. The remaining pre-governance work is Domain 2 carry-forward review,
not additional lakehouse closure implementation.
