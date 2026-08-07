<!-- markdownlint-disable MD013 MD060 -->

# SAP-C02 Revision Notes Library

**Last consolidated:** 2026-08-07
**Canonical location:** `docs/exam-prep/revision-notes/`

## Purpose

This directory is the single repository home for SAP-C02 **revision and
mental-model notes** that previously lived across a standalone revision pack,
a governance study repository, and Downloads.

It does not absorb artifacts whose location expresses their role:

- the controlling readiness tracker remains in `docs/planning/`;
- mock examinations, frozen submissions, reviews, and wrong-answer evidence
  remain in `docs/exam-prep/`;
- Lakehouse design, ADR, implementation, and evidence artifacts remain in
  their existing repository directories; and
- Python/serverless tutorial lessons remain in the separate tutorial
  workspace and are not Lakehouse implementation evidence.

## Library Structure

| Area | Use it for | Contents |
|---|---|---|
| [Core revision pack](core/README.md) | Broad service selection, scenario cues, decision matrices, traps, and flashcards | Seventeen numbered chapters plus provenance metadata |
| [Governance and mental models](governance/) | Organizations, IAM Identity Center, IAM, multi-account networking, cross-account observability, and four reference diagrams | Five consolidated study guides |
| [Domain deep dives](domain-deep-dives/) | Exam-domain synthesis that cuts across individual services | Domain 3 continuous-improvement guide |
| Targeted lessons in the parent folder | Newer, assessment-led remediation where the broad pack is not deep enough | Networking, Route 53, resilience/DR, non-relational databases, and migration lessons |

## Recommended Route

1. Start with the [exam scope and study map](core/00-exam-scope-and-study-map.md).
2. Use the [service-scenario index](core/01-service-scenario-index.md) for fast
   recognition practice.
3. Read the relevant numbered core chapter.
4. If the topic has a newer targeted lesson, use that for the final decision
   boundaries:

   - [Networking beyond Route 53](../aws-networking-sap-c02-key-lessons-20260717.md)
   - [Route 53](../route-53-sap-c02-key-lessons-20260715.md)
   - [Resilience and disaster recovery](../aws-resilience-dr-sap-c02-key-lessons-20260718.md)
   - [Non-relational databases](../aws-non-relational-databases-sap-c02-key-lessons-20260724.md)
   - [Migration, discovery, transfer, tracking, and Transform](../aws-migration-discovery-transfer-tracking-sap-c02-key-lessons-20260728.md)

5. Close the notes before attempting a question-only mock or retest.

## Canonical and Duplicate Rules

- The numbered files in `core/` are canonical. The former combined
  `sap-c02-revision-pack-v2.md` repeated those chapters and was removed during
  consolidation.
- The governance copy of `SAP-C02_Mental_Model_Reference_Diagrams.md` contained
  later accuracy corrections. The older Downloads copy was superseded and
  removed.
- Broad core chapters and targeted lessons are **not** classified as
  duplicates. The core chapters provide coverage; the targeted lessons record
  deeper and newer assessment-led remediation.
- The [consolidation manifest](CONSOLIDATION-MANIFEST.md) records origins,
  exclusions, and recoverable duplicate disposition.
