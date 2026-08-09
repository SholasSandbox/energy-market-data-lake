# Domain 2 Resilience and DR Study Plan - 2026-07-18

<!-- markdownlint-disable MD013 MD060 -->

## Objective

Transition from the completed Networking study slice into the tracker-ordered
resilience and disaster-recovery workstream. Close the SAP-C02 decision gaps
around recovery objectives, DR patterns, backup, replication, recovery testing,
and the Lakehouse recovery boundary without creating AWS resources.

## Tracker Mapping

This plan supports:

- SAP-C02 Domain 2: design reliable new solutions;
- SAP-C02 Domain 3: improve recovery, operations, and testability;
- the accelerated 2026-07-27 Resilience/DR closure gate and the formal
  2026-09-07 exam-readiness review;
- the required DR pattern matrix and RTO/RPO decision table; and
- the booking criterion that currently retains major DR and migration unknowns.

The transition is explicitly authorized by the learner's instruction to proceed
to the next tracker-controlled priority.

## Existing Evidence Boundary

The repository currently proves:

- a working single-Region raw-to-curated Lakehouse path;
- S3 versioning for overwrite/delete recovery;
- lifecycle controls for current and noncurrent objects;
- infrastructure and operational evidence for the existing pipeline; and
- governance foundations that could support future cross-account controls.

It does not currently prove:

- approved workload RTO or RPO targets;
- a cross-Region data copy or application recovery environment;
- an AWS Backup plan, vault, restore test, or recovery policy for the Lakehouse;
- a tested Regional failover or failback path; or
- a business requirement that justifies pilot light, warm standby, or
  multi-site active/active cost and complexity.

## Bounded Scope

### Included

1. DR-pattern selection using RTO, RPO, cost, and operational complexity.
2. RTO/RPO decision rules and dependency alignment.
3. Backup versus replication versus high-availability distinctions.
4. AWS Backup, AWS Elastic Disaster Recovery, S3 replication, and common
   database recovery patterns at exam-decision depth.
5. A Lakehouse recovery posture and promotion-trigger review.
6. A source-backed scenario review followed by a separate blind attempt.

### Excluded

- live AWS Backup, DRS, S3 replication, multi-Region, Route 53, or networking
  changes;
- numeric Lakehouse recovery objectives without a business owner;
- a production active/active implementation;
- unrelated application, dashboard, AI, Kubernetes, or container expansion;
  and
- treating documentation as proof that a restore or failover works.

## Ordered Deliverables

| Order | Deliverable | Completion rule | Status |
|---:|---|---|---|
| 1 | DR pattern matrix and key lesson | Compare backup/restore, pilot light, warm standby, and multi-site active/active; distinguish HA, backup, replication, and DR | Completed in `docs/exam-prep/revision-notes/targeted-lessons/aws-resilience-dr-sap-c02-key-lessons-20260718.md` |
| 2 | RTO/RPO decision table | Define business-led selection rules, dependency constraints, and qualitative recovery tiers without inventing Lakehouse targets | Completed in `docs/planning/domain-2-rto-rpo-decision-table-20260718.md` |
| 3 | Lakehouse recovery mapping | Map S3, Glue, Athena, IAM, infrastructure definitions, and operational dependencies to recovery mechanisms and gaps | Completed in `docs/planning/domain-2-lakehouse-recovery-mapping-20260719.md` |
| 4 | Source-backed scenarios | Test pattern selection, backup/replication boundaries, Regional failure, cyber recovery, and service-specific cues | Completed in `docs/exam-prep/resilience-dr-scenario-drill-review-20260719.md`; answer-bearing review |
| 5 | Recall submission | Keep questions and explanations separate; score only after explicit submission | Submitted 2026-07-20 at 12/12 in `docs/exam-prep/resilience-dr-scenario-drill-submission-20260720.md`; learner-attested no-key first attempt, but the answer-bearing source means structural isolation is not claimed |
| 6 | Fresh question-only spaced retest | Use a new question-only set after spacing; freeze answers before scoring | Next Resilience/DR evidence gate, no earlier than 2026-07-27 |

## Quality Gates

- Every pattern must state the winning requirement, meaningful alternatives,
  cost/complexity trade-off, and failure or test assumptions.
- RTO and RPO must be objectives derived from business impact, not copied from
  generic service claims.
- Multi-AZ availability must not be described as multi-Region DR.
- Replication must not be described as a substitute for versioned or immutable
  recovery from corruption and malicious deletion.
- A backup is not considered proven until restore integrity and recovery time
  are tested.
- Lakehouse recommendations remain proposals until a named requirement and
  explicit AWS-change approval exist.

## State Transition

The short-plan gate has occurred. The resilience/DR documentation workstream is
active, and its DR pattern matrix, business-led RTO/RPO decision table,
Lakehouse recovery mapping, source-backed scenario review, and focused 12/12
learner submission are complete in the repository. The submission is untimed and carries
an explicit structural-isolation caveat because its source was answer-bearing.
No transition to live implementation has occurred or is authorized.

The next Resilience/DR evidence gate is a fresh, separate question-only spaced
retest no earlier than 2026-07-27. Its answers must be frozen and explicitly
submitted before any scoring or review.
