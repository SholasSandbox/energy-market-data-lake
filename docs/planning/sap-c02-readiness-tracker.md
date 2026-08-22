<!-- markdownlint-disable MD013 MD060 -->

# SAP-C02 Readiness Tracker

**Owner:** [redacted-owner]  
**Created:** 2026-06-12  
**Last revised:** 2026-08-22<br>
**Target exam:** AWS Certified Solutions Architect – Professional, SAP-C02  
**Target attempt window:** September 2026; no later than 2026-09-30
**Earliest booking decision gate:** completed 2026-08-22 after full mock 009; **recommendation:** GO; **booking:** not performed
**Weekly capacity assumption:** 10–12 focused hours while not working  
**Controlling principle:** SAP-C02 is the steering architecture. The Energy Data Lakehouse is the practical case study. Everything else must support exam readiness, lakehouse credibility, or job-market positioning.
**Last repository reconciliation:** 2026-08-22
**Last practice evidence update:** 2026-08-22
**Last readiness-state publication:** 2026-08-22 (Mocks 008-009, exact-match reviews, GO recommendation, bounded final-review plan, and 7 Rs strategy matrix; see Git history)
**Last tutorial evidence update:** 2026-07-01
**Last governance study evidence reconciliation:** 2026-07-19 (published evidence through 2026-07-13; later legend edits remain local)

---

## 0. Steering Rules

### Non-negotiable rule

Every study/build session must produce at least one artifact:

- code commit
- architecture diagram
- Architecture Decision Record (ADR)
- service comparison table
- IAM/SCP policy example
- wrong-answer log entry
- exam-domain note
- operational runbook/checklist

### Scope filter

Before adding any new topic, answer:

1. Does it improve SAP-C02 readiness?
2. Does it strengthen the Energy Data Lakehouse case study?
3. Does it improve job-market positioning within 4 weeks?
4. Is it required for IAM, governance, networking, resilience, migration, or cost?

If the answer is **no**, defer it.

### Hard deferrals until after SAP-C02 attempt

- Deep Kubernetes / EKS
- AI orchestration beyond light conceptual notes
- Polished UI/dashboard
- Full Control Tower deployment unless cheap and quick
- Complex microservices platform
- Deep REMIT workflow build-out
- Excessive Python refinement beyond reliable AWS automation
- Non-essential portfolio polish

### Status and evidence rules

| Status | Meaning |
|---|---|
| Verified | Implemented and supported by current repository or live evidence |
| Implemented | Code or configuration exists, but current end-to-end or live evidence is incomplete |
| Partial | Some of the outcome exists, but a material design, security, evidence, or completion gap remains |
| In progress | The item is the active programme focus and still has open completion criteria |
| Not started | No sufficient implementation or evidence was found |
| Deferred | Intentionally frozen by the scope rules |

Status must follow evidence, not intention. A Terraform resource proves an
implemented configuration path; it does not by itself prove that a live
resource exists. Existing dashboard and AI capabilities may be maintained, but
new expansion remains deferred unless the tracker is explicitly changed.

This vocabulary applies to readiness and deliverable status fields. Booking
criteria use `Met`, `Partially met`, or `Not met`; checklist items use their
checkbox state.

### Planning authority

1. This tracker controls scope, milestones, and next-step priority.
2. `README.md` describes current implemented platform truth.
3. `PLANS.md` preserves delivery history and translates this tracker into the
   current execution sequence.
4. Phase documents and evidence files prove individual outcomes; they do not
   override tracker deferrals or sequencing.

### Programme workspace and sequence control

#### Python/serverless tutorial workspace

Purpose: advance practical AWS serverless implementation skill and SAP-C02
readiness through Lambda, Step Functions, S3, DynamoDB, EventBridge, SQS, IAM,
observability, testing, and production-oriented handler patterns.

Source of truth: `/Users/[redacted-user]/Kiro-Workspace/handlers/LEARNING-PLAN.md`.
This is a separate tutorial workspace, not part of the Energy Data Lakehouse
implementation. Its sessions count as study/lab work only when mapped to an
exam domain or documented weak area.

#### Energy Data Lakehouse repository

Purpose: provide the portfolio and SAP-C02 case study for governed AWS data
architecture, including S3, Glue, Athena, Parquet, IAM, KMS, CloudTrail, AWS
Config, cost controls, and Organizations/SCP guardrails.

Source of truth: this repository, its README, its evidence files, and the
lakehouse sections of this tracker. Its repo-side June-July closure milestone is
complete; the remaining work before the formal Domain 1 focus is study cadence,
practice review, and carry-forward review.

#### Scheduled AWS governance and multi-account work

Purpose: add professional-level governance competence through AWS
Organizations, OUs, SCPs, IAM Identity Center, centralized logging, audit
controls, and cost governance.

This work supports both workspaces where relevant, but it is sequenced rather
than maintained as a third simultaneous backlog. The formal Domain 1 governance
focus begins on 2026-07-13. Before then, repo-only design artifacts may begin
once the prior state is complete and no architectural, design, structural, or
sequencing blocker remains. Bounded live governance changes before that focus
require separate explicit approval, prechange evidence, and defined rollback,
validation, cost, and blast-radius boundaries; they do not open a general
implementation backlog.

#### Parked work

- Docker and containers
- new AI orchestration expansion

Do not start parked work unless the task directly supports a current
SAP-C02 milestone and is explicitly approved as a short exception. Maintenance,
observation, cost control, and rollback work for the already-proven lakehouse AI
workflow are allowed when required by this tracker.

#### Cross-workspace evidence rule

Tutorial code must not be described as lakehouse implementation evidence,
copied into this repository by default, or used to bypass the lakehouse closure
sequence. Before promoting a tutorial pattern into the lakehouse:

1. Identify the tracker closure gap or SAP-C02 weak area it addresses.
2. Record why it belongs in the lakehouse rather than remaining a standalone
   exercise.
3. Adapt and test it against lakehouse contracts and IAM boundaries; do not
   copy tutorial code unchanged.
4. Update a lakehouse status only after repository evidence exists.

### 2026-06-12 repository reconciliation findings

| Tracker area that was behind | Repository reality | Reconciliation |
|---|---|---|
| Domain 1 marked `Not started` | Workload IAM, logging, tagging, alerting, and cost evidence exist | Changed to `Partial`; enterprise governance remains open |
| Domain 3 marked `Not started` | Parquet, lifecycle, validation, observability, public-access controls, and cost guardrails exist | Changed to `Partial`; formal improvement evidence remains open |
| Raw and curated described as separate buckets | The implemented lake uses one data bucket with `raw/` and `curated/` zones | Corrected architecture and checklist wording; ADR accepted on 2026-06-13 |
| Entire lakehouse baseline marked `Not started` | S3 zones, Glue Catalog/ETL, Parquet, Athena, logging, tags, and diagrams are implemented or verified | Replaced blanket statuses with evidence-backed row statuses |
| Glue IAM described only as missing | A dedicated role exists, but its data-bucket access is too broad | Changed to `Partial`; least-privilege hardening added to closure checklist |
| Athena query access described only as missing infrastructure | The Athena workgroup exists, but no dedicated analyst/query role is defined | Split query-layer proof from the remaining access-policy gap |
| Governance cost controls marked `Not started` | Terraform tags and a live managed-workflow AWS Budget exist | Changed tags and budget rows to `Partial`; broader account coverage remains open |
| Lakehouse booking gate marked simply `Not met` | The core path is already proven, but security and consolidation gaps remain | Changed to `Partially met` with named blockers |
| AI and dashboard listed only as future deferred ideas | Both are already implemented; the managed workflow is scheduled and budget-guarded | Reframed as frozen existing baselines with maintenance only |
| `PLANS.md` and `README.md` still directed work toward Phase 17 continuation | The new programme makes this tracker controlling | Added an active SAP-C02 sequence and preserved old phases as history |
| Phase 1 checklist assumes the platform is still unimplemented | Mixed-energy ingestion, ETL, Athena, dashboard, and evidence already exist | Added reconciliation of that checklist as a June-July closure task |

---

## 1. Target Exam Date

| Item | Target |
|---|---|
| Earliest evidence-led booking decision | Completed 2026-08-22 after full mock 009 and seven further mocks after full mock 002 |
| Formal readiness-review backstop | No longer required as a decision gate; use Monday, 2026-09-07 as a bounded plan checkpoint |
| Earliest exam attempt | 2026-09-14 |
| Preferred exam window | 2026-09-21 to 2026-09-30; preferred appointment sub-window 2026-09-23 to 2026-09-25 |
| Latest practical exam attempt | 2026-09-30 |
| Exam booking rule | Satisfied on 2026-08-22: post-Mock-009 review found the quantitative gate, stable domains, exact-match performance, remediation transfer, and no recurring unresolved trap; purchase still requires explicit authorization |

### Booking decision criteria

| Practice evidence at the post-mock-009 decision point | Decision |
|---:|---|
| 80%+ twice timed | Book the September attempt |
| 75–79% timed | Book September only if weak areas are narrow and can be remediated before the selected date |
| 65–74% timed | High-risk attempt; intensify remediation and use the September 7 backstop review to reassess |
| Below 65% timed | Do not book unless accepting likely failure |

### Evidence decision point after mock 009; formal backstop Monday, 2026-09-07

No booking decision occurs before full mock 009. Once at least Mocks 003–009
are complete and assessed, perform the first evidence-led booking review. If
the evidence is not yet sufficient, continue the mock programme and use Monday,
2026-09-07 as the formal backstop review. Either review must produce a recorded
decision and a bounded plan through the selected September exam date. Review:

1. all completed full timed-exam scores, durations, and domain breakdowns,
   including at least Mocks 001–009;
2. unresolved wrong-answer themes and whether each is narrow and remediable;
3. completion of the Resilience/DR, migration, and cost decision artifacts;
4. the remaining booking criteria in Section 10;
5. exam availability and booking status for a date no later than 2026-09-30;
   and
6. the final remediation schedule between the review and exam attempt.

The one-mock extension from Mock 007 to Mock 008 was adopted on 2026-08-09
after the AWS Skill Builder assessment exposed a broader layer of service-
boundary detail than the generated full mocks had tested. It does not reflect
a collapse in timed performance and does not move the September exam window.
It creates three independent transfer opportunities—Mocks 006, 007, and 008—
after the new revision material was added, which is stronger evidence than an
immediate post-remediation booking decision. On 2026-08-14, the learner chose
one further evidence-building extension to Mock 009 while remaining on the
September schedule. Mock 007 was completed on 2026-08-15 at 75/75 in 142
minutes, with all four Mock 006 remediation targets transferred. Mocks 008 and
009 were completed in the week beginning 2026-08-17, preserving two full mocks
in that study week before the go/no-go review. Mock 008 was completed on
2026-08-18 at 75/75: 139 wall-clock minutes included an explicit 22-minute
pause, giving 117 active minutes; all 27 multiple-response and all eight
uncertain answers were correct. Mock 009 was completed on 2026-08-22 at
73/75: 47/48 single-response, 26/27 exact-match multiple-response, 15/16
uncertain answers, and 101 active minutes after a five-minute pause. The
learner-selected evidence gate is now reached; the explicit booking review is
recorded below and recommends GO. This is a recommendation to book, not an
external booking or authorization to incur the exam charge.

### Post-Mock-009 go/no-go review - 2026-08-22

**Recommendation: GO.** Prefer an appointment from Wednesday, 2026-09-23 to
Friday, 2026-09-25. Use 2026-09-28 to 2026-09-30 only as the fallback window.
No appointment was selected, held, purchased, or otherwise booked during this
review.

| Decision factor | Evidence and disposition |
|---|---|
| Nine timed full mocks and trend | 656/675 overall (97.2%); range 70/75 to 75/75 (93.3% to 100%). Every mock exceeded 80%. The last three scored 223/225 (99.1%), so Mock 009's 73/75 after two 75/75 results is normal narrow variance, not decline. |
| Timing and endurance | Every attempt completed within the 180-minute active allowance; Mock 006 used an estimated 180 active minutes and is the qualified edge case. Mocks 007-009 used 142, 117, and 101 active minutes, leaving substantial review capacity. |
| Domain floors | No primary-domain result fell below 75%; the lowest recorded floor was Domain 3 at 14/18 (77.8%) in Mock 004 and it did not recur. Mock 009 scored 100%, 95.5%, 94.4%, and 100% across Domains 1-4. |
| Exact-match multiple-response | 234/243 (96.3%) across Mocks 001-009, graded only by exact match. Every mock exceeded the 80% threshold; the lowest was 24/27 (88.9%) in Mock 006, followed by 27/27, 27/27, and 26/27. |
| Uncertain answers | Mocks 002-009 recorded 88/95 correct (92.6%); Mock 001 did not record an uncertainty set. Mocks 007-009 recorded 30/31 correct (96.8%). Uncertainty is therefore well calibrated rather than a hidden failure mode. |
| Earlier remediation transfer | Mock 003 retained all Mock 002 themes; Mock 005 closed the Lambda@Edge/write-continuity gap; Mock 006 retained ARC and AS2; Mock 007 transferred all four Mock 006 composition gaps; Mock 008 retained the earlier organization-control, DynamoDB, CloudFront, Batch, warm-standby, AS2, and migration boundaries. No recurring unresolved trap remains. |
| Mock 009 misses | Q11 is a narrow conventional PrivateLink NLB-versus-GWLB endpoint-role discrimination; Q32 retained the broader PrivateLink composition. Q40 is a narrow action-versus-query exact-match miss: Inventory supplies the manifest and Batch Operations Copy performs the rewrite; S3 Select does not. Neither miss warrants a broad restart or another full mock. |
| Migration matrix | At decision time, the strategy artifact was incomplete but non-blocking because Domain 4 scored 133/135 (98.5%) across the nine mocks, including 15/15 in Mocks 006-009. The learner subsequently completed the 7 Rs matrix, including Relocate, at `docs/planning/domain-4-migration-decision-matrix-20260823.md`. The separate database and data-transfer comparisons remain bounded consolidation work rather than a new decision gate. |
| September availability | The signed-in AWS Certification account listed SAP-C02 as an eligible exam and showed no existing appointment on 2026-08-22. AWS states that most online-proctored appointments are available 24/7. Exact Pearson VUE date/time inventory remains unverified until the separately authorized scheduling flow; confirm a 2026-09-23 to 2026-09-25 slot before payment. |
| Tutorials Dojo | Optional corroborating evidence only. The current SAP-C02 product offers six timed sets, but adding another provider as a mandatory gate after the agreed nine-mock programme would move the decision boundary without evidence of a readiness gap. If used, take at most one previously unseen timed set and review it diagnostically; its result does not reopen the GO decision unless it exposes a broad recurring weakness. |

Official scheduling references: [AWS schedule-an-exam options](https://aws.amazon.com/certification/certification-prep/testing/)
and [AWS before-testing and rescheduling policy](https://aws.amazon.com/certification/policies/before-testing/).
Optional third-party reference: [Tutorials Dojo SAP-C02 practice exams](https://portal.tutorialsdojo.com/courses/aws-certified-solutions-architect-professional-practice-exams/).

### Bounded final-review plan

| Period | Maximum scope | Required outcome |
|---|---|---|
| 2026-08-24 to 2026-08-30 | 4-6 focused hours | The 7 Rs strategy matrix was completed early, including Relocate. Use the remaining scope for one short closed-book recall pass on NLB/GWLB endpoint roles and Inventory/Batch Operations/S3 Select; no full mock. |
| 2026-08-31 to 2026-09-06 | 5-7 focused hours | Complete the separate database and data-transfer comparisons; one mixed exact-match drill of no more than 20 questions. |
| 2026-09-07 | 30-minute checkpoint | Confirm the 7 Rs matrix and remaining migration comparisons are usable, the two Mock 009 rules are recalled, the appointment is booked if separately authorized, and no new broad weakness exists. This is a plan check, not a second booking gate. |
| 2026-09-08 to 2026-09-13 | 4-6 focused hours | Review only the recurring service-boundary families from the wrong-answer log and the official exam guide; no content expansion. |
| 2026-09-14 to 2026-09-20 | 3-5 focused hours | One bounded cross-domain rehearsal or, optionally, one unseen Tutorials Dojo timed set. A tenth full mock is not required by default. |
| 2026-09-21 to exam day | 2-3 light hours plus logistics | Verify Pearson system test, ID/profile match, room and check-in requirements; use flash recall only on the 7 Rs, migration-service boundaries, and other narrow service distinctions. Stop heavy study the day before the exam. |

Another full mock is required only if a new broad weakness appears, exact-match
discipline materially deteriorates, or the exam moves beyond 2026-09-30. It is
not required by the current evidence.

---

## 2. Weekly Hours Logged

Target: **10–12 focused hours/week**.

During June-July, record Python/serverless tutorial sessions as study/lab hours
and Energy Data Lakehouse implementation as build hours. Use the Notes column
to identify the workspace and artifact. Do not count one artifact as evidence
for both workspaces.

For completed historical weeks, use `Not recorded` when durable time data is
unavailable; do not infer hours from artifact timestamps. Use `In progress` for
the current week and leave future planned weeks blank.

### Programme kickoff

| Date | Session | Planned artifact | Status |
|---|---|---|---|
| Sunday, 2026-06-14 target | Lakehouse architecture decision | Shared S3-zone and bucket naming/ownership ADR | Completed 2026-06-13 |

| Week starting | Target hours | Actual hours | Build hours | Study hours | Practice hours | Notes |
|---|---:|---:|---:|---:|---:|---|
| 2026-06-15 | 10–12 | 20 | 6 | 10 | 4 | Tutorial Lesson 26 evidence: `/Users/[redacted-user]/Kiro-Workspace/handlers/learning-summary.md`; Glue/Athena IAM evidence: `docs/evidence/glue-athena-iam-live-verification-20260615.md`; practice blocks: Sections 8 and 9 below |
| 2026-06-22 | 10–12 | 18 | 9 | 4 | 5 | Tutorial hardening evidence now includes Lesson 28 boundary isolation and the Ruff formatting baseline in `/Users/[redacted-user]/Kiro-Workspace/handlers/LEARNING-PLAN.md` and `learning-summary.md`; lakehouse IAM hardening |
| 2026-06-29 | 10–12 | 20 | 10 | 4 | 6 | Governance study evidence now includes SAP-C02 mental-model diagrams, OAM vs CloudTrail log archive vs AWS Config aggregator comparison, and local practice blocks 003-006 in `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance`; Serverless Architecture evidence now includes Lessons 29-33 and 217 local tests in `/Users/[redacted-user]/Kiro-Workspace/handlers`; no new lakehouse implementation evidence |
| 2026-07-06 | 10–12 | 20 | 12 | 4 | 4 | Tutorial evidence + lakehouse Domain 2 closure review |
| 2026-07-13 | 10–12 | 13 | 3 | 6 | 4 | IAM foundation in progress: `BillingAdmin` now has a design-only `billing-admins` group, `PT1H` session, and update-only policy; private primary/backup ownership with an email notification path, finalized monthly dataset, April-June cost history, separate pending IAM/budget change notes, and a lakehouse Workload-tag precheck are recorded. Fourteen core lakehouse resources are tagged and Terraform protects the future default. A 2026-07-13 read-only refresh attributes the previously untagged June Lakehouse spend and its EBS driver at account/service level. The missing finalized history is a non-blocking gate only for new thresholds or live BillingAdmin update access; no numeric threshold, assignment, or AWS change is authorized yet. Early Networking study also produced the verified VPC connectivity matrix and an untimed focused blind attempt scoring 8/8; no study/practice hours are inferred. |
| 2026-07-20 | 10–12 | 14 | 2 | 8 | 4 | The timed 30-question mixed diagnostic was completed from 00:00 to 01:07 on 2026-07-21: 29/30 in 67 minutes, with all 12 multiple-response questions correct. On 2026-07-23, full mock 001 scored 73/75 in 2 hours 13 minutes, with 47 minutes remaining. Its two narrow multiple-response traps are logged. On 2026-07-24, the 18-minute SCP/boundary exact-match retest scored 6/6; its early-spacing caveat is preserved. No other week-to-date hours are inferred. |
| 2026-07-27 | 10–12 | 15 | 1 | 6 | 8 | Resilience/DR and non-relational-database spaced retests completed; migration foundations expanded; full mock 002 scored 71/75; its fresh focused retest scored 8/8 in 17 minutes on 2026-08-01. No total hours are inferred from artifact timestamps. Full Mock 003 was the next broad check at week close. |
| 2026-08-03 | 10–12 | 14 | 0 | 6 | 8 | Full Mock 003 scored 75/75, Full Mock 004 scored 70/75, and its focused retest scored 7/8. Full Mock 005 then scored 73/75 in 108 minutes with 27/27 exact-match multiple-response, every domain above 93%, and successful Lambda@Edge transfer. Its two misses were ARC single-response over-selection and Transfer Family AS2 service selection. AWS Skill Builder official-practice attempt 2 also passed at scaled score 775 against 750, but its recorded 12h29 duration keeps it outside timed booking evidence. Continue to Mock 006 and use only remaining capacity for the migration matrix. No total hours are inferred from artifact timestamps. |
| 2026-08-10 | 10–12 | 15 | 0 | 6 | 9 | The focused Domain 3 diagnostic scored 25/25 in 47 minutes, including 20/20 on Domain 3. Full Mock 006 then scored 71/75 (94.7%): 190 wall-clock minutes with approximately 10 minutes of learner-reported short interruptions near the end, giving an estimated 180 active minutes; 24/27 exact-match multiple-response, 11/11 uncertain answers, Domains 1, 3, and 4 at 100%, and Domain 2 at 18/22. The learner completed the miss review on 2026-08-14. Full Mock 007 then scored 75/75 in 142 minutes on 2026-08-15: 48/48 single-response, 27/27 exact-match multiple-response, 7/7 uncertain, every domain at 100%, and all four Mock 006 remediation targets transferred. No total hours are inferred from artifact timestamps. |
| 2026-08-17 | 10–12 |  |  |  |  | Full Mocks 008 and 009 preserved the two-mock cadence. Mock 008 scored 75/75 in 117 active minutes. Mock 009 scored 73/75 on 2026-08-22 in 101 active minutes within a 106-minute wall clock: 47/48 single-response, 26/27 exact-match multiple-response, 15/16 uncertain, and primary-domain scores of 20/20, 21/22, 17/18, and 15/15. The post-Mock-009 gate is reached; run the explicit go/no-go booking review next. No total hours are inferred from artifact timestamps. |
| 2026-08-24 | 10–12 |  |  |  |  | The 7 Rs strategy matrix is complete, including Relocate. Begin bounded final review with the two Mock 009 service boundaries; no default full mock. |
| 2026-08-31 | 10–12 |  |  |  |  | Complete the remaining database and data-transfer comparisons by 2026-09-06 and run one mixed exact-match drill of no more than 20 questions. |
| 2026-09-07 | 10–12 |  |  |  |  | **Plan checkpoint:** verify the migration artifact, narrow recall, logistics, and booking state. The 2026-08-22 GO decision stands unless material contrary evidence appears. |
| 2026-09-14 | 10–12 |  |  |  |  | Targeted cross-domain rehearsal only; Tutorials Dojo is optional corroboration and a tenth full mock is not required by default. |
| 2026-09-21 | 10–12 |  |  |  |  | Preferred SAP-C02 appointment sub-window is 2026-09-23 to 2026-09-25; use only light recall and logistics preparation. |
| 2026-09-28 | 10–12 |  |  |  |  | Fallback SAP-C02 exam window; attempt no later than Wednesday, 2026-09-30. |

### External tutorial evidence register

| Date | Workspace lesson | Status | SAP-C02 mapping | Evidence and boundary |
|---|---|---|---|---|
| 2026-06-21 | Python/serverless Lesson 26: idempotency and duplicate persistence protection | Completed locally | Domain 2 resilience; Domain 3 continuous improvement | `/Users/[redacted-user]/Kiro-Workspace/handlers/LEARNING-PLAN.md` and `learning-summary.md`; 168 tests passed; fake conditional-check exception injected at `persist_trade_status_record`; no AWS deployment; not lakehouse implementation evidence |
| 2026-06-21 | Python/serverless Lesson 27: consolidation review | Completed locally | Domain 3 operational excellence | `/Users/[redacted-user]/Kiro-Workspace/handlers/LEARNING-PLAN.md` and `learning-summary.md`; naming/import/formatting cleanup recorded; README/git-baseline note refreshed; 168 tests passed; no AWS deployment; not lakehouse implementation evidence |
| 2026-06-25 | Python/serverless Lesson 28: persistence handler boundary hardening | Completed locally | Domain 3 operational excellence; Domain 2 resilience | `/Users/[redacted-user]/Kiro-Workspace/handlers/LEARNING-PLAN.md` and `learning-summary.md`; strict input validation, typed helpers, structured error responses, and 36 new handler-boundary tests recorded; 204 tests passed; no AWS deployment; not lakehouse implementation evidence |
| 2026-06-25 | Python/serverless Ruff formatting baseline | Completed locally | Domain 3 operational excellence | `/Users/[redacted-user]/Kiro-Workspace/handlers/LEARNING-PLAN.md` and `learning-summary.md`; `ruff` added to `pyproject.toml`; consistent style applied across 20 tutorial source/test files; 204 tests passed with no behaviour changes; no AWS deployment; not lakehouse implementation evidence |
| 2026-06-26 | Python/serverless Lesson 29: persistence failure ordering | Completed locally | Domain 2 resilience; Domain 3 continuous improvement | `/Users/[redacted-user]/Kiro-Workspace/handlers/docs/lessons/lesson-29-persistence-failure-ordering.md`, `tests/test_trade_persistence_workflow.py`, `LEARNING-PLAN.md`, and `learning-summary.md`; S3-success/DynamoDB-failure and deterministic retry key behavior documented; no AWS deployment; not lakehouse implementation evidence |
| 2026-06-26 | Python/serverless Lesson 30: least-privilege IAM checklist for persistence | Completed locally | Domain 1 secure architecture; Domain 3 security improvement | `/Users/[redacted-user]/Kiro-Workspace/handlers/docs/iam/persistence-handler-iam-checklist.md`, `LEARNING-PLAN.md`, and `learning-summary.md`; Lambda vs Step Functions role boundary, S3/DynamoDB/log permissions, and encryption cautions documented; no AWS deployment; not lakehouse implementation evidence |
| 2026-06-27 | Python/serverless Lesson 31: retry-safe persistence and reconciliation | Completed locally | Domain 2 resilience; Domain 3 operational excellence | `/Users/[redacted-user]/Kiro-Workspace/handlers/docs/lessons/lesson-31-retry-safety-and-reconciliation.md`, `LEARNING-PLAN.md`, and `learning-summary.md`; retry, catch, fail, reconciliation, and no-default-delete compensation guidance documented; no AWS deployment; not lakehouse implementation evidence |
| 2026-06-27 | Python/serverless Lesson 32: S3 key design and encryption assumptions | Completed locally | Domain 1 security boundaries; Domain 2 storage design; Domain 3 improvement | `/Users/[redacted-user]/Kiro-Workspace/handlers/docs/lessons/lesson-32-s3-key-design-and-encryption.md`, `LEARNING-PLAN.md`, and `learning-summary.md`; accepted/rejected prefixes, deterministic keys, overwrite behavior, and bucket default encryption assumptions documented; no AWS deployment; not lakehouse implementation evidence |
| 2026-07-01 | Python/serverless Lesson 33: Step Functions timeout and terminal failure | Completed locally | Domain 1 role-boundary reasoning; Domain 2 resilience; Domain 3 continuous improvement | `/Users/[redacted-user]/Kiro-Workspace/handlers/step-functions/persistence-task-timeout-terminal-failure.asl.json`, `tests/test_step_functions_timeout_terminal_failure_definition.py`, `docs/lessons/lesson-33-step-functions-timeout-and-terminal-failure.md`, `LEARNING-PLAN.md`, and `learning-summary.md`; timeout, bounded retry, catch, reconciliation routing, explicit fail state, and Lambda-only Step Functions role boundary verified; 217 tests passed; no AWS deployment; not lakehouse implementation evidence |

### External governance study evidence register

| Date | Governance study artifact | Status | SAP-C02 mapping | Evidence and boundary |
|---|---|---|---|---|
| 2026-06-26 | Organizations, Identity Center, IAM, and multi-account Networking study guides | Committed to governance repo | Domain 1 governance and networking; Domain 2 solution design | `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/SAP-C02_Organizations_IdentityCenter_IAM.md` and `SAP-C02_MultiAccount_Networking.md`; committed in governance repo `7eb4026`; external revision evidence, not lakehouse implementation evidence |
| 2026-06-27 | SAP-C02 mental-model reference diagrams | Committed to governance repo | Domains 1-4 mental-model consolidation | `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/SAP-C02_Mental_Model_Reference_Diagrams.md`; committed in governance repo `d068a65`; external revision evidence, not lakehouse implementation evidence |
| 2026-06-28 | OAM vs CloudTrail log archive vs AWS Config aggregator comparison | Committed to governance repo | Domain 1 governance; Domain 3 observability and improvement | `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/SAP-C02_Security_Observability_Comparison.md`; committed in governance repo `d068a65`; supports the Security Tooling vs Log Archive split recorded in ADR 0005 |
| 2026-07-01 | SAP-C02 practice review blocks 003-006 | Committed and pushed to governance repo | Domains 1-4 practice remediation | `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/exercises/sap-c02-exercise-block-003-review.md` through `sap-c02-exercise-block-006-review.md`; committed and pushed in governance repo `5f6158e`; updated notes include Block 006's answer-distribution quality caveat |
| 2026-07-02 | Governance structure mental model | Committed to governance repo | Domain 1 account, OU, SCP, identity, and emergency-access relationships | `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/SAP-C02_Governance_Structure_Mental_Model.md`; committed in governance repo `16c6a69`; external revision evidence, not lakehouse implementation evidence |
| 2026-07-13 | Mental-model reference refinement | Committed and pushed to governance repo | Domains 1-4 mental-model consolidation | `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/SAP-C02_Mental_Model_Reference_Diagrams.md`; governance repo `HEAD` and `origin/main` are `035857b`; external revision evidence, not lakehouse implementation evidence |
| 2026-07-19 | Acronym and term legend improvements across governance study guides | Completed locally; unpublished | Domains 1-4 revision usability | Five modified Markdown files remain uncommitted in the governance workspace; this is local study evidence only and must not be described as published until committed and pushed |
| 2026-08-07 | Revision-note library consolidation | Completed locally; unpublished | Domains 1-4 revision discoverability and source-note control | Canonical revision chapters, governance mental models, and the Domain 3 deep dive now live under `docs/exam-prep/revision-notes/`; two redundant reading copies were moved to Trash; mocks, evidence, trackers, runbooks, and tutorial artifacts remained in their authoritative locations; no new recall or implementation evidence |

---

## 3. SAP-C02 Domain Coverage

Official SAP-C02 domains:

| Domain | Weight | Status | Evidence required |
|---|---:|---|---|
| Domain 1: Design Solutions for Organizational Complexity | 26% | Partial | Workload IAM, logging, tagging, budget evidence, Organizations membership, selected Cost Allocation Tag activation, governance preflight evidence, Security Tooling vs Log Archive separation, external governance study diagrams, the first two Lakehouse Workloads OU SCP guardrails, live Security Tooling account placement, Security Tooling alternate-contact readiness, AWS Config delegated-admin migration, Security Tooling AWS Config recorder onboarding, the `so-aws-admin` decommission-path decision, final closure package, and account-closure evidence, GuardDuty delegated-admin planning, GuardDuty live-readiness evidence, and live GuardDuty delegated administration with foundational coverage in `eu-west-2` exist. Full Mock 009 scored 20/20 in this primary-domain mapping; Security Hub/OAM adoption, broader Identity Center model, and enterprise networking remain open |
| Domain 2: Design for New Solutions | 29% | In progress | Lakehouse readiness closure, repository-side Domain 2 consolidation, two 20-question practice blocks, separate tutorial evidence through Lesson 34A with conceptual/design evidence through Lesson 37, later practice blocks 003-006, the Networking comparison package, the DR pattern matrix, the RTO/RPO decision table, the Lakehouse recovery mapping, the source-backed DR scenario review, a focused 12/12 Resilience/DR submission, the fresh 12/12 structurally isolated DR retest, and a 29/30 timed mixed diagnostic are complete repository evidence. Full Mock 009 scored 21/22, with one narrow NLB-versus-GWLB endpoint-role miss; Question 32 retained the broader PrivateLink composition. Later migration decisions remain open |
| Domain 3: Continuous Improvement for Existing Solutions | 25% | Partial | Parquet, lifecycle, validation, observability, public-access controls, alerting, cost guardrails, separate tutorial implementation evidence through Lesson 34A with conceptual/design evidence through Lesson 37, the 218-test tutorial baseline, and OAM vs CloudTrail vs Config study evidence exist. A timed focused diagnostic scored 20/20 on Domain 3, with 4/4 in each current task area, and Full Mock 009 scored 17/18; its one miss was S3 Batch Operations re-encryption exact-match completeness. Systematic repository improvement notes and remaining hardening are still open |
| Domain 4: Accelerate Workload Migration and Modernization | 20% | Partial | Exercise 002 exposed a rehost-vs-refactor/MGN weak area; later practice blocks show stronger mixed-domain performance, the Domain 3 diagnostic cross-domain controls scored 5/5, and Full Mock 009 scored 15/15 in this primary-domain mapping with complex migration composition retained. The 7 Rs strategy matrix is complete, including Relocate; the migration playbook and separate database/data-transfer comparisons remain open |

Full Mocks 001-009 now provide repeated evidence across all four domains, with
overall scores of 97.3%, 94.7%, 100%, 93.3%, 97.3%, 94.7%, 100%, 100%, and
97.3%. Mock 009 scored 26/27 on exact-match multiple-response and 15/16 on
uncertain answers in 101 active minutes within a 106-minute wall-clock
interval. The untimed Skill Builder
assessment adds calibration depth but does not replace the timed series. This
remains study evidence rather than closure of repository artifacts or the
post-Mock-009 booking gate.

### Weekly domain focus

| Period | Primary domain focus | Secondary focus |
|---|---|---|
| 2026-06-15 to 2026-07-12 | Domain 2 | Domain 3 |
| 2026-07-13 to 2026-07-18 | Domain 1 | Domain 2 Networking |
| 2026-07-19 to 2026-07-26 | Domain 2 Resilience/DR | Domain 3 recovery validation |
| 2026-07-27 to 2026-08-16 | Domain 4 Migration | Domain 2 Resilience/DR retention and Domain 3 cost foundations |
| 2026-08-17 to 2026-09-06 | All domains | Full timed exams and evidence-led remediation |
| 2026-09-07 | All domains | Formal readiness and booking review |
| 2026-09-08 to 2026-09-30 | Narrow confirmed weak areas | SAP-C02 exam attempt |

---

## 4. Energy Data Lakehouse Readiness Status

### Target architecture

```text
Energy Data Lakehouse
│
├── S3 data bucket
│   ├── raw/ zone
│   └── curated/ zone
├── Glue Data Catalog
├── Glue ETL to Parquet
├── Athena query layer
├── IAM roles and policies
├── KMS encryption
├── CloudWatch logs
├── CloudTrail / AWS Config design
├── Cost tags and budgets
└── Governance guardrails using Organizations/SCPs
```

### Implemented baseline and closure status

| Item | Status | Evidence |
|---|---|---|
| S3 raw zone available | Verified | Shared data bucket with `raw/` prefix; `README.md`; `docs/entsog-gas-build-plan.md`; `docs/evidence/phase14d-lambda-reconciliation-apply-summary-20260521.md` |
| S3 curated zone available | Verified | Shared data bucket with `curated/` prefix; curated Parquet and `docs/evidence/athena-gas-query-summary-20260506.md` |
| Bucket naming and ownership standard defined | Verified | `docs/adr/0001-shared-s3-data-bucket.md` accepts the current name, defines the naming pattern, and distinguishes workload ownership from Terraform management |
| Data bucket versioning posture | Verified | Versioning was enabled on 2026-06-15 after lifecycle protection was applied; see `docs/evidence/s3-versioning-tagging-apply-20260615.md` |
| Encryption model and KMS target defined | Verified | `docs/adr/0002-encryption-and-kms-design.md` retains SSE-S3 for the current public-data lab and defines promotion triggers, ownership, rotation, migration, rollback, and cost controls; `docs/policies/kms-lakehouse-key-policy.example.json` provides the policy design |
| Data bucket Public Access Block | Verified | All four bucket-level public-access controls were enabled on 2026-06-14 |
| Data bucket lifecycle protection | Verified | Raw transitions remain 30/90/180 days; noncurrent versions expire after 30 days, expired delete markers are removed, and incomplete multipart uploads abort after 7 days |
| Glue Data Catalog created | Verified | `aws_glue_catalog_database.lakehouse`, raw/curated crawlers, and Phase 9 import evidence |
| Glue ETL job converts raw to Parquet | Verified | `glue/etl_raw_to_parquet.py`, `aws_glue_job.raw_to_parquet`, and ENTSOG curated Parquet evidence |
| Athena can query curated data | Verified | Athena workgroup plus schema/query evidence under `docs/evidence/athena-*` |
| IAM role for Glue least privilege | Verified | Terraform restricts listing and reads to `raw/`, `curated/`, and `scripts/`, with writes/deletes limited to `curated/`; live Glue crawler/job verification passed on 2026-06-15 |
| IAM role or policy for Athena query access | Verified | Dedicated role is workgroup-scoped, catalog-read-only, limited to `curated/` reads and `athena-results/` writes; assumed-role Athena query passed and raw-prefix list was denied on 2026-06-15 |
| CloudWatch logging enabled | Implemented | Lambda log groups, Glue continuous logging, Glue metrics, and Athena workgroup metrics are configured |
| Cost and ownership tags applied | Verified | Eight live data-bucket tags were verified on 2026-06-15; selected Billing Cost Allocation Tags were activated from the Organizations management account on 2026-06-17 |
| Architecture diagram created | Verified | Current and target diagrams exist under `diagrams/`; `docs/target-operating-model.md` documents the target posture |

### Lakehouse closure gaps

The core lakehouse path is implemented, and the June-July closure gaps are
complete with evidence links:

- [x] On 2026-06-13, record an ADR confirming one shared data bucket
  with `raw/` and `curated/` prefixes, including when separate buckets would be
  preferable: `docs/adr/0001-shared-s3-data-bucket.md`.
- [x] In the same ADR, record the bucket naming and ownership standard,
  including the current referenced-but-not-Terraform-managed posture.
- [x] On 2026-06-14, verify the live data bucket's versioning, encryption,
  public-access block, lifecycle, and tags using read-only commands:
  `docs/evidence/s3-data-bucket-posture-20260614.md`.
- [x] On 2026-06-15, enable versioning after applying 30-day noncurrent-version
  expiry, expired delete-marker cleanup, and 7-day multipart-upload cleanup.
- [x] On 2026-06-15, apply and verify the approved ownership, classification,
  environment, workload, and cost-attribution tags:
  `docs/evidence/s3-versioning-tagging-apply-20260615.md`.
- [x] On 2026-06-17, activate selected user-defined cost-allocation tags in
  AWS Billing from the Organizations management account:
  `docs/evidence/cost-allocation-tag-activation-20260617.md`.
- [x] On 2026-06-14, decide and document SSE-S3 versus SSE-KMS for raw,
  curated, Athena results, logs, and dashboard artifacts:
  `docs/adr/0002-encryption-and-kms-design.md`.
- [x] In the same ADR, design KMS key ownership, key policy, rotation,
  service-role access, migration, rollback, cost, and monitoring controls.
- [x] Record SSE-KMS implementation as conditional on a documented promotion
  trigger and explicit approval; it is not a current closure requirement.
- [x] On 2026-06-15, restrict the Glue role in local Terraform to required
  prefixes and actions; validate the boundary with
  `scripts/check_lakehouse_iam_policies.py`, ADR 0004, and
  `docs/evidence/glue-athena-iam-preflight-20260615.md`.
- [x] On 2026-06-15, add a dedicated Athena query role in local Terraform with
  bounded workgroup, catalog, curated-data, and query-result permissions;
  record the access model in ADR 0004 and the preflight evidence file.
- [x] On 2026-06-15, review and explicitly approve the IAM Terraform plan, apply the Glue and
  Athena policy changes using `docs/glue-athena-iam-deployment-runbook.md`,
  and capture live role/service verification evidence:
  `docs/evidence/glue-athena-iam-live-verification-20260615.md`.
- [x] On 2026-06-15, capture one current raw -> Glue -> curated Parquet ->
  Athena validation evidence chain:
  `docs/evidence/glue-athena-iam-live-verification-20260615.md`.
- [x] On 2026-06-16, reconcile
  `docs/phase-1-stabilize-ingestion-lakehouse.md` against the current
  mixed-energy implementation and close its stale checks.
- [x] On 2026-06-17, mark the lakehouse readiness closure complete after all
  required June-July closure gaps above had evidence links.

### Lakehouse scope boundaries

| Must have before exam | Existing optional baseline | Defer new expansion |
|---|---|---|
| S3 raw/curated | Lake Formation | New UI/dashboard expansion |
| Glue ETL | Existing EventBridge schedules, maintenance only | New AI orchestration expansion |
| Parquet | Step Functions orchestration | Deep REMIT workflow |
| Athena | DynamoDB metadata | Complex API |
| IAM | Basic data quality checks | Multi-region deployment |
| KMS |  |  |
| CloudWatch |  |  |
| CloudTrail/Config design |  |  |
| Cost tags |  |  |

---

## 5. Governance Readiness Status

### Multi-account target

```text
AWS Organization
│
├── Root / Management Account
│   └── management-account-alias / 349687196588
│       ├── AWS Organizations
│       ├── Billing
│       ├── IAM Identity Center
│       └── SCP administration
│
├── Security OU / ou-gbyf-mug20ym0
│   ├── Security Log Archive / 955659429518
│   │   ├── CloudTrail organization-trail archive bucket and KMS key
│   │   ├── AWS Config archive bucket and KMS key
│   │   └── storage-only audit boundary
│   ├── Security Tooling / 668848431187
│   │   ├── AWS Config delegated administrator
│   │   ├── AWS Config aggregator in eu-west-2
│   │   ├── AWS Config recorder in eu-west-2
│   │   ├── Included in organization CloudTrail Config rule
│   │   ├── GuardDuty delegated administrator in eu-west-2
│   │   ├── foundational GuardDuty coverage owner
│   │   └── future Security Hub / OAM home if adopted
│   └── so-aws-admin / 054394900225
│       └── Closed on 2026-07-09 after final pre-close checks returned no blockers
│
├── Lakehouse Workloads OU / ou-gbyf-m6ppfmpq
│   └── lakehouse-workload-account / 464975959576
│       └── Energy Data Lakehouse workload account
│
└── Container Sandbox
    └── Sandbox account / 974893866311
```

### Governance checklist

| Item | Status | Evidence |
|---|---|---|
| AWS Organizations enabled | Verified | Management account and member accounts verified in `docs/evidence/cost-allocation-tag-activation-20260617.md`; prechange root/account/service-access inventory is recorded in `docs/evidence/domain1-governance-org-inventory-summary-20260621.md`; live OU creation and lakehouse account move are recorded in `docs/evidence/domain1-governance-lakehouse-workloads-ou-change-note-20260621.md` and `docs/evidence/domain1-governance-lakehouse-account-move-change-note-20260622.md` |
| OU structure designed | Partial | Target OU model recorded in `docs/adr/0005-aws-organizations-governance-design.md`; current-to-target placement decision recorded in `docs/planning/domain-1-ou-account-placement-decision-20260621.md`; live evidence now shows `Container Sandbox`, `Lakehouse Workloads OU`, and `Security OU` exist under root, and the lakehouse account has been moved into `ou-gbyf-m6ppfmpq`; see `docs/evidence/domain1-governance-security-ou-change-note-20260622.md` and `docs/evidence/domain1-governance-lakehouse-account-move-change-note-20260622.md` |
| Management account rules documented | Verified | Control-plane account rules recorded in `docs/adr/0005-aws-organizations-governance-design.md`; implementation boundary remains future approval |
| Workload account purpose defined | Verified | Lakehouse workload and sandbox account boundaries recorded in `docs/adr/0005-aws-organizations-governance-design.md` |
| Security/log archive account design documented | Verified | Target security/log archive boundary recorded in `docs/adr/0005-aws-organizations-governance-design.md`; detailed design recorded in `docs/planning/domain-1-cloudtrail-log-archive-design-20260621.md`; the first bounded baseline/change note is recorded in `docs/evidence/domain1-governance-cloudtrail-log-archive-change-note-20260622.md`; the design-to-implementation boundary is recorded in `docs/planning/domain-1-security-log-archive-account-implementation-boundary-20260622.md`; `Security OU`, the dedicated `Security Log Archive` account, Account Management trusted access, alternate contacts, the dedicated log-archive bucket, and the customer-managed KMS key are now live via `docs/evidence/domain1-governance-cloudtrail-log-archive-storage-change-note-20260624.md`; the separate `Security Tooling` account now exists in `Security OU` via `docs/evidence/domain1-governance-security-tooling-account-placement-change-note-20260704.md`, its `SECURITY`, `OPERATIONS`, and `BILLING` alternate contacts are configured via `docs/evidence/domain1-governance-security-tooling-alt-contacts-change-note-20260706.md`, and AWS Config delegated administration plus aggregation migrated into it via `docs/evidence/domain1-governance-config-security-tooling-migration-change-note-20260706.md`; long-term design keeps `Security Log Archive` storage-only and places active delegated security tooling in `Security Tooling` |
| Public governance evidence redaction | Verified | The public-repository evidence boundary is recorded in `docs/runbooks/domain-1-governance-live-readiness-runbook.md`; `scripts/check_public_evidence_redaction.sh` is the required pre-staging check for governance evidence; exact AWS contact values, raw contact JSON, and private evidence belong outside this public repository; the known GitHub cache/fork limitation remains documented in the runbook after the 2026-07-07 history-cleanup pass |
| IAM Identity Center access model documented | Partial | Permission-set candidates and account targets recorded in `docs/adr/0005-aws-organizations-governance-design.md`; detailed matrix recorded in `docs/planning/identity-center-permission-set-matrix-20260619.md`; same-day evidence now confirms one active IAM Identity Center instance, the live management-account admin principal `org-admin-principal` / `[redacted-email]`, the dedicated emergency principal `breakglass-principal` / `[redacted-email]`, and current management/sandbox account assignments in `docs/evidence/domain1-governance-identity-center-current-state-20260625.md`; follow-on live cleanup removed inherited `AdministratorAccess` from `breakglass-principal` by removing its `cloud-lab-aws-admins` group membership in `docs/evidence/domain1-governance-breakglass-group-membership-cleanup-20260702.md`; the 2026-07-10 fresh read-only inventory confirms the direct/group normal-management paths and separate direct emergency path in `docs/evidence/domain1-governance-identity-center-assignment-inventory-20260710.md`; the bounded routine read-only path is live through `security-tooling-auditors` and `SecurityAudit` in Security Tooling only; the routine administrator path is now the least-privilege `SecurityToolingAdmin` assignment for `security-tooling-admins` in Security Tooling only; and the emergency path is unchanged. The staged assignment, idempotent GuardDuty write, matching delayed Event History and organization-trail object, and separately approved broad-role removal with fresh-session validation are recorded in `docs/evidence/domain1-governance-identity-center-security-tooling-admin-staged-assignment-change-note-20260712.md`, `docs/evidence/domain1-governance-identity-center-security-tooling-admin-guardduty-write-test-20260712.md`, and `docs/evidence/domain1-governance-identity-center-security-tooling-admin-broad-assignment-removal-change-note-20260712.md`; the broader governance permission-set model remains open |
| Permission sets defined | Partial | Permission-set matrix recorded in `docs/planning/identity-center-permission-set-matrix-20260619.md`; live permission sets now include `AdministratorAccess`, `BreakGlassAdmin`, `SecurityAudit`, and `SecurityToolingAdmin`; the first direct management-account emergency assignment for `breakglass-principal` remains recorded in `docs/evidence/domain1-governance-identity-center-current-state-20260625.md`; the 2026-07-11 `SecurityAudit` permission set has `PT1H`, only the custom no-mutation inline policy, and one Security Tooling group assignment, as recorded in `docs/evidence/domain1-governance-identity-center-security-audit-assignment-change-note-20260711.md`; the custom one-hour `SecurityToolingAdmin` now provides the routine Security Tooling administrator path, with read-only entitlement, successful GuardDuty write/postcondition checks, delayed Event History, organization-trail object evidence, and fresh-session validation recorded in the 2026-07-12 staged-assignment, write-test, and broad-assignment-removal notes; `OrganizationAdmin`, `BillingAdmin`, `LakehouseOperator`, `LakehouseReadOnly`, and later hardening of `BreakGlassAdmin` remain open under the documented assignment gates |
| Break-glass access model documented | Partial | Break-glass target recorded in ADR 0005 and procedure recorded in `docs/runbooks/break-glass-access-procedure.md`; same-day IAM Identity Center evidence in `docs/evidence/domain1-governance-identity-center-current-state-20260625.md` now distinguishes the documented emergency owner from the currently live management-account admin principal and records that the dedicated break-glass principal exists with MFA plus a management-account emergency permission-set assignment; follow-on cleanup evidence in `docs/evidence/domain1-governance-breakglass-group-membership-cleanup-20260702.md` confirms the emergency user now has no group memberships and retains only the direct `BreakGlassAdmin` management-account path; root MFA for workload account `464975959576` is confirmed in `docs/evidence/domain1-governance-root-mfa-readiness-check-20260702.md`; second Identity Center MFA for `breakglass-principal` is confirmed in `docs/evidence/domain1-governance-breakglass-mfa2-readiness-check-20260703.md`; emergency SMS notification reachability is confirmed in `docs/evidence/domain1-governance-notification-reachability-check-20260703.md`; recovery-code readability is confirmed in `docs/evidence/domain1-governance-recovery-code-readability-check-20260703.md`; light procedural validation is confirmed in `docs/evidence/domain1-governance-breakglass-procedural-validation-20260703.md`; the 2026-07-10 decision keeps the direct emergency assignment unchanged pending a separately approved hardening change; post-use review implementation remains open for any actual emergency use |
| SCP catalogue drafted | Partial | Accepted SCP catalogue recorded in ADR 0005; example policy files recorded in `docs/policies/scp/`; the first live OU-targeted `DenyLeavingOrganization` attempt, rollback, root policy-type enablement, and successful retry are recorded in `docs/evidence/domain1-governance-deny-leaving-organization-change-note-20260622.md`, `docs/evidence/domain1-governance-enable-scp-root-change-note-20260622.md`, and `docs/evidence/domain1-governance-deny-leaving-organization-attach-success-change-note-20260622.md`; the second live OU-targeted guardrail, `DenyRootUserActions-LakehouseWorkloads`, is recorded in `docs/evidence/domain1-governance-deny-root-user-actions-attach-success-change-note-20260703.md` |
| CloudTrail organization trail design documented | Verified | Organization trail direction recorded in `docs/adr/0005-aws-organizations-governance-design.md`; detailed log archive/KMS/retention/delete-protection design recorded in `docs/planning/domain-1-cloudtrail-log-archive-design-20260621.md`; earlier baseline evidence is recorded in `docs/evidence/domain1-governance-cloudtrail-log-archive-change-note-20260622.md`; fresh post-account baseline evidence is recorded in `docs/evidence/domain1-governance-cloudtrail-management-sts-prechange-20260624.json`, `docs/evidence/domain1-governance-cloudtrail-service-access-prechange-20260624.json`, `docs/evidence/domain1-governance-cloudtrail-list-prechange-20260624.json`, and the paired security-account prechange files; exact policy examples are recorded in `docs/policies/s3-cloudtrail-log-archive-bucket-policy.example.json`, `docs/policies/kms-cloudtrail-log-archive-key-policy.example.json`, and `docs/policies/s3-cloudtrail-log-archive-encryption.example.json`; live storage evidence is recorded in `docs/evidence/domain1-governance-cloudtrail-log-archive-storage-change-note-20260624.md`; live trusted-access, organization-trail, and first delivered log/digest evidence are now recorded in `docs/evidence/domain1-governance-cloudtrail-organization-trail-change-note-20260624.md` |
| AWS Config design documented | Partial | Organization aggregation direction recorded in `docs/adr/0005-aws-organizations-governance-design.md`; detailed recorder scope, aggregation, rule, and cost-control design recorded in `docs/planning/domain-1-config-guardduty-design-20260621.md`; fresh baseline evidence is recorded in `docs/evidence/domain1-governance-config-*-prechange-20260624.json`, the follow-on lakehouse prechange evidence is recorded in `docs/evidence/domain1-governance-config-lakehouse-*-prechange-20260625.json`, the follow-on security-account prechange evidence is recorded in `docs/evidence/domain1-governance-config-security-*-prechange-20260625.json`, and the follow-on sandbox prechange evidence is recorded in `docs/evidence/domain1-governance-config-sandbox-*-prechange-20260625.json`; exact storage and role-trust policy examples are recorded in `docs/policies/s3-config-log-archive-bucket-policy.example.json`, `docs/policies/kms-config-log-archive-key-policy.example.json`, `docs/policies/s3-config-log-archive-encryption.example.json`, and `docs/policies/iam-config-organization-aggregator-role-trust-policy.example.json`; live storage evidence is recorded in `docs/evidence/domain1-governance-config-log-archive-storage-change-note-20260624.md`; live trusted access, delegated administration, and organization aggregation were first recorded in `docs/evidence/domain1-governance-config-organization-aggregation-change-note-20260624.md`, then migrated from `Security Log Archive` to `Security Tooling` in `docs/evidence/domain1-governance-config-security-tooling-migration-change-note-20260706.md`; the live management-account recorder rollout is recorded in `docs/evidence/domain1-governance-config-management-recorder-change-note-20260624.md`; the live lakehouse-account recorder rollout is recorded in `docs/evidence/domain1-governance-config-lakehouse-recorder-change-note-20260625.md`; the live security-account recorder rollout is recorded in `docs/evidence/domain1-governance-config-security-recorder-change-note-20260625.md`; Security Tooling recorder onboarding and removal of only `668848431187` from the migrated organization CloudTrail Config rule exclusions are recorded in `docs/evidence/domain1-governance-config-security-tooling-recorder-change-note-20260707.md`; `so-aws-admin` was excluded pending its 2026-07-09 closure; additional Config rules remain open |
| GuardDuty/Security Hub/OAM concept documented | Partial | Security-service sequencing recorded in `docs/adr/0005-aws-organizations-governance-design.md`; detailed GuardDuty delegated-admin and cost-control design plus Security Hub defer/adopt decision recorded in `docs/planning/domain-1-config-guardduty-design-20260621.md`; `so-aws-admin` decommission and future Security Hub placement in `Security Tooling` are recorded in `docs/planning/domain-1-so-aws-admin-decommission-decision-20260706.md`; GuardDuty delegated-admin planning with no new account and `Security Tooling` as the target delegated administrator is recorded in `docs/planning/domain-1-guardduty-delegated-admin-planning-20260706.md`; fresh GuardDuty live-readiness evidence is recorded in `docs/evidence/domain1-governance-guardduty-live-readiness-20260707.md`; GuardDuty delegated administration and foundational coverage in `eu-west-2` are live via `docs/evidence/domain1-governance-guardduty-delegated-admin-change-note-20260707.md`; OAM vs CloudTrail log archive vs AWS Config aggregator study note recorded in `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/SAP-C02_Security_Observability_Comparison.md`; Security Hub and OAM enablement remain open |
| Cost allocation tags defined | Verified | Common Terraform tags exist; selected Billing Cost Allocation Tags were activated from the Organizations management account on 2026-06-17 |
| Budget alarms configured | Partial | A live `$1` managed-workflow AWS Budget with notifications is verified; broader workload/account budget design remains open |

### SCP catalogue

| SCP | Purpose | Status |
|---|---|---|
| Deny disabling CloudTrail | Protect audit evidence | Partial |
| Deny deleting log buckets | Protect log archive | Partial |
| Deny public S3 exposure | Reduce data leakage risk | Partial |
| Deny unapproved regions | Cost/compliance control | Partial |
| Deny root-user actions except emergencies | Reduce blast radius | Verified for `Lakehouse Workloads OU` as `DenyRootUserActions-LakehouseWorkloads` / `p-dv2ss5us` |
| Require encryption where feasible | Improve compliance posture | Partial |
| Deny leaving AWS Organization | Prevent governance bypass | Verified for `Lakehouse Workloads OU` |

Critical note: **SCPs do not grant permissions.** They define maximum allowed permissions. IAM policies still grant permissions.

Example SCP files now exist in `docs/policies/scp/`. The first bounded
OU-targeted SCP change note records the initial failed attach attempt and
rollback, the root-level policy-type enablement change note records how the
structural blocker was resolved, and the retry change note records the
successful attach to `Lakehouse Workloads OU`:
`docs/evidence/domain1-governance-deny-leaving-organization-change-note-20260622.md`,
`docs/evidence/domain1-governance-enable-scp-root-change-note-20260622.md`,
and
`docs/evidence/domain1-governance-deny-leaving-organization-attach-success-change-note-20260622.md`.
The next narrow SCP candidate, `deny-root-user-actions.example.json`, is now
recorded as prepared with the target emergency owner, contact, notification,
storage, evidence, scope, and reduction values defined, with the current live
IAM Identity Center principal inventory clarified in
`docs/evidence/domain1-governance-identity-center-current-state-20260625.md`,
and follow-on MFA, SMS reachability, and recovery-code readability evidence
recorded, plus light procedural-validation evidence in
`docs/evidence/domain1-governance-breakglass-procedural-validation-20260703.md`.
The live attachment is recorded in
`docs/evidence/domain1-governance-deny-root-user-actions-attach-success-change-note-20260703.md`.

---

## 6. Networking Weak Areas

### Required comparison matrix

| Topic | Current confidence | Required by exam? | Evidence required |
|---|---:|---|---|
| VPC fundamentals | Medium | Yes | VPC/subnet/route-table source and reading guide are tracked in `diagrams/vpc-subnet-route-table-study.mmd` and `docs/planning/domain-2-vpc-subnet-route-table-diagram-20260715.md`; the source includes a syntax-only Mermaid correction and the rendered SVG is tracked |
| Security groups vs NACLs | Medium: source-backed comparison recorded | Yes | Comparison note in `docs/planning/domain-2-security-groups-network-acls-comparison-20260715.md` |
| VPC peering | Improving: source-backed decision rule recorded | Yes | Use cases, non-transitivity, routing, cost, and scale limits are recorded in `docs/planning/domain-2-network-access-patterns-20260621.md` and the broader Networking revision lesson; no full-domain recall assessment exists |
| Transit Gateway | Improving: source-backed artifact and reviewed decision rule recorded | Yes | Tracked hub-and-spoke source, rendered SVG, and decision guidance exist; Review Cycle 2 corrected the hybrid-transport reasoning, but no full-domain recall assessment exists |
| PrivateLink | Improving: source-backed decision rule recorded | Yes | Comparison with peering and TGW plus service-provider/consumer cues is recorded in `docs/planning/domain-2-privatelink-peering-tgw-decision-20260714.md`; no full-domain recall assessment exists |
| VPC Lattice | Improving: one correct but uncertain Mock 004 recognition result plus expanded source note | Yes | `docs/exam-prep/revision-notes/targeted-lessons/aws-networking-sap-c02-key-lessons-20260717.md` now explains service networks, service and VPC associations, listeners/targets, DNS, IAM auth policies, cross-account sharing, overlapping-CIDR support, and the TGW/PrivateLink boundary; independent transfer evidence remains limited to Mock 004 Question 5 |
| VPC endpoints | High: 8/8 focused blind drill | Yes | Tracked source, rendered SVG, and reading guide exist in `diagrams/vpc-endpoint-study.mmd` and `docs/planning/domain-2-vpc-endpoint-diagram-20260715.md`; blind evidence is in `docs/exam-prep/networking-scenario-drill-blind-attempt-20260715.md` |
| NAT Gateway | High: correct in 8/8 focused blind drill | Yes | Cost and routing note plus blind evidence in `docs/exam-prep/networking-scenario-drill-blind-attempt-20260715.md` |
| Direct Connect | Improving: source-backed artifact, corrected review reasoning, and BGP decision material recorded | Yes | Hybrid transport table and promotion triggers are recorded in `docs/planning/domain-2-direct-connect-vpn-decision-20260714.md`; `docs/exam-prep/revision-notes/targeted-lessons/aws-networking-sap-c02-key-lessons-20260717.md` now covers BGP direction, longest-prefix/local-preference/AS_PATH/MED order, communities, ECMP, VGW/TGW boundaries, prefix limits, and SiteLink; no full-domain recall assessment exists |
| Site-to-Site VPN | Improving: source-backed artifact and corrected review reasoning recorded | Yes | DX-versus-VPN comparison and promotion triggers are recorded in `docs/planning/domain-2-direct-connect-vpn-decision-20260714.md`; Review Cycle 2 final reasoning correctly combines DX with VPN resilience, but no full-domain recall assessment exists |
| Route 53 Resolver | Improving: Review Cycle 1 correct at 4/4 overall after the exercise 002 miss | Yes | Tracked hybrid-DNS source, rendered SVG, reading guide, and consolidation lesson exist |
| Centralized inspection VPC | Improving: source-backed architecture sketch recorded | Yes | The two-pass route-domain source, rendered SVG, and reading guide are tracked in the repository |

### Networking deliverables

| Deliverable | Due | Status |
|---|---|---|
| VPC connectivity comparison matrix | 2026-08-31 | Verified: the decision matrix, source-backed review, Mermaid sources, and rendered SVGs are tracked; the VPC/subnet source includes a syntax-only Mermaid correction; no AWS implementation is required |
| Transit Gateway hub-and-spoke diagram | 2026-09-07 | Verified: the tracked conceptual Mermaid source and rendered SVG passed the source-backed review; no AWS implementation is required |
| PrivateLink vs peering vs TGW decision table | 2026-09-07 | Verified: decision table and Lakehouse promotion triggers in `docs/planning/domain-2-privatelink-peering-tgw-decision-20260714.md` passed the source-backed scenario review |
| Direct Connect vs VPN decision table | 2026-09-14 | Verified: hybrid transport decision table and Lakehouse promotion triggers in `docs/planning/domain-2-direct-connect-vpn-decision-20260714.md` passed the source-backed scenario review |
| Route 53 Resolver hybrid DNS diagram | 2026-09-14 | Verified: the tracked directional hybrid-DNS source, rendered SVG, and reading guide passed the source-backed review |
| NAT Gateway cost warning note | 2026-09-14 | Verified: endpoint-first cost hierarchy and evidence gates in `docs/planning/domain-2-nat-gateway-cost-warning-20260715.md` passed the source-backed scenario review |
| Centralized inspection VPC architecture sketch | 2026-09-14 | Verified as a documentation-only artifact: the tracked reading guide, Mermaid source, and rendered SVG record the two-pass route domains, appliance-mode symmetry, Availability Zone, bypass-prevention, security, logging, and cost checks; no AWS implementation is required |

The source-backed review is recorded in
`docs/exam-prep/networking-scenario-drill-review-20260715.md`. The separate
focused blind attempt in
`docs/exam-prep/networking-scenario-drill-blind-attempt-20260715.md` was
explicitly submitted and scored 8/8 on 2026-07-15. It supports the VPC endpoint
and NAT Gateway confidence updates above, but it is untimed and does not count
as a full Networking-domain assessment, wrong-answer review cycle, or booking
evidence.

The revision hub in `docs/exam-prep/README.md` organizes the paired lessons in
`docs/exam-prep/revision-notes/targeted-lessons/route-53-sap-c02-key-lessons-20260715.md` and
`docs/exam-prep/revision-notes/targeted-lessons/aws-networking-sap-c02-key-lessons-20260717.md` and labels every
exam-prep artifact as lesson, blind attempt, answer-bearing review, or evidence
log/manifest. It provides time-boxed and scenario-indexed review paths across
Route 53 and the Networking areas not taught in depth by the existing folder
artifacts: IPv6/NAT64, Direct Connect virtual interfaces and gateway roles,
ALB/NLB/GWLB selection, global ingress and WAN choices, inspection-control
boundaries, and network troubleshooting tools. This is study evidence, not
additional learner-recall, timed-practice, or booking evidence.

---

## 7. Resilience, DR, and Migration Weak Areas

### Resilience and DR services and concepts

| Service / concept | Current confidence | Evidence required |
|---|---:|---|
| RTO and RPO | Retained in fresh spaced evidence | Fresh question-only retest completed 2026-07-27 at 12/12 in 16 minutes; objective measurement correct under exact-match transfer testing |
| Backup and restore / pilot light / warm standby / active-active | Retained in fresh spaced evidence | Fresh question-only retest completed 2026-07-27 at 12/12; warm-standby and active/active capacity decisions correct |
| AWS Backup | Retained in fresh spaced evidence | Cross-account/cross-Region and restore-validation decisions correct in the 2026-07-27 isolated retest; no live Lakehouse backup/restore evidence |
| AWS Elastic Disaster Recovery | Retained in fresh spaced evidence | Drill-versus-traffic-failover boundary correct in the 2026-07-27 isolated retest; no live DRS implementation |
| S3 versioning, backup, and cross-Region replication | Retained in fresh spaced evidence | MRAP routing versus two-way replication and corruption-recovery boundaries correct in the 2026-07-27 retest; no Lakehouse copy or restore implementation is proved |
| Multi-AZ versus multi-Region recovery | Retained in fresh spaced evidence | Regional dependency and active-capacity decisions correct in the 2026-07-27 retest; continue transfer monitoring in full mocks |

### Resilience and DR deliverables

| Deliverable | Due | Status |
|---|---|---|
| Bounded resilience/DR transition plan | 2026-07-20 | Complete repository artifact: `docs/planning/domain-2-resilience-dr-study-plan-20260718.md` records scope, evidence boundary, ordered artifacts, and quality gates; no AWS change |
| DR pattern matrix | 2026-07-20 | Complete source-backed repository artifact: `docs/exam-prep/revision-notes/targeted-lessons/aws-resilience-dr-sap-c02-key-lessons-20260718.md` compares backup/restore, pilot light, warm standby, and multi-site active/active and separates HA, backup, replication, and DR |
| RTO/RPO decision table | 2026-07-20 | Complete repository artifact: `docs/planning/domain-2-rto-rpo-decision-table-20260718.md` separates business objectives, designed capability, and tested results; records dependency and failure-scope constraints; and leaves all Lakehouse objectives unset pending an accountable owner |
| Lakehouse recovery mapping | 2026-07-20 | Complete repository artifact: `docs/planning/domain-2-lakehouse-recovery-mapping-20260719.md` maps S3, Glue, Athena, IAM, infrastructure definitions, operational dependencies, failure-scope limits, candidate validation, and promotion triggers without setting RTO/RPO or claiming tested recovery |
| Source-backed resilience/DR scenario review | 2026-07-20 | Complete repository artifact: `docs/exam-prep/resilience-dr-scenario-drill-review-20260719.md` applies current official AWS guidance across 12 answer-bearing scenarios; the later learner submission is recorded separately |
| Resilience/DR recall submission | 2026-07-20 | Submitted at 12/12 in `docs/exam-prep/resilience-dr-scenario-drill-submission-20260720.md`; the learner explicitly attests to a first attempt without viewing the key, but the source file was answer-bearing, no duration was supplied, and structural isolation is not claimed |
| Fresh question-only Resilience/DR spaced retest | 2026-07-27 | Complete: frozen 12-question attempt scored 12/12 in 16 minutes, including 7/7 exact-match multiple-response; source-verified assessment in `docs/exam-prep/sap-c02-resilience-dr-spaced-retest-review-20260727.md` |

The documentation transition into resilience/DR has occurred, and the DR
pattern matrix, RTO/RPO decision table, Lakehouse recovery mapping, and
source-backed scenario review are complete repository artifacts. The learner's explicit
12/12 focused submission is also recorded, with its untimed and
answer-bearing-source isolation caveats. The next Resilience/DR evidence gate
was completed on 2026-07-27 at 12/12 in 16 minutes using the structurally
isolated question-only artifact. Questions 2 and 4 were marked uncertain and
both were correct. This closes the focused spaced-recall gate but does not
prove a live backup, restore, replication, failover, or failback path. No AWS
Backup, DRS, S3 replication, or multi-Region resource is authorized by these
artifacts.

### Migration services

### Required services

| Service / concept | Current confidence | Evidence required |
|---|---:|---|
| 7 Rs migration strategy | Medium | Decision table; complete with Relocate |
| AWS Application Migration Service | Source note expanded; transfer untested | `docs/exam-prep/revision-notes/targeted-lessons/aws-migration-discovery-transfer-tracking-sap-c02-key-lessons-20260728.md` now introduces AWS MGN, its staging architecture, test-to-cutover lifecycle, finalization boundary, and MGN-versus-DMS/DataSync/DRS/Transform selection; validate in independent mocks |
| Application Discovery Service Agent versus Agentless Collector | Source note expanded; transfer untested | `docs/exam-prep/revision-notes/targeted-lessons/aws-migration-discovery-transfer-tracking-sap-c02-key-lessons-20260728.md` distinguishes Agent, VMware inventory module, and WinRM/SNMP-based Network Data Collection module; validate in full mocks |
| AWS Database Migration Service | Medium | Homogeneous vs heterogeneous examples |
| AWS DataSync | Medium: correct in the 2026-07-21 mixed diagnostic; source note expanded | Retain agent/location/task/execution, incremental copy, verification, Basic/Enhanced mode, and service-boundary decisions; validate in full mocks |
| Snow Family | Low | Offline transfer decision note |
| Storage Gateway | Low | Hybrid storage use-case note |
| Migration Hub | Source note expanded; transfer untested | Retain home-Region, application grouping, tool authorization, import, and tracking-not-engine boundaries; validate in full mocks |
| AWS Transform | Source note expanded; transfer untested | Retain migrations-versus-MGN, mainframe, .NET, human-review, and specialist-service boundaries in `docs/exam-prep/revision-notes/targeted-lessons/aws-migration-discovery-transfer-tracking-sap-c02-key-lessons-20260728.md`; validate in independent mocks |
| RDS/Aurora migration paths | Medium | DMS/RDS/Aurora comparison |

### Migration deliverables

| Deliverable | Due | Status |
|---|---|---|
| 7 Rs migration strategy matrix | 2026-08-03 | Complete: `docs/planning/domain-4-migration-decision-matrix-20260823.md`; includes Relocate |
| Discovery, transfer, and tracking key lesson | 2026-07-28 | Complete source-backed artifact: `docs/exam-prep/revision-notes/targeted-lessons/aws-migration-discovery-transfer-tracking-sap-c02-key-lessons-20260728.md`; no blind-recall score or live migration evidence |
| Data migration service comparison | 2026-08-03 | In progress: Application Discovery Service, MGN, DataSync, Migration Hub, and bounded AWS Transform service-selection material are complete; DMS/SCT, Snow Family, Storage Gateway, and transfer-method consolidation remain |
| Database migration decision table | 2026-08-10 | Not started |

### External-assessment correction - 2026-07-28

Six Gemini Flash assessment extracts were reviewed against current AWS
documentation in
`docs/exam-prep/sap-c02-gemini-flash-weak-area-review-20260728.md`. No total
score, timing, or complete question set was supplied, so this is remediation
evidence rather than a practice result.

- The Route 53 Region-SCP exemption and Direct Connect longest-prefix answers
  were sound, although the SCP explanation was corrected: global services are
  not automatically ignored by `aws:RequestedRegion`.
- The ARC answer identified the routing-control mechanism but falsely claimed
  that it bypasses DNS caching; the Route 53 notes now retain TTL, resolver,
  client, and connection-reuse effects.
- The DynamoDB `ValidationException` answer was rejected as obsolete/incorrect:
  MREC accepts a strong local table read but does not guarantee freshness from
  another Region, while MRSC provides cross-Region strong consistency.
- The `io2` Block Express answer is conditional on the required sustained
  performance, latency, or durability; `gp3` remains the cost-aware repair when
  its provisioned envelope is sufficient.
- The physical-server dependency-discovery miss is genuine. Application
  Discovery Agent is the safe answer for host-level process/TCP evidence, while
  the current Agentless Collector's VMware network module prevents the broader
  claim that agentless discovery can never map dependencies.

These corrections did not insert an extra test before the database retest,
which later passed 6/6 on 2026-07-28. Full mock 002 subsequently completed at
71/75; its four new misses now control the bounded remediation before full mock
003.

---

## 8. Practice Question Scores

### Rule

Continue targeted remediation while completing two full-length simulated exams
per week. Full mocks 001 and 002 establish the initial baseline. Complete at
least seven further mocks—Mocks 003–009—before making any booking decision. Keep
answer keys unavailable until submission, grade multiple-response questions by
exact match, and preserve the result and remediation evidence from every
attempt for the post-mock-009 decision point and, if needed, the formal
2026-09-07 backstop review.

The seven-further-mock minimum is the learner's chosen validation programme and
the current cadence commitment. Mock 008 was added after the Skill Builder
assessment; Mock 009 was added on 2026-08-14 so Mocks 008 and 009 can preserve
the two-full-mock cadence in the week beginning 2026-08-17 before the go/no-go
review. More mocks may be completed if the evidence is not yet decisive. The minimum quantitative
booking rule in Sections 1 and 10
is a necessary gate, not a substitute for this higher-confidence validation
series. Mock count alone is not readiness evidence: score stability, domain
floors, multiple-response performance, explanation quality, and non-recurrence
of unresolved errors remain controlling.

| Date | Source | Mode | Score | Domain weakness | Action |
|---|---|---|---:|---|---|
| 2026-06-19 | `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/exercises/exercise-001.md` | Untimed 20 questions | 20/20 | None identified | User-confirmed 20/20; no wrong-answer logging required |
| 2026-06-19 | `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/exercises/sap-c02-exercise-002-marking-and-revision-log.md` | Untimed 20 questions | 18/20 | Hybrid DNS; migration strategy selection | Wrong answers logged; drill Route 53 Resolver and rehost/MGN scenario wording |
| 2026-07-01 | `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/exercises/sap-c02-exercise-block-003-review.md` | Untimed 25 questions | 24/25 | Hybrid network architecture multi-select discipline | Wrong answer logged; recheck every selected service against a stated requirement |
| 2026-07-01 | `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/exercises/sap-c02-exercise-block-004-review.md` | Untimed 25 questions | 25/25 | None identified | Clean pass; keep no-heading exam-style blocks |
| 2026-07-01 | `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/exercises/sap-c02-exercise-block-005-review.md` | Untimed 25 questions | 25/25 | None identified | Clean pass; no-heading format improved scenario parsing |
| 2026-07-01 | `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/exercises/sap-c02-exercise-block-006-review.md` | Untimed 25 questions | 24/25 | Kinesis Data Streams vs SQS FIFO; block quality caveat | Wrong answer logged; drill event-streaming vs queueing decision patterns; treat the score as useful learning evidence but not fully exam-quality because the block had a flawed single-answer distribution |
| 2026-07-07 | `docs/exam-prep/wrong-answers.md` | Untimed 25 questions | 25/25 | None identified | Block 007 clean pass; stronger coverage of Domain 2 and Domain 3 topics |
| 2026-07-15 | `docs/exam-prep/networking-scenario-drill-blind-attempt-20260715.md` | Untimed blind 8 questions | 8/8 | None in focused VPC endpoint/NAT scope | Clean pass; no wrong-answer entry required; narrow recall evidence only, not a timed exam or full Networking-domain assessment |
| 2026-07-15 | `docs/exam-prep/wrong-answers.md` | Untimed blind Review Cycle 1 | 4/4 | Four prior weak areas recalled correctly | First recall cycle complete; retain per-partition-key precision for Kinesis; second review cycle and timed evidence remain open |
| 2026-07-18 | `docs/exam-prep/sap-c02-mixed-practice-block-2-submission-20260718.md` | Mixed practice Block 2; timing not supplied | 25/25 | None reported | Explicitly submitted clean pass: 15/15 single-choice and 10/10 multi-select; question text and answer key are unavailable for independent re-marking; separate from Review Cycle 2 and not timed/full-simulation evidence |
| 2026-07-18 | `docs/exam-prep/wrong-answer-review-cycle-2-blind-attempt-20260715.md` | Untimed Review Cycle 2; corrected final submission | 4/4 final | Initial drafts had material precision gaps in service naming, hybrid transport, and Kinesis/SQS reasoning | Review-and-correction cycle complete; final answers correct, but not an unchanged clean blind pass; no new wrong-answer theme or timed evidence |
| 2026-07-20 | `docs/exam-prep/resilience-dr-scenario-drill-submission-20260720.md` | Focused Resilience/DR recall; timing not supplied | 12/12 | None in the tested scenario scope | Explicit learner submission; learner attests first attempt without viewing the key, but the source was answer-bearing rather than structurally isolated; the later question-only spaced retest was completed 12/12 on 2026-07-27 |
| 2026-07-21 | `docs/exam-prep/sap-c02-mixed-diagnostic-30q-20260720.md` | Timed 30-question mixed diagnostic; 67 of 72 minutes | 29/30 | ECS blue/green deployment and rollback versus Systems Manager Patch Manager | Explicit submission: 17/18 single-choice and 12/12 multiple-response; answer-bearing assessment in `docs/exam-prep/sap-c02-mixed-diagnostic-30q-review-20260721.md`; new miss logged for spaced remediation; not a full 75-question simulation |
| 2026-07-23 | User-supplied simulated-certification result | Full timed 75-question mock; 133 of 180 minutes | 73/75 (97.3%) | SCP versus IAM permissions boundary; DynamoDB write sharding and read fan-out | Full mock 001: 48/48 single choice, 25/27 multiple response, 47 minutes remaining; record both exact-match traps, complete targeted remediation, and validate performance through the longitudinal series, now extended through full mock 009 before a booking decision |
| 2026-07-24 | `docs/exam-prep/sap-c02-scp-permissions-boundary-closed-book-retest-6q-20260724.md` | Focused exact-match retest; 18 minutes | 6/6 (100%) | None in the focused scope | Original SCP/boundary trap did not recur and all three uncertain responses were correct; attempted before the preferred 2026-07-26 spacing date, so retain as immediate-remediation evidence and monitor transfer in independent mocks |
| 2026-07-25 | `docs/exam-prep/sap-c02-non-relational-databases-diagnostic-18q-20260724.md` | Closed-book focused diagnostic; 40 minutes | 15/18 (83.3%) | Random write-shard read ordering and GSI consistency; DynamoDB Streams versus GSI lifecycle mapping; Timestream history plus DynamoDB latest-state decomposition | Frozen submission independently scored by exact match: 9/9 single-choice and 6/9 multiple-response; review the three demonstrated gaps and complete the six-question spaced retest no earlier than 2026-07-28 before treating the topic as closed |
| 2026-07-27 | `docs/exam-prep/sap-c02-resilience-dr-spaced-retest-12q-20260726.md` | Fresh structurally isolated Resilience/DR spaced retest; 16 minutes | 12/12 (100%) | None in the tested scope | Frozen submission independently scored: 5/5 single-response and 7/7 exact-match multiple-response; Questions 2 and 4 were uncertain and both correct; focused DR gate complete, continue independent-mock transfer monitoring |
| 2026-07-28 | `docs/exam-prep/sap-c02-non-relational-databases-spaced-retest-6q-20260728.md` | Fresh spaced non-relational database retest; 15 minutes | 6/6 (100%) | None in the tested scope | Frozen submission independently scored: 1/1 single-response and 5/5 exact-match multiple-response; Questions 1 and 2 were uncertain and both correct; focused database remediation gate complete, continue transfer monitoring in full mock 002 |
| 2026-07-29 | `docs/exam-prep/sap-c02-full-mock-002-75q-20260728.md` | Full timed 75-question mock; 139 of 180 minutes | 71/75 (94.7%) | IAM Identity Center versus Cognito; Migration Hub home Region; DAX cluster plus client; S3 interface versus gateway endpoint for on-premises access | Frozen submission independently scored: 45/48 single-response and 26/27 exact-match multiple-response; all four domain floors exceeded 93%; minimum two-qualifying-mock score gate met, but continue the evidence-led repeated-mock programme and bounded remediation |
| 2026-08-01 | `docs/exam-prep/sap-c02-full-mock-002-spaced-retest-8q-20260801.md` | Fresh closed-book close-distractor retest; 17 minutes | 8/8 (100%) | None in the focused four-gap scope | Frozen submission independently scored: 4/4 single-response and 4/4 exact-match multiple-response; Questions 4 and 7 were uncertain and correct; focused Mock 002 remediation complete, continue transfer monitoring in Mock 003 and later independent mocks |
| 2026-08-03 | `docs/exam-prep/sap-c02-full-mock-003-75q-20260801.md` | Full timed 75-question mock; 106 of 180 minutes | 75/75 (100%) | None identified | Frozen submission independently scored: 48/48 single-response and 27/27 exact-match multiple-response; all 12 uncertain answers and all four Mock 002 transfer checks were correct; no immediate retest, continue to Full Mock 004 |
| 2026-08-05 | `docs/exam-prep/sap-c02-full-mock-004-75q-20260804.md` | Full timed 75-question mock; 113 of 180 minutes | 70/75 (93.3%) | Logically air-gapped backup isolation and restore evidence; CloudFront origin-failover methods; ECS blue/green exact-match completeness; DynamoDB MRSC TTL restriction | Frozen submission independently scored: 45/48 single-response, 25/27 exact-match multiple-response, 13/15 uncertain answers, and every domain above 77%; Questions 13 and 75 share one backup-isolation theme; ECS blue/green recurred as an incomplete multi-response answer; complete a short spaced retest no earlier than 2026-08-07 without replacing Mock 005 |
| 2026-08-07 | `docs/exam-prep/sap-c02-full-mock-004-spaced-retest-8q-20260807.md` | Fresh closed-book close-distractor retest; 22 minutes | 7/8 (87.5%) | Lambda@Edge versus separate write-continuity architecture | Frozen submission independently scored: 4/4 single-response, 3/4 exact-match multiple-response, and both uncertain answers correct; backup, ECS blue/green, and DynamoDB focused remediation passed; Question 4 selected `AD` rather than `AC`, so use Full Mock 005 or a later independent mock as the CloudFront/write-continuity transfer check |
| 2026-08-07 | `docs/exam-prep/sap-c02-full-mock-005-75q-20260807.md` | Full timed 75-question mock; 108 of 180 minutes | 73/75 (97.3%) | ARC single-response over-selection; Transfer Family AS2 versus Amazon MQ | Frozen submission independently scored: 46/48 single-response, 27/27 exact-match multiple-response, 14/16 uncertain answers, Domains 1 and 2 at 100%, Domain 3 at 94.4%, and Domain 4 at 93.3%; the prior Lambda@Edge/write-continuity gap transferred successfully |

| 2026-08-09 | `docs/exam-prep/aws-skill-builder-sap-c02-assessment-review-20260809.md` and `docs/exam-prep/aws-skill-builder-sap-c02-answer-difference-audit-20260809.md` | AWS Skill Builder official-practice attempt 2; paused/review-style duration of 12h29 | Scaled 775; pass threshold 750; 45/75 keyed correct | Broad service-boundary calibration; 11 confident misses; dated Object Lock and KMS keys isolated | The complete local workbook was inspected but is Git-ignored because it contains the proprietary full assessment and learner-identifying display text. All 30 selection/key differences are captured in the publishable audit: 28 keys stand, Question 7 is learner-correct under current behaviour, and Question 10 has a dated key but the learner answer remains wrong. No domain-score values were exported; this is excluded from timed/full-mock and booking-gate evidence. Full Mock 006 later passed several transfer checks but repeated three service-composition boundaries, which remain in focused remediation |
| 2026-08-12 | `docs/exam-prep/sap-c02-domain-3-mental-model-diagnostic-25q-20260812.md` and `docs/exam-prep/sap-c02-domain-3-mental-model-diagnostic-review-20260812.md` | Timed focused 25-question diagnostic; 47 of 60 minutes | 25/25 (100%) | None demonstrated: Domain 3 20/20, cross-domain controls 5/5, and all five Domain 3 task areas 4/4 | Frozen submission independently scored: 16/16 single-response and 9/9 exact-match multiple-response, with no uncertainty recorded. No focused retest is required; this is not a full simulation or booking-gate result. Domain 3 later transferred at 18/18 in Full Mock 006 |
| 2026-08-12 | `docs/exam-prep/sap-c02-full-mock-006-75q-20260812.md` and `docs/exam-prep/sap-c02-full-mock-006-review-20260812.md` | Full 75-question mock; 190 wall-clock minutes with approximately 10 minutes of learner-reported short interruptions near the end; estimated active time approximately 180 minutes | 71/75 (94.7%) | OAC plus dynamic origin selection; Regional EFS versus periodic EBS copies; Batch EC2 custom AMI plus Spot; complete warm-standby routing | Frozen submission independently scored: 47/48 single-response, 24/27 exact-match multiple-response, and 11/11 uncertain answers. Primary-domain mapping: Domain 1 20/20, Domain 2 18/22, Domain 3 18/18, Domain 4 15/15. All four misses were confident; three recur from Skill Builder service-boundary calibration, so complete focused close-distractor remediation no earlier than 2026-08-15, then continue to Full Mock 007 |
| 2026-08-15 | `docs/exam-prep/sap-c02-full-mock-007-75q-20260815.md` and `docs/exam-prep/sap-c02-full-mock-007-review-20260815.md` | Full 75-question mock; 142 of 180 minutes; no interruption recorded | 75/75 (100%) | None identified | Submitted response set independently scored: 48/48 single-response, 27/27 exact-match multiple-response, 7/7 uncertain, and every domain at 100%. OAC plus Lambda@Edge, Regional EFS, Batch EC2 custom AMI plus Spot, and complete warm-standby orchestration all transferred from Mock 006; no new wrong-answer entry or focused retest is required |
| 2026-08-18 | `docs/exam-prep/sap-c02-full-mock-008-75q-20260817.md` and `docs/exam-prep/sap-c02-full-mock-008-review-20260818.md` | Full 75-question mock; 139 wall-clock minutes with an explicit 22-minute pause; 117 active minutes and 63 active minutes remaining | 75/75 (100%) | None identified | Frozen response set independently scored: 48/48 single-response, 27/27 exact-match multiple-response, 8/8 uncertain, and every domain at 100%. Prior organization-control, DynamoDB, CloudFront/write-continuity, Batch, warm-standby, AS2, and migration service-selection boundaries all held; no new wrong-answer entry or focused retest is required |
| 2026-08-22 | `docs/exam-prep/sap-c02-full-mock-009-75q-20260820.md` and `docs/exam-prep/sap-c02-full-mock-009-review-20260822.md` | Full 75-question mock; 106 wall-clock minutes with an explicit five-minute pause; 101 active minutes and 79 active minutes remaining | 73/75 (97.3%) | PrivateLink NLB versus GWLB endpoint role; S3 Inventory plus Batch Operations Copy for bulk re-encryption | Frozen response set independently scored: 47/48 single-response, 26/27 exact-match multiple-response, 15/16 uncertain, and primary-domain scores of 20/20, 21/22, 17/18, and 15/15. The two-mock weekly cadence and learner-selected evidence gate are complete; perform the explicit booking review next without treating this row as the decision |

Artifact manifest for completed exercise archive Blocks 003 through 006:
`docs/exam-prep/artifacts/sap-c02-completed-exercises-003-to-006-manifest.md`.

### Full mock 001 - 2026-07-23

| Field | Result |
|---|---|
| Exam | SAP-C02 full-length simulated certification examination |
| Questions | 75 |
| Start / end | 12:00 PM / 2:13 PM |
| Time allowed / used / remaining | 3 hours / 2 hours 13 minutes / 47 minutes |
| Correct / incorrect / unanswered | 73 / 2 / 0 |
| Overall score | 97.3% |
| Average time per question | Approximately 1 minute 46 seconds |

| Question type | Correct | Total | Score |
|---|---:|---:|---:|
| Single choice | 48 | 48 | 100% |
| Multiple response | 25 | 27 | 92.6% |
| Overall | 73 | 75 | 97.3% |

| Exam segment | Correct | Total | Score |
|---|---:|---:|---:|
| Questions 1-25 | 23 | 25 | 92% |
| Questions 26-50 | 25 | 25 | 100% |
| Questions 51-75 | 25 | 25 | 100% |

| SAP-C02 domain | Correct | Total | Score |
|---|---:|---:|---:|
| Design Solutions for Organizational Complexity | 19 | 20 | 95.0% |
| Design for New Solutions | 22 | 22 | 100% |
| Continuous Improvement for Existing Solutions | 17 | 18 | 94.4% |
| Accelerate Workload Migration and Modernization | 15 | 15 | 100% |
| **Overall** | **73** | **75** | **97.3%** |

Endurance and pacing were strong: both misses occurred within the first 13
questions, the final 62 answers were correct, and the attempt ended with 47
minutes available for review. Both misses were exact-match multiple-response
failures and are classified as narrow conceptual traps rather than broad domain
failures. The supplied score and breakdown are user-reported; the full question
set and answer key are not stored in this repository for independent re-marking.

### Full mock 002 - 2026-07-29

| Field | Result |
|---|---|
| Exam | SAP-C02 full-length simulated certification examination |
| Source | `docs/exam-prep/sap-c02-full-mock-002-75q-20260728.md` |
| Questions | 75 |
| Start / end | 12:28 / 14:47 |
| Time allowed / used / remaining | 180 / 139 / 41 minutes |
| Correct / incorrect / unanswered | 70 / 5 / 0 |
| Overall score | 93.3% |
| Average time per question | Approximately 1 minute 51 seconds |

| Question type | Correct | Total | Score |
|---|---:|---:|---:|
| Single response | 45 | 48 | 93.8% |
| Multiple response, exact match | 26 | 27 | 96.3% |
| Overall | 71 | 75 | 94.7% |

| Exam segment | Correct | Total | Score |
|---|---:|---:|---:|
| Questions 1-25 | 24 | 25 | 96% |
| Questions 26-50 | 23 | 25 | 92% |
| Questions 51-75 | 24 | 25 | 96% |

| SAP-C02 domain | Correct | Total | Score |
|---|---:|---:|---:|
| Design Solutions for Organizational Complexity | 18 | 19 | 94.7% |
| Design for New Solutions | 21 | 22 | 95.5% |
| Continuous Improvement for Existing Solutions | 18 | 19 | 94.7% |
| Accelerate Workload Migration and Modernization | 14 | 15 | 93.3% |
| **Overall** | **71** | **75** | **94.7%** |

The learner marked Questions 1, 10, 16, 28, 33, 44, 62, 65, 66, and 67
uncertain; eight of those ten answers were correct. The four misses were
Questions 1, 28, 47, and 73. The answer-bearing independent assessment is
`docs/exam-prep/sap-c02-full-mock-002-review-20260729.md`.

The fresh close-distractor retest was frozen on 2026-08-01 and independently
scored 8/8 in 17 minutes: 4/4 single-response and 4/4 exact-match
multiple-response. Questions 4 and 7 were marked uncertain and both were
correct. The focused remediation gate is complete; the answer-bearing
assessment is
`docs/exam-prep/sap-c02-full-mock-002-spaced-retest-review-20260801.md`.
This narrow result does not replace independent transfer evidence from Mock 003
and later full mocks.

This result confirms high score stability, strong pacing, every domain floor
above 93%, and multiple-response performance above the tracker's threshold.
The minimum two-qualifying-mock score gate is met. The chosen requirement to
complete at least seven further mocks—Mocks 003–009—remains the controlling
evidence path for a high-confidence recommendation, so this state change does
not authorize booking by itself.

### Full mock 003 - 2026-08-03

| Field | Result |
|---|---|
| Exam | SAP-C02 full-length simulated certification examination |
| Source | `docs/exam-prep/sap-c02-full-mock-003-75q-20260801.md` |
| Questions | 75 |
| Start / end | 23:04 / 00:50, crossing midnight |
| Time allowed / used / remaining | 180 / 106 / 74 minutes |
| Correct / incorrect / unanswered | 75 / 0 / 0 |
| Overall score | 100% |
| Average time per question | Approximately 1 minute 25 seconds |

| Question type | Correct | Total | Score |
|---|---:|---:|---:|
| Single response | 48 | 48 | 100% |
| Multiple response, exact match | 27 | 27 | 100% |
| **Overall** | **75** | **75** | **100%** |

| Exam segment | Correct | Total | Score |
|---|---:|---:|---:|
| Questions 1-25 | 25 | 25 | 100% |
| Questions 26-50 | 25 | 25 | 100% |
| Questions 51-75 | 25 | 25 | 100% |

| SAP-C02 domain | Correct | Total | Score |
|---|---:|---:|---:|
| Design Solutions for Organizational Complexity | 20 | 20 | 100% |
| Design for New Solutions | 22 | 22 | 100% |
| Continuous Improvement for Existing Solutions | 18 | 18 | 100% |
| Accelerate Workload Migration and Modernization | 15 | 15 | 100% |
| **Overall** | **75** | **75** | **100%** |

The learner marked Questions 8, 9, 11, 16, 20, 21, 26, 28, 34, 48, 61,
and 67 uncertain; all 12 were correct. Full Mock 003 also retested all four
Full Mock 002 themes through different scenarios: workforce permission sets,
migration discovery and tracking boundaries, DAX client integration, and S3
gateway-versus-interface endpoint selection. All held. The answer-bearing
assessment is
`docs/exam-prep/sap-c02-full-mock-003-review-20260803.md`.

This clean result strengthened longitudinal evidence but did not complete the
learner's chosen validation programme. Full Mock 004 now supplies the next
paired result and is recorded below.

### Full mock 004 - 2026-08-05

| Field | Result |
|---|---|
| Exam | SAP-C02 full-length simulated certification examination |
| Source | `docs/exam-prep/sap-c02-full-mock-004-75q-20260804.md` |
| Questions | 75 |
| Start / end | 21:25 / 23:18 |
| Time allowed / used / remaining | 180 / 113 / 67 minutes |
| Correct / incorrect / unanswered | 70 / 5 / 0 |
| Overall score | 93.3% |
| Average time per question | Approximately 1 minute 30 seconds |

| Question type | Correct | Total | Score |
|---|---:|---:|---:|
| Single response | 45 | 48 | 93.8% |
| Multiple response, exact match | 25 | 27 | 92.6% |
| **Overall** | **70** | **75** | **93.3%** |

| Exam segment | Correct | Total | Score |
|---|---:|---:|---:|
| Questions 1-25 | 23 | 25 | 92% |
| Questions 26-50 | 23 | 25 | 92% |
| Questions 51-75 | 24 | 25 | 96% |

| SAP-C02 domain | Correct | Total | Score |
|---|---:|---:|---:|
| Design Solutions for Organizational Complexity | 20 | 20 | 100% |
| Design for New Solutions | 21 | 22 | 95.5% |
| Continuous Improvement for Existing Solutions | 14 | 18 | 77.8% |
| Accelerate Workload Migration and Modernization | 15 | 15 | 100% |
| **Overall** | **70** | **75** | **93.3%** |

The learner marked Questions 3, 4, 5, 7, 14, 18, 23, 35, 38, 42, 43, 47,
52, 58, and 65 uncertain; 13 of 15 were correct. The misses were Questions 13,
14, 31, 47, and 75. Questions 13 and 75 share the same backup-isolation model,
so the remediation scope is four themes rather than five unrelated gaps.

All four Mock 002 themes transferred again. MGN test/cutover and BGP
longest-prefix/local-preference decisions also held after their source-note
expansion. The answer-bearing assessment is
`docs/exam-prep/sap-c02-full-mock-004-review-20260805.md`.

This result preserves the high-scoring trend and every domain floor remains
above target, but the older ECS blue/green gap recurred as an incomplete
Choose TWO response. The fresh eight-question exact-match retest was frozen on
2026-08-07 and scored 7/8 in 22 minutes. Backup, ECS blue/green, and DynamoDB
passed focused remediation. CloudFront method recall held, but Question 4
showed a genuine misconception that Lambda@Edge could expand built-in origin
failover to write methods. The corrected rule is recorded in
`docs/exam-prep/sap-c02-full-mock-004-spaced-retest-review-20260807.md`;
continue to Full Mock 005 without reducing the two-mock cadence.

### Full mock 005 - 2026-08-07

| Field | Result |
|---|---|
| Exam | SAP-C02 full-length simulated certification examination |
| Source | `docs/exam-prep/sap-c02-full-mock-005-75q-20260807.md` |
| Questions | 75 |
| Start / end | 21:28 / 23:16 |
| Time allowed / used / remaining | 180 / 108 / 72 minutes |
| Correct / incorrect / unanswered | 73 / 2 / 0 |
| Overall score | 97.3% |
| Average time per question | Approximately 1 minute 26 seconds |

| Question type | Correct | Total | Score |
|---|---:|---:|---:|
| Single response | 46 | 48 | 95.8% |
| Multiple response, exact match | 27 | 27 | 100% |
| **Overall** | **73** | **75** | **97.3%** |

| SAP-C02 domain | Correct | Total | Score |
|---|---:|---:|---:|
| Design Solutions for Organizational Complexity | 20 | 20 | 100% |
| Design for New Solutions | 22 | 22 | 100% |
| Continuous Improvement for Existing Solutions | 17 | 18 | 94.4% |
| Accelerate Workload Migration and Modernization | 14 | 15 | 93.3% |
| **Overall** | **73** | **75** | **97.3%** |

The learner marked Questions 4, 7, 9, 15, 19, 29, 33, 34, 37, 38, 47, 51,
56, 63, 66, and 72 uncertain; 14 of 16 were correct. The two misses were
Question 47, where `BC` was submitted for a single-response item whose answer
was `B`, and Question 56, where Amazon MQ was selected instead of Transfer
Family AS2.

Question 5 independently transferred the prior Lambda@Edge/write-continuity
correction. Question 31 retained the ECS blue/green configuration, and Question
30 retained the DynamoDB MREC/MRSC and TTL distinction. The answer-bearing
assessment is `docs/exam-prep/sap-c02-full-mock-005-review-20260807.md`.

The result exceeds every quantitative threshold. Review the two narrow misses,
then continue to Full Mock 006. Both misses later transferred successfully in
Mock 006: the ARC item used the cluster data-plane endpoint, and both AS2 items
selected Transfer Family. The post-Mock-005 tutorial checkpoint does not
activate implementation work because neither miss is an implementation gap.

### Full-mock validation plan to the booking decision point

The minimum is **seven further full-length mocks after full mock 002**, producing
nine full mocks before the first booking decision point. Keep at least 48
hours between mocks where practical so each result can drive revision before
the next attempt. Continue beyond mock 009 if the evidence is not yet decisive.

Two implementation-review checkpoints prevent the tutorial backlog from being
forgotten without reducing this cadence:

- after full mock 005, review whether the results permit one bounded local
  correctness repair from genuine spare capacity; neither weekly mock may be
  replaced; and
- after the SAP-C02 attempt, decide whether the local SDK-boundary repair
  becomes the next eligible non-exam slice.

The detailed triggers and parked sequence live in the separate tutorial
workspace at
`/Users/[redacted-user]/Kiro-Workspace/handlers/docs/tutorial-resume-checkpoints.md`.
No checkpoint authorizes an AWS deployment.

The post-Mock-005 checkpoint has now been reviewed. Mock 005 exposed only two
narrow exam-remediation items and no implementation gap requiring tutorial
work. No local implementation slice is activated; the tutorial remains parked
behind the full-mock and remediation cadence.

| Period | Planned mocks | Purpose |
|---|---:|---|
| 2026-07-24 to 2026-07-30 | 1 complete | Full mock 002 established the second high-scoring baseline |
| 2026-07-31 to 2026-08-06 | 2 of 2 complete | Full Mock 003 scored 75/75 and Full Mock 004 scored 70/75; remediate Mock 004's four themes, including the recurring ECS blue/green exact-match issue, before or alongside the next scheduled mock block |
| 2026-08-07 to 2026-08-13 | 2 of 2 complete | Full Mock 005 scored 73/75 with 27/27 multiple-response. Full Mock 006 scored 71/75 with 24/27 multiple-response and 11/11 uncertain answers; its 190-minute wall clock includes approximately 10 minutes of learner-reported interruptions, so pacing evidence is qualified. The learner completed the causal review on 2026-08-14 |
| 2026-08-14 to 2026-08-16 | 1 of 1 complete | Full Mock 007 scored 75/75 in 142 minutes with 27/27 exact-match multiple-response, 7/7 uncertain answers, and successful transfer of all four Mock 006 remediation targets |
| 2026-08-17 onward | 2 of at least 2 complete | Full Mock 008 scored 75/75 in 117 active minutes. Full Mock 009 scored 73/75 with 26/27 exact-match multiple-response and 15/16 uncertain answers in 101 active minutes. The week preserved the two-full-mock cadence and reached the earliest go/no-go booking decision point; additional mocks are required only if that explicit review finds the evidence insufficient |
| **Further minimum before decision** | **7** | **Mocks 003–009 after full mock 002** |

Every mock must use a 75-question, 180-minute format with no answer-key access
before submission. Record date, mock identifier, start and end time, total time
used, time remaining, overall score, all domain scores, single-choice and
multiple-response scores, incorrect question numbers, topics missed, error
category, confidence, fatigue or pacing observations, revision action, and
re-test status.

Use the existing error categories where they fit. New results may also use:
knowledge gap, service-comparison confusion, scope or policy-evaluation error,
missed constraint, multi-select exact-match error, overengineering, cost
trade-off error, availability or disaster-recovery error, migration-strategy
error, wording or reading error, and time-pressure error.

### Score interpretation

| Score | Interpretation |
|---:|---|
| <60% | Knowledge gap, not exam-ready |
| 60–69% | Some foundations, but weak professional judgement |
| 70–74% | Nearing readiness, but risky |
| 75–79% | Potential September attempt if weak areas are narrow and fixable before the selected date |
| 80%+ | Bookable if repeated under timed conditions |

### Full-mock validation thresholds

| Indicator | Target |
|---|---|
| Full mock score | Consistently at or above 80% |
| Recent mock trend | Stable or improving |
| Domain floor | No domain below 75% |
| Multiple-response score | At or above 80% |
| Timing | Complete within 180 minutes with review time |
| Explanation quality | Explain why the correct answer wins and each selected distractor loses |
| Repeat errors | No recurring unresolved trap across multiple mocks |

Full mocks 001-009 all exceed the score, domain-floor, and multiple-response
thresholds. Mock 009 scored 97.3%, achieved 26/27 exact-match
multiple-response, and used 101 active minutes within a 106-minute wall-clock
interval that included an explicit five-minute pause. Its two misses are
narrow and do not establish a recurring unresolved trap. The minimum score and
learner-selected longitudinal gates are met; the explicit booking review is
now the decision boundary.

---

## 9. Wrong-Answer Log

Durable log file: `docs/exam-prep/wrong-answers.md`.

Use this format for every missed question.

```text
Date:
Question theme:
SAP-C02 domain:
My answer:
Correct answer:
Why correct:
Why my answer was wrong:
Exam trap:
Service comparison:
Action:
```

### Wrong-answer table

| Date | Theme | Domain | Trap | Remediation |
|---|---|---|---|---|
| 2026-06-19 | Hybrid DNS: AWS and on-premises private name resolution | Domain 1 / networking | Confused AWS Config aggregation with DNS forwarding | Use Route 53 Resolver inbound/outbound endpoints and forwarding rules; see exercise 002 revision log |
| 2026-06-19 | Migration strategy: urgent data-centre exit with minimal change | Domain 4 | Chose the attractive long-term refactor answer instead of the constraint-led rehost answer | Use AWS Application Migration Service for rehost first, then optimize; see exercise 002 revision log |
| 2026-07-01 | Hybrid network architecture: private connectivity, many VPCs, centralized routing, and hybrid DNS | Domain 1 / networking | Added Internet Gateway even though the scenario required private routing and hybrid DNS | Use Direct Connect Gateway + Transit Gateway + Route 53 Resolver inbound/outbound endpoints; include Internet Gateway only when public internet access is explicitly required |
| 2026-07-01 | High-throughput replayable event ingestion with per-customer ordering and multiple consumers | Domain 2 / Domain 3 | Chose SQS FIFO wording with an absolute "unlimited throughput" claim instead of the stream-processing pattern | Use Kinesis Data Streams with customer-ID partition keys and independent consumers/enhanced fan-out; reserve SQS FIFO for ordered queueing and deduplication |
| 2026-07-21 | ECS blue/green application deployment and rollback | Domain 2 | Selected Patch Manager for an application-revision traffic-shifting requirement | Use ECS blue/green deployment with validation and rollback; complete the spaced free-response retest after 2026-07-28 |
| 2026-07-23 | SCP versus IAM permissions boundary | Domain 1 | Used an identity-level control for an organization-wide preventive restriction | Focused exact-match remediation passed 6/6 on 2026-07-24 in 18 minutes; all uncertain responses were correct, but the early attempt remains immediate-remediation rather than spaced-recall evidence, so monitor recurrence in independent mocks |
| 2026-07-23 | DynamoDB write sharding and read fan-out | Domain 3 | Initially treated capacity mode as a repair for concentrated traffic; on the 2026-07-25 diagnostic, selected fan-out/merge but substituted an impossible strongly consistent GSI for cross-shard ordering | Spaced retest passed 6/6 on 2026-07-28: query all relevant shard keys, merge results, establish cross-shard order, and retain that GSI reads are eventual-only; monitor transfer in independent mocks |
| 2026-07-25 | DynamoDB lifecycle, change processing, and recovery | Domain 2 / Domain 3 | Selected TTL and PITR but used a GSI instead of DynamoDB Streams for item-change processing | Map requirements independently: TTL ages items out, Streams emits item-change records, PITR restores earlier table state, and a GSI adds a query access path |
| 2026-07-25 | Time-series history versus keyed latest-state lookup | Domain 2 / Domain 3 | Selected Timestream for historical analysis but omitted DynamoDB for the separately stated latest-state access pattern | Use Timestream for timestamp-centred history and time-window analysis; use DynamoDB for known-key, millisecond latest-state retrieval |
| 2026-07-28 | Physical-server application dependency discovery | Domain 4 | Selected the legacy Agentless Discovery Connector wording for detailed network mapping across physical servers | Install Application Discovery Agent for host-level process/TCP evidence; distinguish it from the current Agentless Collector's supported VMware network module and from Migration Hub's tracking role |
| 2026-07-29 | IAM Identity Center versus Cognito | Domain 1 | Selected application-user federation for centralized workforce access to many AWS accounts | Use IAM Identity Center groups, account assignments, and permission sets for workforce AWS-account access; reserve Cognito for application-user identity patterns |
| 2026-07-29 | Migration Hub home Region versus DataSync | Domain 4 | Treated missing service-control-plane data as a file-transfer problem | View and manage discovery and migration status in the configured Migration Hub home Region; DataSync moves supported workload data, not Migration Hub's managed discovery database |
| 2026-07-29 | DAX cluster plus client integration | Domain 3 | Selected the highly available DAX cluster but omitted the DAX client in a Choose TWO item | DAX requires both the cluster and an application request path through the DAX client; exact-match grading gives no credit for the incomplete response |
| 2026-07-29 | S3 interface versus gateway endpoint for on-premises access | Domain 2 | Applied the VPC-origin S3 gateway-endpoint default to traffic arriving over Direct Connect | Use an S3 interface endpoint for private on-premises access; gateway endpoint connectivity cannot be extended through Direct Connect, VPN, transit gateway, or peering |
| 2026-08-05 | Logically air-gapped backup and restore evidence | Domain 3 | Treated governance-mode locking as equivalent to service-owned-account isolation and kept the only recovery point inside the compromised workload boundary | Use a logically air-gapped vault for the explicit service-owned-account/compliance-lock/share requirement; combine immutability, administrative isolation, and restore validation for ransomware recovery |
| 2026-08-05 | CloudFront origin-failover method boundary | Domain 3 | Looked for Lambda@Edge rather than the HTTP-method constraint | Built-in origin failover applies to eligible `GET`, `HEAD`, and `OPTIONS` requests, not write methods such as `POST` |
| 2026-08-05 | ECS blue/green exact-match completeness | Domain 3 | Correctly selected CodeDeploy but omitted the required production/test listener component in a Choose TWO response | Select CodeDeploy plus the ALB production/test listeners or listener rules when controlled validation and rollback are required; treat this as recurrence of the 2026-07-21 deployment theme |
| 2026-08-05 | DynamoDB MRSC versus TTL | Domain 2 | Treated the witness Region as if it enabled an otherwise unsupported feature | TTL is supported for MREC global tables, not MRSC; a witness changes topology, not feature support |

### Non-relational database gap closure - 2026-07-24

A repository-wide study-material audit confirmed a bounded SAP-C02 revision
gap rather than a need to restart database study. DynamoDB service-selection
basics and write-sharding remediation already existed, but GSI/LSI decisions,
read consistency, current MREC/MRSC global-table behaviour, DAX, ElastiCache
engine selection, and DocumentDB/Keyspaces/Neptune/Timestream selection lacked
one coherent current revision route.

| Deliverable | Status | Evidence boundary |
|---|---|---|
| `docs/exam-prep/revision-notes/targeted-lessons/aws-non-relational-databases-sap-c02-key-lessons-20260724.md` | Complete; expanded 2026-07-25 | Source-backed lesson now includes physical partition limits and item-collection nuance, capacity calculations, GSI backpressure reason codes, DAX item/query-cache behaviour, concurrency controls, and current MREC/MRSC Streams semantics; documentation is not recall evidence |
| `docs/exam-prep/sap-c02-non-relational-databases-diagnostic-18q-20260724.md` | Complete | Frozen 2026-07-25; 15/18 in 40 minutes, with 9/9 single-choice and 6/9 multiple-response |
| `docs/exam-prep/sap-c02-non-relational-databases-diagnostic-review-20260725.md` | Complete | Answer-bearing exact-match review records only the three demonstrated gaps |
| `docs/exam-prep/sap-c02-non-relational-databases-spaced-retest-6q-20260728.md` | Complete | Frozen 2026-07-28 at 6/6 in 15 minutes; 1/1 single-response and 5/5 exact-match multiple-response; Questions 1 and 2 uncertain and correct |
| `docs/exam-prep/sap-c02-non-relational-databases-spaced-retest-review-20260728.md` | Complete | Answer-bearing review confirms all three demonstrated diagnostic gaps were recalled correctly; focused remediation closed, independent-mock transfer remains |

### Immediate revision actions from full mock 001

- Review AWS Organizations and IAM evaluation logic: SCPs, identity-based and
  resource-based policies, permissions boundaries, session policies, and
  explicit-deny precedence.
- Review Region-restriction SCP patterns: `aws:RequestedRegion`, global-service
  exceptions, OU inheritance, and separate treatment for security and log
  archive accounts.
- Review DynamoDB high-cardinality partition-key design, hot keys, random and
  calculated write-shard suffixes, read fan-out, result merging and ordering,
  and the boundary between capacity mode and access-pattern design.
- The SCP-versus-permissions-boundary closed-book exact-match retest was
  completed on 2026-07-24 at 6/6 in 18 minutes. Because it preceded the
  preferred spacing date, record it as immediate remediation and continue
  recurrence monitoring in independent mocks.
- The bounded non-relational database diagnostic was completed on 2026-07-25
  at 15/18 in 40 minutes. Its fresh six-question spaced retest was completed on
  2026-07-28 at 6/6 in 15 minutes, with all five exact-match multiple-response
  items correct and both uncertain answers correct. Focused remediation is
  complete; monitor transfer in independent mocks.

**Next tracked priority:** the GO recommendation and completed 7 Rs strategy
matrix are recorded. With separate explicit authorization, enter the Pearson
VUE flow and book the best available 2026-09-23 to 2026-09-25 appointment,
using 2026-09-28 to 2026-09-30 only as fallback. Until then, continue the
bounded final-review plan and complete the separate database and data-transfer
comparisons by 2026-09-06. The state transition to a GO recommendation has
occurred; the external booking transition remains pending authorization.

### Revision-maintenance register

These source-note refinements were parked so that they could not interrupt the
two-mock cadence. They were implemented together during the authorized
2026-08-08 cross-domain source-note maintenance pass and verified against
current official AWS documentation. This maintenance is reference material,
not learner-recall evidence.

| Target note | Parked refinement | Trigger |
|---|---|---|
| `docs/exam-prep/revision-notes/core/08-networking-hybrid-private-access.md` | Separate public-subnet classification by Internet Gateway routing from the public IPv4 addressing required for direct IPv4 internet communication. | Completed 2026-08-08 |
| `docs/exam-prep/revision-notes/core/08-networking-hybrid-private-access.md` | Make the Direct Connect paths explicit: transit VIF to Direct Connect gateway to Transit Gateway; private VIF for the virtual-private-gateway path. | Completed 2026-08-08 |
| `docs/exam-prep/revision-notes/core/11-resilience-dr-multiregion.md` | Add that CloudFront origin failover applies to `GET`, `HEAD`, and eligible `OPTIONS` requests, not write methods such as `POST` or `PUT`. | Completed 2026-08-08 |
| `docs/exam-prep/revision-notes/targeted-lessons/route-53-sap-c02-key-lessons-20260715.md` | State the exact Resolver endpoint minimum: at least two IP addresses in different Availability Zones. | Completed 2026-08-08 |
| `docs/exam-prep/revision-notes/core/11-resilience-dr-multiregion.md` and the detailed database lesson | State that DynamoDB global tables support default MREC and optional MRSC and retain MRSC feature restrictions. | Completed 2026-08-08 |

---

## 10. Booking Decision Criteria

### Booking checklist and review disposition

This review distinguishes exam-readiness evidence from broader repository
implementation and documentation completeness. A partial repository item is
booking-blocking only when it represents a material or recurring exam unknown.

| Criterion | Evidence status at review | Booking disposition |
|---|---|---|
| Two timed practice exams at 80%+ OR one 80%+ and one 75–79% with narrow weak areas | Met: all nine timed mocks scored 93.3%-100%; aggregate 656/675 (97.2%) | Met; strongly exceeds the quantitative rule |
| Domain 1 governance notes complete | Partially met as a repository programme: the governance ADR, Organizations/OU evidence, SCPs, Identity Center matrix, break-glass procedure, logging/security design, Config, GuardDuty, and closure evidence exist; Security Hub/OAM and broader enterprise implementation remain deliberate gaps | Non-blocking for booking: Domain 1 evidence is broad and Mock 009 scored 20/20; open implementation breadth is not an exam unknown |
| Networking comparison matrix complete | Met: verified decision matrix, source-backed review, tracked diagrams, and an 8/8 focused blind attempt | Met |
| Migration matrix complete | Met: the 7 Rs strategy matrix includes Relocate and is recorded at `docs/planning/domain-4-migration-decision-matrix-20260823.md` | Met; the separately tracked database and data-transfer comparisons remain bounded final-review work, not booking gates |
| Lakehouse readiness closure complete and documented | Met: core path, encryption, versioning, lifecycle, tags, IAM, current end-to-end evidence, and stale Phase 1 reconciliation are complete | Met |
| IAM/Organizations/SCP design complete | Partially met as a live programme: target structure, catalogue, examples, break-glass path, and two OU-targeted SCPs exist; wider exception testing and assignments remain open | Non-blocking for booking: design knowledge and applied evidence are sufficient; remaining live breadth requires separate approval and is not an exam-prep dependency |
| Wrong-answer log reviewed twice | Met with evidence caveat: Cycle 1 was a blind 4/4; Cycle 2 ended 4/4 after material corrections and is not represented as an unchanged clean pass | Met |
| No major unknowns in VPC, TGW, PrivateLink, DX/VPN, DR, migration | Met for exam readiness: no recurring unresolved trap remains across nine mocks; the Q11 PrivateLink role miss is narrowed by correct broader composition, and migration performance is 98.5% | Met; continue bounded recall only |
| September exam availability | Met at exam level: SAP-C02 is listed as eligible in the signed-in Certification account, no appointment exists, and AWS advertises most online-proctored appointments 24/7; exact slot inventory is unverified | Non-blocking for GO; verify a preferred slot during the separately authorized Pearson VUE flow before purchase |
| Bounded plan through exam date | Met: the dated plan in Section 1 limits migration consolidation, narrow remediation, logistics, and optional corroboration through 2026-09-23 to 2026-09-25 | Met |

### Current readiness assessment - 2026-08-22

| Item | Status |
|---|---|
| Readiness status | GO: strong and repeatable across nine full mocks, ranging from 93.3% to 100%, with a 97.2% aggregate, no domain below 75%, 96.3% exact-match multiple-response, and successful transfer of earlier remediation |
| Recommendation | Book a September SAP-C02 attempt, preferably 2026-09-23 to 2026-09-25, with 2026-09-28 to 2026-09-30 as fallback |
| Booking status | Not booked. The GO recommendation does not authorize an appointment, Pearson VUE submission, or charge |
| Primary residual risk | The 7 Rs matrix is complete. The remaining database/data-transfer comparisons are non-blocking and due by 2026-09-06. Mock 009's PrivateLink and S3 re-encryption misses require narrow recall, not another full mock |

Full mocks 001-009 demonstrate strong endurance, broad domain coverage, stable
scores above every quantitative threshold, and recovery after narrow misses.
Mock 009's 73/75 result retained strong exact-match, domain-floor, and timing
performance. No immediate focused retest or broad domain restart is justified.
The post-Mock-009 review recommends GO. Tutorials Dojo remains optional
corroboration, and a tenth full mock is not required unless new evidence
reveals a broad or recurring weakness.

### Final booking decision

| Date | Decision | Reason |
|---|---|---|
| 2026-07-23 | Do not book yet | One strong full mock is insufficient; continue independent mocks and resolve recurring traps before an evidence-led booking decision |
| 2026-07-29 | Do not book yet; minimum score gate met | Full mock 002 repeated strong performance at 94.7%, but four narrow gaps require remediation and the learner's broader consistency programme remains active |
| 2026-08-01 | Do not book yet; focused remediation complete | The four Full Mock 002 gaps passed a fresh 8/8 retest, but the then-current Mocks 003–007 validation series and migration-matrix closure remained required evidence; the gate was later extended by one mock on 2026-08-09 |
| 2026-08-03 | Do not book yet; Mock 003 clean pass | Full Mock 003 scored 75/75 with 27/27 exact-match multiple-response answers and successful transfer of all four Mock 002 gaps; continue the longitudinal series and close the migration matrix before the evidence-led decision |
| 2026-08-05 | Do not book yet; Mock 004 remains strong | Full Mock 004 scored 70/75 with every domain above 77%, 25/27 exact-match multiple-response, and 67 minutes remaining; resolve its four themes, especially the recurring ECS blue/green exact-match issue, continue the longitudinal series, and close the migration matrix |
| 2026-08-07 | Do not book yet; Mock 005 strengthens readiness evidence | Full Mock 005 scored 73/75 with 27/27 exact-match multiple-response, every domain above 93%, and 72 minutes remaining; review the two narrow misses and continue the validation series |
| 2026-08-09 | Extend the earliest booking decision to after Mock 008 | The untimed Skill Builder assessment passed but exposed broader service-boundary detail and 11 confident keyed misses. One additional full mock provides a third independent transfer opportunity after remediation without changing the two-mock cadence or September exam window |
| 2026-08-12 | Do not book yet; Mock 006 remains strong but exposes recurring exact-match gaps | Full Mock 006 scored 71/75, transferred ARC, AS2, and Domain 3 cleanly, but three of four confident Domain 2 misses recur from Skill Builder calibration. Complete focused remediation no earlier than 2026-08-15, then continue to Mocks 007 and 008 |
| 2026-08-14 | Extend the earliest go/no-go booking decision to after Mock 009 | The learner completed the Mock 006 causal review and remains on the September schedule. Mock 007 is scheduled for 2026-08-15; Mocks 008 and 009 will preserve two full mocks in the week beginning 2026-08-17 before the decision review |
| 2026-08-15 | Do not book yet; Mock 007 cleanly closes Mock 006 remediation | Full Mock 007 scored 75/75 in 142 minutes with 27/27 exact-match multiple-response, 7/7 uncertain answers, and successful transfer of all four Mock 006 remediation targets. Continue to Mocks 008 and 009 because the learner-selected booking gate remains after Mock 009 |
| 2026-08-18 | Do not book yet; Mock 008 repeats a clean pass | Full Mock 008 scored 75/75 with 27/27 exact-match multiple-response, 8/8 uncertain answers, and every domain at 100%. The 139-minute wall clock included an explicit 22-minute pause, giving 117 active minutes. Continue to Mock 009 because the learner-selected booking gate remains unchanged |
| 2026-08-22 | Mock 009 evidence reconciled; separate decision review required | Full Mock 009 scored 73/75 in 101 active minutes, with two narrow misses and every quantitative threshold exceeded. This evidence-reconciliation step deliberately stopped before the GO/NO-GO decision recorded in the next row |
| 2026-08-22 | **GO recommendation; booking not performed** | Nine mocks average 97.2%; every score exceeds 80%, every domain floor exceeds 75%, exact-match multiple-response is 234/243, recorded uncertainty is 88/95, prior remediation transferred, and the two Mock 009 misses are narrow. At review time, the incomplete migration matrix was a bounded pre-exam artifact rather than a demonstrated Domain 4 weakness. Prefer 2026-09-23 to 2026-09-25 and require explicit authorization before entering the Pearson VUE purchase flow |
| 2026-08-22 | 7 Rs strategy matrix complete; GO unchanged | The learner submitted `docs/planning/domain-4-migration-decision-matrix-20260823.md` with Rehost, Replatform, Refactor, Repurchase, Retain, Retire, and Relocate. This closes the strategy-matrix criterion; the separately tracked database and data-transfer comparisons remain bounded, non-blocking final-review work |
| 2026-09-07 | Plan checkpoint only | The post-Mock-009 evidence was decisive. Use the former backstop to verify the bounded migration artifact, narrow recall, logistics, and booking state; do not reopen the decision without material new evidence |

---

## 11. Weekly Operating Template

### Validation-period override: 2026-07-24 to 2026-08-20

This override takes precedence over the generic template below:

- complete two independent full-length timed mocks per week;
- keep at least 48 hours between mocks where practical;
- review, classify, and record every genuine miss before the next mock;
- use remaining capacity for spaced retests and the current DR, migration, and
  cost decision gaps; and
- allocate no default tutorial implementation time. A bounded local repair may
  begin only at a documented checkpoint, from genuine spare capacity, without
  replacing either mock or its review.

The generic build/lab blocks below resume only when this override ends or the
tracker is explicitly revised from new evidence.

### Monday to Friday

| Day | Session | Timebox | Output |
|---|---|---:|---|
| Monday | SAP-C02 study | 60 min | Notes + 5 review questions |
| Tuesday | Build/lab | 60–90 min | Code commit or config artifact |
| Wednesday | Practice questions | 60 min | Score + wrong-answer log |
| Thursday | Build/documentation | 60–90 min | Diagram/policy/ADR |
| Friday | Weak-area review | 60 min | Updated tracker |

### Weekend

| Block | Timebox | Output |
|---|---:|---|
| Saturday deep block | 3–4 hrs | Main build milestone |
| Sunday review block | 2–3 hrs | Diagrams, remediation, next-week plan |

---

## 12. Monthly Milestones

| Month | Main objective | Exit criteria |
|---|---|---|
| June–July | Lakehouse closure, governance evidence, Networking, and Resilience/DR transition | Lakehouse closure and bounded Domain 1 governance are evidenced; Networking package is complete; DR pattern, RTO/RPO, Lakehouse recovery-mapping, source-review, focused 12/12 submission, and fresh structurally isolated 12/12 spaced-retest artifacts are complete repository evidence |
| August | Resilience/DR closure, repeated full-mock validation, and explicit booking review | Fresh DR retest complete; Mocks 003–009 and remediation evidence recorded; post-Mock-009 review completed with a GO recommendation; the 7 Rs strategy matrix is complete, including Relocate |
| September | Bounded final review and SAP-C02 attempt | Complete the remaining database/data-transfer comparisons by 2026-09-06, use 2026-09-07 as a plan checkpoint rather than a second decision gate, and attempt the exam preferably 2026-09-23 to 2026-09-25 and no later than 2026-09-30 |
| Post-exam | Resume or re-plan deferred portfolio work | Begin only after the SAP-C02 attempt and a tracker/handover state transition |

---

## 13. Parking Lot

Use this to capture attractive distractions without acting on them.

| Idea | Why attractive | Decision | Revisit date |
|---|---|---|---|
| Further AI orchestration expansion | Existing managed path is already proven and scheduled | Freeze at current baseline; maintenance and cost control only | After SAP-C02 |
| Deep EKS | Interesting but not critical | Defer | After SAP-C02 |
| Docker/container implementation | Useful skill, but not required for the current readiness path | Defer unless a short milestone-linked exception is approved | After SAP-C02 |
| Further UI/dashboard expansion | Existing hosted dashboard is already proven | Freeze at current baseline; maintenance only | After SAP-C02 |
| Complex REMIT workflow | Domain-relevant but large | Defer | After SAP-C02 |

---

## 14. Repository Reconciliation Checklist

Use this checklist to prevent the tracker, platform documentation, and old
phase plans from drifting apart again.

### Historical repository reset completed in June 2026

- [x] Merge the completed Phase 17AU evidence branch.
- [x] Synchronize local `main` with `origin/main`, including the resolved
  repository governance instructions.
- [x] Confirm the worktree contains no conflict markers or accidental
  untracked governance file.
- [x] Reconcile tracker status labels against repository evidence.
- [x] Make the tracker the explicit planning authority in `README.md` and
  `PLANS.md`.
- [x] Replace the old AI/dashboard continuation path with the SAP-C02 active
  sequence while preserving completed phase history.

### Weekly progress control

- [ ] Record actual, build, study, and practice hours for each current week;
  retain `Not recorded` where historical totals have no durable source.
- [x] Produce at least one required artifact for every study/build session: see
  `/Users/[redacted-user]/Kiro-Workspace/handlers/learning-summary.md`,
  `docs/evidence/glue-athena-iam-live-verification-20260615.md`, and Sections 8
  and 9 below.
- [x] Run one practice-question block and record misses in the wrong-answer
  log.
- [x] Update changed checklist rows with an evidence link, not only a status:
  see the 2026-06-15 weekly hours row above, the External tutorial evidence
  register, Sections 8 and 9 below, and
  `docs/planning/domain-2-lakehouse-consolidation-20260617.md`.
- [x] Review hard deferrals before opening a new implementation branch:
  `docs/planning/domain-1-governance-focus-preflight-20260709.md` confirms the
  2026-07-13 focus remains documentation-first and preserves the tracker
  deferrals.
- [ ] Verify that any AWS-changing task has explicit approval and a cost/rollback
  boundary.

### June-July exit checklist

- [x] Complete every item in the Lakehouse closure gaps checklist.
- [x] Confirm Domain 2 evidence includes storage, transformation, query,
  security, observability, cost, and resilience decisions:
  `docs/planning/domain-2-lakehouse-consolidation-20260617.md`.
- [x] Complete at least two 20-question practice blocks and log all wrong
  answers: see Sections 8 and 9 below,
  `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/exercises/exercise-001.md`,
  and
  `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/exercises/sap-c02-exercise-002-marking-and-revision-log.md`.
- [x] Record Python/serverless Lessons 26-33 evidence separately from
  lakehouse implementation evidence; latest local suite passed 217 tests and no AWS
  resources were deployed: see the External tutorial evidence register above,
  `/Users/[redacted-user]/Kiro-Workspace/handlers/LEARNING-PLAN.md`, and
  `/Users/[redacted-user]/Kiro-Workspace/handlers/learning-summary.md`.
- [x] Review Domain 2 weak areas and update the next four-week plan:
  `docs/planning/domain-2-lakehouse-consolidation-20260617.md`.
- [x] Change the June-July milestone to complete only when code,
  documentation, diagrams, evidence, practice blocks, and separate tutorial
  evidence agree: see Section 12 above, `README.md`, `PLANS.md`, and
  `docs/planning/domain-2-lakehouse-consolidation-20260617.md`.

### Domain 1 governance preflight

- [x] Record a repo-only governance preflight that aligns with the tracker and
  keeps the Python/serverless tutorial workspace separate:
  `docs/planning/domain-1-governance-preflight-20260618.md`.
- [x] Draft current account structure, target OU shape, management-account
  rules, workload boundaries, Identity Center permission-set candidates, SCP
  catalogue, logging/security-service outline, and budget/tagging governance
  without making live AWS changes.
- [x] Convert the preflight into an accepted Organizations governance design
  ADR with explicit decisions, trade-offs, rejected alternatives, SAP-C02
  implications, revisit conditions, and implementation boundaries:
  `docs/adr/0005-aws-organizations-governance-design.md`.
- [x] Create SCP policy examples for the accepted catalogue:
  `docs/policies/scp/`.
- [x] Create an IAM Identity Center permission-set matrix:
  `docs/planning/identity-center-permission-set-matrix-20260619.md`.
- [x] Record the documentation-only IAM Identity Center assignment decision,
  including normal/emergency path boundaries and first-live-assignment gates:
  `docs/planning/domain-1-identity-center-assignment-decision-20260710.md`.
- [x] Prepare the first bounded `SecurityAudit` direct-access change for
  Security Tooling, including a dedicated Workforce Identity group, policy
  boundary, precheck, rollback, validation, and approval wording:
  `docs/planning/domain-1-identity-center-security-audit-direct-access-change-note-20260711.md`.
  The 2026-07-11 fresh precheck and custom no-mutation policy are recorded in
  `docs/evidence/domain1-governance-identity-center-security-audit-precheck-20260711.md`
  and
  `docs/policies/iam-identity-center-security-audit-security-tooling.inline-policy.example.json`.
- [x] Under separate explicit approval, execute only the prepared
  `SecurityAudit` group assignment in Security Tooling after an immediate
  fresh read-only precheck succeeds:
  `docs/evidence/domain1-governance-identity-center-security-audit-assignment-change-note-20260711.md`.
- [x] Document the break-glass access procedure:
  `docs/runbooks/break-glass-access-procedure.md`.
- [x] Document CloudTrail/log archive design:
  `docs/planning/domain-1-cloudtrail-log-archive-design-20260621.md`.
- [x] Document AWS Config and GuardDuty design, and the Security Hub
  defer/adopt decision:
  `docs/planning/domain-1-config-guardduty-design-20260621.md`.
- [x] Reconcile the Security/Observability posture with the SAP-C02 governance
  study diagrams by adopting the long-term split between storage-only
  `Security Log Archive` and live `Security Tooling`, including OAM as a
  future Security Tooling concern rather than log archive storage:
  `docs/adr/0005-aws-organizations-governance-design.md`,
  `docs/planning/domain-1-cloudtrail-log-archive-design-20260621.md`,
  `docs/planning/domain-1-config-guardduty-design-20260621.md`, and
  `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/SAP-C02_Security_Observability_Comparison.md`.
- [x] Create a governance live-readiness runbook that turns the accepted design
  into bounded change units, read-only evidence capture, rollback checkpoints,
  and validation steps:
  `docs/runbooks/domain-1-governance-live-readiness-runbook.md`.
- [x] Capture the first read-only Organizations inventory evidence and record
  the root/account/service-access state:
  `docs/evidence/domain1-governance-org-inventory-summary-20260621.md`.
- [x] Record the current-to-target OU/account-placement decision from that
  inventory evidence:
  `docs/planning/domain-1-ou-account-placement-decision-20260621.md`.
- [x] Under explicit approval, create `Lakehouse Workloads OU` and record the
  change boundary, prechange state, rollback, validation, and postchange
  evidence:
  `docs/evidence/domain1-governance-lakehouse-workloads-ou-change-note-20260621.md`.
- [x] Under separate explicit approval, move `lakehouse-workload-account` from root into
  `ou-gbyf-m6ppfmpq` and record the change boundary, propagation nuance,
  rollback, validation, and postchange evidence:
  `docs/evidence/domain1-governance-lakehouse-account-move-change-note-20260622.md`.
- [x] Draft the first bounded OU-targeted SCP change note for
  `Lakehouse Workloads OU`, using `DenyLeavingOrganization` as the narrowest
  first guardrail candidate:
  `docs/evidence/domain1-governance-deny-leaving-organization-change-note-20260622.md`.
- [x] Attempt the first bounded OU-targeted SCP live change and record the
  blocker, error evidence, and rollback cleanup when `AttachPolicy` fails
  because root `r-gbyf` is not yet ready for SCP attachment:
  `docs/evidence/domain1-governance-deny-leaving-organization-change-note-20260622.md`.
- [x] Under separate explicit approval, enable `SERVICE_CONTROL_POLICY` for
  root `r-gbyf`, validate that the root now exposes the enabled policy type,
  and record that prerequisite change:
  `docs/evidence/domain1-governance-enable-scp-root-change-note-20260622.md`.
- [x] Retry the OU-targeted `DenyLeavingOrganization` attach after root
  enablement is validated and record the successful policy attachment:
  `docs/evidence/domain1-governance-deny-leaving-organization-attach-success-change-note-20260622.md`.
- [x] Record the next narrow root-user emergency-only SCP as the follow-on
  candidate for `Lakehouse Workloads OU`, and capture the specific blocker that
  prevents safe live attachment today:
  `docs/evidence/domain1-governance-deny-root-user-actions-change-note-20260622.md`.
- [x] Define the break-glass live values for the next root-user emergency-only
  SCP candidate: emergency owner, notification path, evidence location,
  credential storage/recovery location, current scope, and post-use access
  reduction path.
- [x] Capture the first bounded CloudTrail/log archive baseline in the
  management account and record the current no-trail/no-bucket state:
  `docs/evidence/domain1-governance-cloudtrail-log-archive-change-note-20260622.md`.
- [x] Choose the SAP-C02-preferred CloudTrail ownership path and package the
  security/log archive account implementation boundary:
  `docs/planning/domain-1-security-log-archive-account-implementation-boundary-20260622.md`.
- [x] Draft the next bounded `Security OU` live-change package with current
  prechange evidence, rollback, validation, and blast-radius notes:
  `docs/evidence/domain1-governance-security-ou-change-note-20260622.md`.
- [x] Under separate explicit approval, create `Security OU` as the next narrow
  live change unit for the centralized logging path:
  `docs/evidence/domain1-governance-security-ou-change-note-20260622.md`.
- [x] Under separate explicit approval, create the dedicated
  `Security Log Archive` member account and record the durable
  account-creation boundary:
  `docs/evidence/domain1-governance-security-log-archive-account-change-note-20260624.md`.
- [x] Under separate explicit approval, enable `account.amazonaws.com`
  trusted access and set the alternate contacts for account `955659429518`:
  `docs/evidence/domain1-governance-account-management-trusted-access-and-alternate-contacts-change-note-20260624.md`.
- [x] Under separate explicit approval, move `Security Log Archive`
  (`955659429518`) from root `r-gbyf` into `Security OU`
  (`ou-gbyf-mug20ym0`):
  `docs/evidence/domain1-governance-security-log-archive-account-move-change-note-20260624.md`.
- [x] Capture fresh post-account CloudTrail baseline evidence from the
  management account and `Security Log Archive` account, including the current
  `cloudtrail.amazonaws.com` trusted-access gap and the no-bucket/no-CMK
  storage state:
  `docs/evidence/domain1-governance-cloudtrail-management-sts-prechange-20260624.json`,
  `docs/evidence/domain1-governance-cloudtrail-service-access-prechange-20260624.json`,
  `docs/evidence/domain1-governance-cloudtrail-list-prechange-20260624.json`,
  and the paired security-account prechange files.
- [x] Write the exact dedicated-log-archive S3 bucket policy and KMS key policy
  examples for the CloudTrail target design:
  `docs/policies/s3-cloudtrail-log-archive-bucket-policy.example.json` and
  `docs/policies/kms-cloudtrail-log-archive-key-policy.example.json`.
- [x] Package the next narrow `Security Log Archive` storage-step live change
  for the CloudTrail bucket and KMS key:
  `docs/evidence/domain1-governance-cloudtrail-log-archive-storage-change-note-20260624.md`.
- [x] Under separate explicit approval, create the dedicated CloudTrail
  log-archive bucket and customer-managed KMS key in the `Security Log Archive`
  account, enable versioning, Block Public Access, SSE-KMS default encryption,
  and key rotation, and attach the resolved bucket and key policies:
  `docs/evidence/domain1-governance-cloudtrail-log-archive-storage-change-note-20260624.md`.
- [x] Package the follow-on management-account organization-trail live change,
  including the explicit `cloudtrail.amazonaws.com` trusted-access
  prerequisite:
  `docs/evidence/domain1-governance-cloudtrail-organization-trail-change-note-20260624.md`.
- [x] Under separate explicit approval, enable
  `cloudtrail.amazonaws.com` trusted access, create the multi-Region
  organization trail `organization-management-events` from the management
  account, start logging, and capture the first postchange trail and
  bucket-path evidence:
  `docs/evidence/domain1-governance-cloudtrail-organization-trail-change-note-20260624.md`.
- [x] Capture the first AWS Config baseline evidence across the management,
  lakehouse, and security accounts, including the current trusted-access and
  delegated-administrator gaps:
  `docs/evidence/domain1-governance-config-*-prechange-20260624.json`.
- [x] Write the exact dedicated AWS Config archive bucket policy and KMS key
  policy examples for the security-account storage target:
  `docs/policies/s3-config-log-archive-bucket-policy.example.json` and
  `docs/policies/kms-config-log-archive-key-policy.example.json`.
- [x] Write the exact delegated-admin organization-aggregator trust-policy
  example for the AWS Config security-account role:
  `docs/policies/iam-config-organization-aggregator-role-trust-policy.example.json`.
- [x] Package the next narrow `Security Log Archive` storage-step live change
  for the AWS Config bucket and KMS key:
  `docs/evidence/domain1-governance-config-log-archive-storage-change-note-20260624.md`.
- [x] Under explicit approval, create the dedicated AWS Config archive bucket
  and customer-managed KMS key in the `Security Log Archive` account, enable
  Block Public Access, versioning, and SSE-KMS default encryption, apply the
  resolved bucket and key policies, and confirm that no recorder, delivery
  channel, or aggregator was created as part of the storage-only step:
  `docs/evidence/domain1-governance-config-log-archive-storage-change-note-20260624.md`.
- [x] Under explicit approval, enable `config.amazonaws.com` and
  `config-multiaccountsetup.amazonaws.com` trusted access, register
  `Security Log Archive` (`955659429518`) as delegated administrator for both
  service principals, create IAM role
  `aws-config-organization-aggregator-role`, and create aggregator
  `organization-config-aggregator-eu-west-2` in `eu-west-2`, while confirming
  that no recorder, delivery channel, or rule was created as part of the
  control-plane step:
  `docs/evidence/domain1-governance-config-organization-aggregation-change-note-20260624.md`.
- [x] Under explicit approval, create the management-account AWS Config
  service-linked role, create and start the first customer managed recorder and
  delivery channel in `eu-west-2`, trigger a snapshot delivery into the
  security-account archive bucket, and confirm successful delivery plus
  preserved organization aggregation:
  `docs/evidence/domain1-governance-config-management-recorder-change-note-20260624.md`.
- [x] Under explicit approval, create the lakehouse-account AWS Config
  service-linked role, create and start the next customer managed recorder and
  delivery channel in `eu-west-2`, trigger a snapshot delivery into the
  security-account archive bucket, and confirm successful recorder plus
  delivery-channel status without changing the existing organization
  aggregation boundary:
  `docs/evidence/domain1-governance-config-lakehouse-recorder-change-note-20260625.md`.
- [x] Under explicit approval, create the security-account AWS Config
  service-linked role, create and start the final intended customer managed
  recorder and delivery channel in `eu-west-2`, trigger a snapshot delivery
  into the security-account archive bucket, and confirm successful recorder,
  delivery-channel, bucket-object, and preserved aggregation evidence without
  enabling Config rules:
  `docs/evidence/domain1-governance-config-security-recorder-change-note-20260625.md`.
- [x] Under explicit approval, create the first organization AWS Config managed
  rule from the delegated-admin security account using
  `MULTI_REGION_CLOUD_TRAIL_ENABLED`, capture the sandbox-recorder blocker in
  `974893866311`, narrow the rule by excluding that sandbox account for now,
  resolve the management-account
  `AWSServiceRoleForConfigMultiAccountSetup` blocker, retry the same bounded
  deployment, and record final successful deployment across management,
  lakehouse, and security accounts:
  `docs/evidence/domain1-governance-config-org-cloudtrail-rule-change-note-20260625.md`.
- [x] Re-check `org-multi-region-cloudtrail-enabled` until management account
  `349687196588` no longer reports `UPDATE_IN_PROGRESS`, resolve the
  management-account `AWSServiceRoleForConfigMultiAccountSetup` blocker, and
  refresh the evidence:
  `docs/evidence/domain1-governance-config-org-cloudtrail-rule-change-note-20260625.md`.
- [x] Under explicit approval, extend the central Config archive bucket and
  Config KMS key policies to include sandbox account `974893866311`, create
  the sandbox-account AWS Config service-linked role, recorder, and delivery
  channel in `eu-west-2`, trigger a snapshot delivery into the central archive
  bucket, and confirm successful recorder, delivery-channel, and bucket-object
  evidence without yet changing the current organization rule exclusion:
  `docs/evidence/domain1-governance-config-sandbox-recorder-change-note-20260625.md`.
- [x] Update `org-multi-region-cloudtrail-enabled` so sandbox account
  `974893866311` is no longer excluded, then verify successful organization
  rule deployment across management, lakehouse, security, and sandbox:
  `docs/evidence/domain1-governance-config-org-cloudtrail-rule-change-note-20260625.md`.
- [x] Create the dedicated IAM Identity Center break-glass principal
  `breakglass-principal` / `[redacted-email]` and confirm one
  enrolled MFA device in same-day console evidence:
  `docs/evidence/domain1-governance-identity-center-current-state-20260625.md`.
- [x] Under explicit approval, create the dedicated emergency
  `BreakGlassAdmin` permission set with `PT1H` session duration, attach the
  AWS-managed `AdministratorAccess` policy as the first staged implementation,
  and assign it only to management account `349687196588` for
  `breakglass-principal`:
  `docs/evidence/domain1-governance-breakglass-permission-set-change-note-20260625.md`.
- [x] Under explicit approval, remove `breakglass-principal` from
  `cloud-lab-aws-admins` so the emergency user no longer inherits the routine
  management-account `AdministratorAccess` path and retains only direct
  `BreakGlassAdmin`:
  `docs/evidence/domain1-governance-breakglass-group-membership-cleanup-20260702.md`.
- [x] Preserve read-only evidence that the workload account `464975959576` root
  user has MFA assigned through the `emergency@464975959576` authenticator
  entry:
  `docs/evidence/domain1-governance-root-mfa-readiness-check-20260702.md`.
- [x] Preserve console evidence that `breakglass-principal` has a second IAM
  Identity Center authenticator-app MFA device registered on a separate device:
  `docs/evidence/domain1-governance-breakglass-mfa2-readiness-check-20260703.md`.
- [x] Preserve evidence that the active emergency SMS notification path is
  reachable before restrictive root-user guardrails are attached:
  `docs/evidence/domain1-governance-notification-reachability-check-20260703.md`.
- [x] Preserve evidence that the Google backup codes and Microsoft/Outlook
  recovery code are readable in both private recorded storage locations and
  stored in electronic and paper formats:
  `docs/evidence/domain1-governance-recovery-code-readability-check-20260703.md`.
- [x] Record a light procedural validation of the notification, evidence, and
  access-reduction path before any live root-user emergency-only SCP attachment:
  `docs/evidence/domain1-governance-breakglass-procedural-validation-20260703.md`.
- [x] Under explicit approval, create the customer-managed
  `DenyRootUserActions-LakehouseWorkloads` SCP from the repository example,
  attach it only to `Lakehouse Workloads OU`, and record prechange,
  postchange, and rollback evidence:
  `docs/evidence/domain1-governance-deny-root-user-actions-attach-success-change-note-20260703.md`.
- [x] Under a separately authorized billing-recovery exception, temporarily
  detach `DenyRootUserActions-LakehouseWorkloads` only from `Lakehouse
  Workloads OU`, restore it immediately after owner-confirmed payment, verify
  the protected OU attachment set, and reconcile the delayed Organizations
  CloudTrail `DetachPolicy` and `AttachPolicy` events:
  `docs/evidence/domain1-governance-workload-billing-root-recovery-20260713.md`.
- [x] Draft the bounded `Security Tooling` account design-to-implementation
  note, preserving the split between write-mostly `Security Log Archive` and
  active delegated security tooling:
  `docs/planning/domain-1-security-tooling-account-implementation-boundary-20260704.md`.
- [x] Collect fresh read-only Organizations/account prechange evidence for the
  `Security Tooling` account slice after refreshing the `org-admin` SSO token:
  `docs/evidence/domain1-governance-security-tooling-account-prechange-summary-20260704.md`.
- [x] Close the non-secret account-boundary decisions for the first
  `Security Tooling` implementation slice: account name, target OU,
  same-session root-to-OU move, alternate-contact deferral, no delegated-admin
  migration, `Security Log Archive` storage-only role, and `so-aws-admin`
  current-state acknowledgement. The unique root email was then supplied and
  approved before live account creation:
  `docs/planning/domain-1-security-tooling-account-implementation-boundary-20260704.md`.
- [x] Under separate explicit approval, create a separate `Security Tooling`
  account in `Security OU`; keep `Security Log Archive` storage-only; do not
  migrate delegated-admin functions during account creation:
  `docs/evidence/domain1-governance-security-tooling-account-placement-change-note-20260704.md`.
- [x] Under separate explicit approval, configure `SECURITY`, `OPERATIONS`,
  and `BILLING` alternate contacts for `Security Tooling` (`668848431187`)
  before security-service migration:
  `docs/evidence/domain1-governance-security-tooling-alt-contacts-change-note-20260706.md`.
- [x] Record the public-repository evidence redaction gate after the
  2026-07-07 history-cleanup pass: keep exact AWS contact values and raw
  contact/account evidence outside this public repository, run
  `scripts/check_public_evidence_redaction.sh` before staging governance
  evidence, and remember that GitHub caches, forks, or local clones may retain
  old objects outside this repository's cleaned refs:
  `docs/runbooks/domain-1-governance-live-readiness-runbook.md`.
- [x] Migrate AWS Config delegated administration and aggregation from
  `Security Log Archive` (`955659429518`) to `Security Tooling`
  (`668848431187`) first:
  `docs/evidence/domain1-governance-config-security-tooling-migration-change-note-20260706.md`.
- [x] Decide recorder scope for `so-aws-admin` (`054394900225`) and
  `Security Tooling` (`668848431187`) before extending the migrated
  organization CloudTrail Config rule to those accounts:
  `docs/planning/domain-1-config-recorder-scope-decision-20260706.md`.
- [x] Record the `so-aws-admin` (`054394900225`) account-purpose decision:
  place it on the decommission path, retire it only after read-only dependency
  checks and dependency resolution, and keep future Security Hub in
  `Security Tooling` (`668848431187`) if adopted:
  `docs/planning/domain-1-so-aws-admin-decommission-decision-20260706.md`.
- [x] Under separate explicit approval, onboard the `Security Tooling`
  (`668848431187`) AWS Config recorder in `eu-west-2`, then remove only that
  account from the migrated organization CloudTrail Config rule exclusions;
  keep `so-aws-admin` excluded on the decommission path:
  `docs/evidence/domain1-governance-config-security-tooling-recorder-change-note-20260707.md`.
- [x] Collect read-only dependency evidence for `so-aws-admin` retirement
  readiness, resolve any dependencies, and require separate explicit approval
  before account closure. Initial management-visible evidence is recorded in
  `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-20260709.md`;
  the existing-profile access check is recorded in
  `docs/evidence/domain1-governance-so-aws-admin-direct-access-profile-check-20260709.md`;
  the temporary direct-inventory access plan is recorded in
  `docs/planning/domain-1-so-aws-admin-direct-inventory-access-plan-20260709.md`.
  Under separate explicit approval, direct read-only inventory was captured and
  the temporary target-account assignment was removed and verified:
  `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-readiness-20260709.md`.
  Follow-on deletion of the temporary permission set is recorded in
  `docs/evidence/domain1-governance-so-aws-admin-permission-set-cleanup-20260709.md`.
  Under separate explicit approval, the account was closed on 2026-07-09 after
  a fresh pre-close check returned zero blockers; post-close Organizations
  evidence reports `Status: SUSPENDED` and `State: CLOSED`:
  `docs/evidence/domain1-governance-so-aws-admin-account-closure-20260709.md`
  and
  `docs/evidence/domain1-governance-so-aws-admin-account-closure-postclose-status-20260709.json`.
- [x] Record GuardDuty delegated-admin planning with no new account and
  `Security Tooling` (`668848431187`) as the target delegated administrator:
  `docs/planning/domain-1-guardduty-delegated-admin-planning-20260706.md`.
- [x] Under separate explicit approval, collect fresh GuardDuty prechange and
  live-readiness evidence without enabling GuardDuty or configuring delegated
  administration:
  `docs/evidence/domain1-governance-guardduty-live-readiness-20260707.md`.
- [x] Under separate explicit approval, configure GuardDuty delegated
  administration in `Security Tooling` (`668848431187`) for `eu-west-2`, enable
  foundational coverage for approved active accounts, keep `so-aws-admin`
  excluded, and leave optional protection plans disabled:
  `docs/evidence/domain1-governance-guardduty-delegated-admin-change-note-20260707.md`.
- [x] Capture the first read-only GuardDuty usage/cost observation before
  considering another Region or optional protection plan:
  `docs/evidence/domain1-governance-guardduty-usage-cost-observation-20260709.md`.
  The initial Cost Explorer buckets are estimated at `$0`; keep the current
  foundational-only posture and continue observing before any expansion.
- [ ] Adopt Security Hub only if later intentionally adopted in
  `Security Tooling`.
- [ ] During the remaining governance focus, maintain current live-readiness
  evidence and use separate explicit approval for any further live changes.

---

## 15. Acronym Legend

| Acronym | Meaning |
|---|---|
| ADR | Architecture Decision Record |
| ALB | Application Load Balancer |
| AWS | Amazon Web Services |
| DNS | Domain Name System |
| DR | Disaster Recovery |
| ECR | Elastic Container Registry |
| ECS | Elastic Container Service |
| EKS | Elastic Kubernetes Service |
| IAM | Identity and Access Management |
| KMS | Key Management Service |
| OU | Organizational Unit |
| REMIT | Regulation on Wholesale Energy Market Integrity and Transparency |
| RPO | Recovery Point Objective |
| RTO | Recovery Time Objective |
| S3 | Simple Storage Service |
| SAP-C02 | AWS Certified Solutions Architect – Professional exam version |
| SCP | Service Control Policy |
| TGW | Transit Gateway |
| VPC | Virtual Private Cloud |
| VPN | Virtual Private Network |

Documentation note: write `AWS Well-Architected Framework` in full. Avoid using
`WAF` for the framework because `AWS WAF` commonly means the AWS Web Application
Firewall service.
