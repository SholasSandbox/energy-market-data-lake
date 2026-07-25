# SAP-C02 Exam-Prep Revision Hub

<!-- markdownlint-disable MD013 MD060 -->

**Last revised:** 2026-07-25

## Start Here

This folder separates four kinds of evidence. Choose the artifact by task, not
just by topic:

| Artifact type | Purpose | Open when | Do not use it for |
|---|---|---|---|
| Lesson | Learn and consolidate decision rules | First exposure or remediation | Proving blind recall |
| Blind attempt | Retrieve answers without cues | After spacing and before review material | Learning from an answer key |
| Review | Check reasoning and study decision rules | Only after submitting the related attempt | A blind attempt |
| Log or manifest | Preserve scores, misses, status, and evidence provenance | Planning remediation or auditing progress | Last-minute topic teaching |

## Current Immediately Actionable Priority

The bounded non-relational database gap-closing diagnostic was frozen on
2026-07-25 and scored **15/18 (83.3%)** in 40 minutes: 9/9 single-choice and
6/9 multiple-response. Use the
[answer-bearing diagnostic review](sap-c02-non-relational-databases-diagnostic-review-20260725.md)
for questions 4, 8, and 17 only. The demonstrated gaps are random write-shard
read ordering/GSI consistency, DynamoDB lifecycle and recovery feature mapping,
and separating time-series history from latest-state lookup. Complete the
[six-question spaced retest](sap-c02-non-relational-databases-spaced-retest-6q-20260725.md)
no earlier than 2026-07-28; it remains question-only.
The source-backed lesson was expanded on 2026-07-25 with physical-partition
limits, capacity arithmetic, GSI backpressure diagnosis, DAX cache mechanics,
concurrency controls, and current MREC/MRSC Streams behaviour. Outdated claims
that all global tables are eventual-only or that on-demand capacity repairs a
hot GSI key were deliberately not carried forward.

The
[four-question Review Cycle 2 test](wrong-answer-review-cycle-2-blind-attempt-20260715.md)
was explicitly submitted on 2026-07-18. Its corrected final answers scored 4/4;
the earlier saved drafts and their material precision gaps remain preserved, so
the result is a completed review-and-correction cycle rather than an unchanged
clean blind pass. The separate
[mixed practice Block 2](sap-c02-mixed-practice-block-2-submission-20260718.md)
remains independent 25/25 practice evidence.

Do not repeat another easy mixed block immediately. The tracker-controlled
transition into resilience/DR has occurred through a bounded plan and the first
DR pattern matrix. The business-led
[RTO/RPO decision table](../planning/domain-2-rto-rpo-decision-table-20260718.md)
is also complete in the repository without inventing Lakehouse targets. The Lakehouse
recovery mapping and the answer-bearing
[source-backed scenario review](resilience-dr-scenario-drill-review-20260719.md)
are complete in the repository. The learner explicitly submitted all 12 scenario choices
on 2026-07-20 and scored 12/12, self-attesting that the first attempt was made
without viewing the key. The
[submission record](resilience-dr-scenario-drill-submission-20260720.md)
preserves the untimed and answer-bearing-source isolation caveats. The next
Resilience/DR gate is a fresh question-only spaced retest no earlier than
2026-07-27. Following the 2026-07-23 baseline, the learner plans two independent
75-question simulated exams per week over four weeks, ahead of the formal
readiness and booking review on Monday, 2026-09-07. The target is to attempt
SAP-C02 no later than 2026-09-30.

The
[30-question timed mixed diagnostic](sap-c02-mixed-diagnostic-30q-20260720.md)
was explicitly submitted on 2026-07-21 at 29/30 in 67 of 72 minutes. All 12
multiple-response questions were correct. The single miss was ECS blue/green
deployment and rollback versus Systems Manager Patch Manager; use the
[answer-bearing review](sap-c02-mixed-diagnostic-30q-review-20260721.md) only
for remediation. The fresh Resilience/DR spaced retest remains scheduled for
2026-07-27, and the deployment-strategy miss receives its own later spaced
free-response retest.

## Recommended Revision Routes

### 15-Minute Networking Refresh

1. Use the fast route in
   [AWS Networking Beyond Route 53](aws-networking-sap-c02-key-lessons-20260717.md).
2. Use the 10-minute route in
   [Amazon Route 53 Key Lessons](route-53-sap-c02-key-lessons-20260715.md).
3. Stop reading and write the answers to both Recall Checks from memory.

### 45-Minute Networking Consolidation

1. Review VPC routing, connectivity, hybrid transport, load balancers, global
   ingress, security, and troubleshooting in the broader Networking lesson.
2. Review routing policies, health checks, private DNS, Resolver direction,
   DNS Firewall, and DNSSEC in the Route 53 lesson.
3. Draw the principal packet and DNS paths without notes.
4. Answer both Recall Checks blind and review only the missed sections.

### 60–90-Minute Learn-Test-Review Block

1. **Learn:** use the two key-lesson documents.
2. **Close the lessons:** do not keep them beside the attempt.
3. **Test:** use a fresh blind artifact appropriate to the next scheduled
   review.
4. **Submit:** explicitly freeze the answers before checking them.
5. **Review:** use the scenario review or wrong-answer log only now.
6. **Record:** log genuine misses and schedule a spaced retest.

## Topic-to-Document Map

| Topic | Learn or refresh | Test or review |
|---|---|---|
| VPC routing, IPv4/IPv6, endpoints, NAT | [Broader Networking lesson](aws-networking-sap-c02-key-lessons-20260717.md) | [Focused blind attempt](networking-scenario-drill-blind-attempt-20260715.md) and [scenario review](networking-scenario-drill-review-20260715.md) |
| Peering, Transit Gateway, PrivateLink | [Broader Networking lesson](aws-networking-sap-c02-key-lessons-20260717.md) | [Scenario review](networking-scenario-drill-review-20260715.md) |
| Direct Connect, VPN, and hybrid gateway roles | [Broader Networking lesson](aws-networking-sap-c02-key-lessons-20260717.md) | [Scenario review](networking-scenario-drill-review-20260715.md) |
| Route 53, hosted zones, routing policies, health, DNSSEC | [Route 53 lesson](route-53-sap-c02-key-lessons-20260715.md) | Recall Check in the same lesson; use a separate answer sheet |
| Resolver inbound/outbound and hybrid DNS | [Route 53 lesson](route-53-sap-c02-key-lessons-20260715.md) | [Review Cycle 2 evidence](wrong-answer-review-cycle-2-blind-attempt-20260715.md#final-assessment) records the corrected final answer and initial gap |
| ALB, NLB, GWLB, CloudFront, Global Accelerator | [Broader Networking lesson](aws-networking-sap-c02-key-lessons-20260717.md) | Recall Check in the same lesson; use a separate answer sheet |
| SG, NACL, WAF, Shield, Network Firewall | [Broader Networking lesson](aws-networking-sap-c02-key-lessons-20260717.md) | [Scenario review](networking-scenario-drill-review-20260715.md) |
| Flow Logs, Reachability Analyzer, Traffic Mirroring | [Broader Networking lesson](aws-networking-sap-c02-key-lessons-20260717.md) | Recall Check in the same lesson; use a separate answer sheet |
| RTO/RPO, dependency constraints, and objective worksheet | [RTO/RPO decision table](../planning/domain-2-rto-rpo-decision-table-20260718.md) | Apply the reusable record without assigning unapproved Lakehouse targets |
| Backup/restore, pilot light, warm standby, active/active | [Resilience and DR lesson](aws-resilience-dr-sap-c02-key-lessons-20260718.md) | [12/12 focused submission](resilience-dr-scenario-drill-submission-20260720.md); fresh isolated spaced retest pending |
| Lakehouse recovery boundary | [Lakehouse recovery mapping](../planning/domain-2-lakehouse-recovery-mapping-20260719.md) | [Source-backed scenario review](resilience-dr-scenario-drill-review-20260719.md) and [focused submission](resilience-dr-scenario-drill-submission-20260720.md); no live recovery claim |
| DynamoDB data modelling, indexes, consistency, hot keys, global tables and DAX | [Non-relational databases key lesson](aws-non-relational-databases-sap-c02-key-lessons-20260724.md) and [hidden-gap model review](sap-c02-hidden-gap-model-review-20260725.md) | [Frozen 15/18 diagnostic](sap-c02-non-relational-databases-diagnostic-18q-20260724.md), [review](sap-c02-non-relational-databases-diagnostic-review-20260725.md), and [pending spaced retest](sap-c02-non-relational-databases-spaced-retest-6q-20260725.md) |
| ElastiCache Valkey/Redis OSS versus Memcached | [Non-relational databases key lesson](aws-non-relational-databases-sap-c02-key-lessons-20260724.md) | Diagnostic questions 11–13 passed 3/3; continue transfer testing in independent mocks |
| DocumentDB, Keyspaces, Neptune, Timestream and OpenSearch contrast | [Non-relational databases key lesson](aws-non-relational-databases-sap-c02-key-lessons-20260724.md) | Diagnostic questions 14–18 scored 4/5; [review question 17](sap-c02-non-relational-databases-diagnostic-review-20260725.md#17---time-series-selection) and complete the spaced retest |
| Migration rehost decision and Kinesis versus SQS FIFO | [Wrong-answer log](wrong-answers.md) during remediation only | [Review Cycle 2 evidence](wrong-answer-review-cycle-2-blind-attempt-20260715.md#final-assessment) records corrected final answers and initial gaps |

## Document Catalogue and Status

| Document | Role | Status |
|---|---|---|
| [AWS Networking Beyond Route 53](aws-networking-sap-c02-key-lessons-20260717.md) | Source-backed lesson | Ready for revision; no recall score implied |
| [Amazon Route 53 Key Lessons](route-53-sap-c02-key-lessons-20260715.md) | Source-backed lesson | Ready for revision; no recall score implied |
| [AWS Resilience and Disaster Recovery](aws-resilience-dr-sap-c02-key-lessons-20260718.md) | Source-backed lesson and DR pattern matrix | Ready for revision; companion RTO/RPO table complete |
| [RTO/RPO Decision Table](../planning/domain-2-rto-rpo-decision-table-20260718.md) | Business-led planning worksheet and revision aid | Complete; all Lakehouse objectives remain unset pending an owner |
| [Lakehouse Recovery Mapping](../planning/domain-2-lakehouse-recovery-mapping-20260719.md) | Repository-grounded recovery inventory and gap analysis | Complete; recoverability foundations recorded, tested recovery not proved |
| [AWS Non-Relational Databases — SAP-C02 Key Lessons](aws-non-relational-databases-sap-c02-key-lessons-20260724.md) | Source-backed, bounded database gap-closure lesson | Expanded 2026-07-25 with verified DynamoDB physical mechanics, capacity arithmetic, GSI backpressure, DAX internals, concurrency, and current global-table modes; no recall score implied |
| [SAP-C02 Closed-Book Hidden-Gap Diagnostic](sap-c02-hidden-gap-diagnostic-15q-20260723.md) | Frozen question-only transfer diagnostic | Learner answer set preserved; answer-bearing remediation remains isolated in the model review |
| [SAP-C02 Hidden-Gap Model Review](sap-c02-hidden-gap-model-review-20260725.md) | Answer-bearing remediation guide | Covers the recurring SCP/boundary and DynamoDB sharding/fan-out models; not recall evidence |
| [SCP Versus Permissions Boundary Closed-Book Retest](sap-c02-scp-permissions-boundary-closed-book-retest-6q-20260724.md) | Frozen six-question exact-match retest | Submitted 2026-07-24; 6/6 in 18 minutes; early-spacing caveat retained |
| [SCP Versus Permissions Boundary Retest Review](sap-c02-scp-permissions-boundary-closed-book-retest-review-20260724.md) | Answer-bearing assessment | Original focused trap did not recur; independent-mock transfer monitoring remains open |
| [SAP-C02 Non-Relational Databases — Closed-Book Diagnostic](sap-c02-non-relational-databases-diagnostic-18q-20260724.md) | Frozen 18-question transfer diagnostic | Submitted 2026-07-25; 15/18 in 40 minutes; 9/9 single-choice and 6/9 multiple-response |
| [Non-Relational Databases Diagnostic Review](sap-c02-non-relational-databases-diagnostic-review-20260725.md) | Answer-bearing assessment and remediation | Questions 4, 8, and 17 reviewed; three demonstrated gaps recorded |
| [Non-Relational Databases Spaced Retest](sap-c02-non-relational-databases-spaced-retest-6q-20260725.md) | Question-only six-question retest | Ready no earlier than 2026-07-28; submission and review pending |
| [Resilience and DR Scenario Drill Review](resilience-dr-scenario-drill-review-20260719.md) | Answer-bearing, source-backed review | Complete; later learner result is recorded separately |
| [Resilience and DR Scenario Submission](resilience-dr-scenario-drill-submission-20260720.md) | Learner submission and assessment | Explicitly submitted 12/12; untimed, learner-attested no-key first attempt, answer-bearing-source isolation caveat |
| [SAP-C02 Timed Mixed Diagnostic - 30 Questions](sap-c02-mixed-diagnostic-30q-20260720.md) | Completed timed learner submission | Submitted 2026-07-21; 29/30 in 67/72 minutes; 12/12 multiple-response |
| [SAP-C02 Timed Mixed Diagnostic Review](sap-c02-mixed-diagnostic-30q-review-20260721.md) | Answer-bearing assessment and remediation | One miss: ECS blue/green deployment and rollback versus Patch Manager |
| [Networking Scenario Drill — Blind Attempt](networking-scenario-drill-blind-attempt-20260715.md) | Learner submission | Completed 2026-07-15, 8/8; focused and untimed |
| [Networking Scenario Drill Review](networking-scenario-drill-review-20260715.md) | Answer-bearing review | Use only after a blind attempt |
| [Mixed Practice Block 2](sap-c02-mixed-practice-block-2-submission-20260718.md) | Learner submission | Explicitly submitted; supplied score 25/25; timing and question text unavailable |
| [Four-question Review Cycle 2](wrong-answer-review-cycle-2-blind-attempt-20260715.md) | Completed retention review | Corrected final submission 4/4; initial draft gaps preserved; untimed |
| [Wrong-Answer Log](wrong-answers.md) | Cumulative remediation and status log | Current through the 2026-07-25 non-relational database diagnostic |
| [Completed Exercises 003–006 Manifest](artifacts/sap-c02-completed-exercises-003-to-006-manifest.md) | Evidence provenance | Audit artifact, not a revision lesson |

## Revision Discipline

- Start each session by stating **learn**, **test**, **review**, or **audit**.
- During a blind attempt, do not open lessons, reviews, logs, diagrams, search,
  documentation, or AI assistance.
- Freeze and explicitly submit answers before scoring.
- Record why an answer lost, not only which service won.
- Convert each miss into a short decision rule and a spaced retest.
- Keep narrow, untimed drills separate from full-domain and timed-exam evidence.
- Do not treat documentation completeness as learner confidence.

## Repository Boundary

These are SAP-C02 study artifacts aligned with the repository tracker. They do
not prove live Lakehouse networking implementation and do not authorize AWS
changes. The controlling sequence and evidence status remain in
the [SAP-C02 readiness tracker](../planning/sap-c02-readiness-tracker.md).
