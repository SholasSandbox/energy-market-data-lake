# SAP-C02 Exam-Prep Revision Hub

<!-- markdownlint-disable MD013 MD060 -->

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
2026-07-27. Full 75-question timed exams are scheduled for the weeks of
2026-08-17 and 2026-08-24, ahead of the formal readiness and booking review on
Monday, 2026-09-07. The target is to attempt SAP-C02 no later than 2026-09-30.

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
| Migration rehost decision and Kinesis versus SQS FIFO | [Wrong-answer log](wrong-answers.md) during remediation only | [Review Cycle 2 evidence](wrong-answer-review-cycle-2-blind-attempt-20260715.md#final-assessment) records corrected final answers and initial gaps |

## Document Catalogue and Status

| Document | Role | Status |
|---|---|---|
| [AWS Networking Beyond Route 53](aws-networking-sap-c02-key-lessons-20260717.md) | Source-backed lesson | Ready for revision; no recall score implied |
| [Amazon Route 53 Key Lessons](route-53-sap-c02-key-lessons-20260715.md) | Source-backed lesson | Ready for revision; no recall score implied |
| [AWS Resilience and Disaster Recovery](aws-resilience-dr-sap-c02-key-lessons-20260718.md) | Source-backed lesson and DR pattern matrix | Ready for revision; companion RTO/RPO table complete |
| [RTO/RPO Decision Table](../planning/domain-2-rto-rpo-decision-table-20260718.md) | Business-led planning worksheet and revision aid | Complete; all Lakehouse objectives remain unset pending an owner |
| [Lakehouse Recovery Mapping](../planning/domain-2-lakehouse-recovery-mapping-20260719.md) | Repository-grounded recovery inventory and gap analysis | Complete; recoverability foundations recorded, tested recovery not proved |
| [Resilience and DR Scenario Drill Review](resilience-dr-scenario-drill-review-20260719.md) | Answer-bearing, source-backed review | Complete; later learner result is recorded separately |
| [Resilience and DR Scenario Submission](resilience-dr-scenario-drill-submission-20260720.md) | Learner submission and assessment | Explicitly submitted 12/12; untimed, learner-attested no-key first attempt, answer-bearing-source isolation caveat |
| [SAP-C02 Timed Mixed Diagnostic - 30 Questions](sap-c02-mixed-diagnostic-30q-20260720.md) | Completed timed learner submission | Submitted 2026-07-21; 29/30 in 67/72 minutes; 12/12 multiple-response |
| [SAP-C02 Timed Mixed Diagnostic Review](sap-c02-mixed-diagnostic-30q-review-20260721.md) | Answer-bearing assessment and remediation | One miss: ECS blue/green deployment and rollback versus Patch Manager |
| [Networking Scenario Drill — Blind Attempt](networking-scenario-drill-blind-attempt-20260715.md) | Learner submission | Completed 2026-07-15, 8/8; focused and untimed |
| [Networking Scenario Drill Review](networking-scenario-drill-review-20260715.md) | Answer-bearing review | Use only after a blind attempt |
| [Mixed Practice Block 2](sap-c02-mixed-practice-block-2-submission-20260718.md) | Learner submission | Explicitly submitted; supplied score 25/25; timing and question text unavailable |
| [Four-question Review Cycle 2](wrong-answer-review-cycle-2-blind-attempt-20260715.md) | Completed retention review | Corrected final submission 4/4; initial draft gaps preserved; untimed |
| [Wrong-Answer Log](wrong-answers.md) | Cumulative remediation and status log | Review Cycles 1 and 2 complete; new deployment-strategy miss recorded from the 29/30 timed diagnostic |
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
