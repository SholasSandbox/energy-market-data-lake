# SAP-C02 Exam-Prep Revision Hub

<!-- markdownlint-disable MD013 -->

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

Complete the four-question
[Wrong-Answer Review Cycle 2 blind attempt](wrong-answer-review-cycle-2-blind-attempt-20260715.md)
without opening the wrong-answer log, lessons, scenario review, external
documentation, search, or AI assistance. It is still awaiting an explicit
learner submission. Do not infer a score from draft or partial answers.

After explicit submission, assess it against the durable decision rules in the
[wrong-answer log](wrong-answers.md), record the outcome, and keep it separate
from timed-exam and booking evidence.

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
3. **Test:** use a fresh blind artifact or the open Review Cycle 2 attempt.
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
| Resolver inbound/outbound and hybrid DNS | [Route 53 lesson](route-53-sap-c02-key-lessons-20260715.md) | [Review Cycle 2 blind attempt](wrong-answer-review-cycle-2-blind-attempt-20260715.md) before consulting the log |
| ALB, NLB, GWLB, CloudFront, Global Accelerator | [Broader Networking lesson](aws-networking-sap-c02-key-lessons-20260717.md) | Recall Check in the same lesson; use a separate answer sheet |
| SG, NACL, WAF, Shield, Network Firewall | [Broader Networking lesson](aws-networking-sap-c02-key-lessons-20260717.md) | [Scenario review](networking-scenario-drill-review-20260715.md) |
| Flow Logs, Reachability Analyzer, Traffic Mirroring | [Broader Networking lesson](aws-networking-sap-c02-key-lessons-20260717.md) | Recall Check in the same lesson; use a separate answer sheet |
| Migration rehost decision and Kinesis versus SQS FIFO | [Wrong-answer log](wrong-answers.md) during remediation only | [Review Cycle 2 blind attempt](wrong-answer-review-cycle-2-blind-attempt-20260715.md) first |

## Document Catalogue and Status

| Document | Role | Status |
|---|---|---|
| [AWS Networking Beyond Route 53](aws-networking-sap-c02-key-lessons-20260717.md) | Source-backed lesson | Ready for revision; no recall score implied |
| [Amazon Route 53 Key Lessons](route-53-sap-c02-key-lessons-20260715.md) | Source-backed lesson | Ready for revision; no recall score implied |
| [Networking Scenario Drill — Blind Attempt](networking-scenario-drill-blind-attempt-20260715.md) | Learner submission | Completed 2026-07-15, 8/8; focused and untimed |
| [Networking Scenario Drill Review](networking-scenario-drill-review-20260715.md) | Answer-bearing review | Use only after a blind attempt |
| [Wrong-Answer Review Cycle 2](wrong-answer-review-cycle-2-blind-attempt-20260715.md) | Active blind attempt | Awaiting explicit submission; preserve existing learner text |
| [Wrong-Answer Log](wrong-answers.md) | Cumulative remediation and status log | Review Cycle 1 complete at 4/4; Cycle 2 open |
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
