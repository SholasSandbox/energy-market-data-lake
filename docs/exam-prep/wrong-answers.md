# SAP-C02 Wrong-Answer Log

This log is the durable exam-prep companion to the tracker wrong-answer table.
It is seeded from the Lakehouse repository tracker and is not Lakehouse
implementation evidence from any external tutorial workspace.

**Document role:** cumulative remediation and status log. Use the
[Exam-Prep Revision Hub](README.md) to choose the correct workflow. Do not open
this log while completing a blind review cycle.

## How to Use This Log

1. During **test**, keep this document closed.
2. After explicit submission, compare the answer with the applicable decision
   rule and explain the losing choice.
3. Add or update an entry only for a genuine miss or material precision gap.
4. Schedule a spaced blind retest; do not count rereading as a review cycle.
5. Keep untimed recall separate from timed-practice and booking evidence.

For the active folder sequence and document status, start at the
[Exam-Prep Revision Hub](README.md). Review Cycle 2 uses the separate
[blind-attempt document](wrong-answer-review-cycle-2-blind-attempt-20260715.md).

## Quick Remediation Index

| Weak area | Durable entry |
|---|---|
| Hybrid private DNS | [Hybrid DNS Private Resolution](#2026-06-19-hybrid-dns-private-resolution) |
| Urgent data-centre exit | [Urgent Data-Centre Exit](#2026-06-19-urgent-data-centre-exit) |
| Private multi-VPC hybrid architecture | [Private Hybrid Network Architecture](#2026-07-01-private-hybrid-network-architecture) |
| Replayable ordered event ingestion | [Replayable Ordered Event Ingestion](#2026-07-01-replayable-ordered-event-ingestion) |

## Review Status

| Item | Status |
|---|---|
| Current through practice block | 007 |
| Source-backed carry-forward review | Completed 2026-07-09; Review Cycle 1 completed 2026-07-15 and Review Cycle 2 remains open |
| First review cycle evidenced | Completed 2026-07-15: 4/4 blind recall |
| Second review cycle evidenced | Not yet |
| Controlling tracker reference | `docs/planning/sap-c02-readiness-tracker.md` |

## Practice Score Summary

| Block | Score | Percentage | Notes |
|---|---:|---:|---|
| Block 002 | 18 / 20 | 90% | Missed hybrid DNS and rehost vs refactor. |
| Block 003 | 24 / 25 | 96% | One multi-select option-selection miss on hybrid networking. |
| Block 004 | 25 / 25 | 100% | Clean pass. |
| Block 005 | 25 / 25 | 100% | Clean pass. Strong multi-select discipline. |
| Block 006 | 24 / 25 | 96% | Missed Kinesis vs SQS FIFO streaming distinction. |
| Block 007 | 25 / 25 | 100% | Clean pass. Stronger coverage of Domain 2 and Domain 3 topics. |

## Artifact Evidence

| Artifact | Scope | Repository Handling |
|---|---|---|
| `sap-c02-completed-exercises-003-to-006.zip` | Completed review logs for Blocks 003 through 006 | Binary archive retained outside the public repo; sanitized manifest recorded at `docs/exam-prep/artifacts/sap-c02-completed-exercises-003-to-006-manifest.md`. |

## Review Cycle 1 Checklist

Opened: 2026-07-07

Completion rule: mark this cycle complete only after each drill can be answered
from memory without looking at the correction text first.

| Drill | Review Question | Status |
|---|---|---|
| Hybrid DNS | Which AWS service pair handles private DNS forwarding between VPCs and on-premises DNS? | Correct from memory 2026-07-15 |
| Urgent migration | When the scenario says urgent data-centre exit and minimal change, which migration pattern wins first? | Correct from memory 2026-07-15 |
| Private hybrid network | Which services map to private multi-VPC hybrid routing and DNS, and why is Internet Gateway excluded? | Correct from memory 2026-07-15 |
| Replayable event ingestion | Which requirements push the answer from SQS FIFO to Kinesis Data Streams? | Correct from memory 2026-07-15 |

## Review Cycle 1 Evidence - 2026-07-15

Mode: four blind, free-response drills completed without the correction text in
the prompt. Result: **4/4 correct**.

| Drill | Learner recall evidence | Assessment |
|---|---|---|
| Hybrid DNS | Identified inbound and outbound Route 53 Resolver endpoints, Resolver rules, and Transit Gateway as transport rather than DNS forwarding. | Correct. |
| Urgent migration | Selected rehost/lift-and-shift with AWS Application Migration Service and described block-level replication. | Correct. |
| Private hybrid network | Selected Transit Gateway with Direct Connect or VPN for private routing and Resolver endpoints for DNS; excluded Internet Gateway because no public path was required. | Correct. |
| Replayable event ingestion | Selected Kinesis Data Streams for replay, high throughput, and multiple independent consumers while preserving ordered stream processing. | Correct; retain the more precise exam wording that ordering is per partition key. |

This completes recall-based Review Cycle 1 only. It is untimed, does not count
as a practice exam, and does not complete the required second review cycle.
The broader Route 53 consolidation lesson is recorded in
`docs/exam-prep/route-53-sap-c02-key-lessons-20260715.md`.

## Carry-Forward Review - 2026-07-09

Scope: this is a repository-backed review of the logged corrections and their
supporting comparison notes. It does not claim that the learner completed the
recall-based Review Cycle 1 checklist; those four drills remain pending.

| Weak area | Verified decision rule | Supporting repository evidence | Carry-forward action |
|---|---|---|---|
| Hybrid DNS | Use Route 53 Resolver inbound/outbound endpoints and forwarding rules; AWS Config does not resolve DNS. | `docs/planning/domain-2-network-access-patterns-20260621.md` | Complete the pending recall drill, then retain this pattern for the September networking matrix. |
| Urgent migration | Rehost first with AWS Application Migration Service when speed and minimal change are explicit. | This log's 2026-06-19 entry and the tracker migration weak-area register. | Revisit in the September migration decision table; do not treat one corrected question as full migration readiness. |
| Private hybrid network | Map each service to a stated need: Direct Connect Gateway and Transit Gateway for private hybrid routing, Route 53 Resolver for DNS, and no Internet Gateway unless public access is required. | `docs/planning/domain-2-network-access-patterns-20260621.md` | Complete the pending recall drill, then carry the decision pattern into the September networking artifacts. |
| Replayable event ingestion | Prefer Kinesis Data Streams for per-key ordering, replay, and independent consumers; use SQS FIFO for ordered queueing and deduplication. | This log's 2026-07-01 entry and tracker wrong-answer table. | Complete the pending recall drill and test the distinction in a future timed practice block. |

The carry-forward review confirms that the identified weak areas have durable
decision rules and a later milestone. It does not change the booking criterion:
two recall-based review cycles and timed-practice evidence are still required.

## Decision Rules To Drill

| Pattern | Preferred Decision Rule |
|---|---|
| Hybrid DNS between AWS and on-premises | Use Route 53 Resolver inbound/outbound endpoints and forwarding rules. |
| Urgent data-centre exit with minimal change | Rehost first with AWS Application Migration Service, then optimize. |
| Private multi-VPC hybrid connectivity | Use Direct Connect Gateway, Transit Gateway, and Route 53 Resolver when private routing and hybrid DNS are stated. |
| Replayable high-throughput event ingestion with multiple consumers | Use Kinesis Data Streams with partition keys and independent consumers; use SQS FIFO for ordered queueing, not stream replay. |

## Entries

### 2026-06-19: Hybrid DNS Private Resolution

```text
Question theme: AWS and on-premises private name resolution
SAP-C02 domain: Domain 1 / networking
My answer pattern: Treated AWS Config aggregation as if it solved DNS forwarding.
Correct answer pattern: Route 53 Resolver inbound/outbound endpoints with forwarding rules.
Why correct: Resolver endpoints bridge DNS queries between VPCs and on-premises DNS servers while keeping private names resolvable.
Why my answer was wrong: AWS Config aggregates resource configuration and compliance state; it does not forward or resolve DNS queries.
Exam trap: A governance or inventory service appears plausible because the scenario says multi-account or hybrid, but the functional requirement is DNS resolution.
Service comparison: Route 53 Resolver vs AWS Config.
Action: Drill hybrid DNS scenarios until the resolver-endpoint wording is automatic.
Review status: Review Cycle 1 correct from memory on 2026-07-15; Review Cycle 2 remains open.
```

### 2026-06-19: Urgent Data-Centre Exit

```text
Question theme: Migration strategy with urgent exit and minimal application change
SAP-C02 domain: Domain 4
My answer pattern: Chose the attractive long-term refactor answer.
Correct answer pattern: Rehost first with AWS Application Migration Service, then optimize after migration.
Why correct: The scenario prioritizes speed, low change, and data-centre exit risk reduction.
Why my answer was wrong: Refactoring may be the better target architecture later, but it violates the immediate time and change constraints.
Exam trap: Prefer the most modern architecture instead of the answer that satisfies the stated constraint first.
Service comparison: AWS Application Migration Service rehost vs refactor/replatform options.
Action: For migration questions, identify time pressure, change tolerance, and cutover risk before choosing the pattern.
Review status: Review Cycle 1 correct from memory on 2026-07-15; Review Cycle 2 remains open.
```

### 2026-07-01: Private Hybrid Network Architecture

```text
Question theme: Private connectivity, many VPCs, centralized routing, and hybrid DNS
SAP-C02 domain: Domain 1 / networking
My answer pattern: Added an Internet Gateway despite private connectivity being required.
Correct answer pattern: Direct Connect Gateway plus Transit Gateway plus Route 53 Resolver inbound/outbound endpoints.
Why correct: DX Gateway and Transit Gateway support scalable private hybrid connectivity, while Resolver endpoints handle hybrid DNS.
Why my answer was wrong: Internet Gateway supports public internet routing and was not required by the private-routing scenario.
Exam trap: Adding a familiar network component that is not mapped to a stated requirement.
Service comparison: Internet Gateway vs Direct Connect Gateway / Transit Gateway / Route 53 Resolver.
Action: For multi-select networking questions, require every selected service to map to an explicit requirement.
Review status: Review Cycle 1 correct from memory on 2026-07-15; Review Cycle 2 remains open.
```

### 2026-07-01: Replayable Ordered Event Ingestion

```text
Question theme: High-throughput replayable ingestion with per-customer ordering and multiple consumers
SAP-C02 domain: Domain 2 / Domain 3
My answer pattern: Chose SQS FIFO because of ordering language.
Correct answer pattern: Kinesis Data Streams with customer-ID partition keys and independent consumers or enhanced fan-out.
Why correct: Kinesis supports ordered records per partition key, replay from stream retention, and multiple independent consumers.
Why my answer was wrong: SQS FIFO is an ordered queueing and deduplication pattern, not the best fit for replayable stream processing with multiple consumers.
Exam trap: Latching onto "ordering" and ignoring replay plus multiple-consumer requirements.
Service comparison: Kinesis Data Streams vs SQS FIFO.
Action: Split event questions into ordering, replay, throughput, and consumer model before selecting the service.
Review status: Review Cycle 1 correct from memory on 2026-07-15; Review Cycle 2 remains open.
```
