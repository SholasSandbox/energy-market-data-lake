# SAP-C02 Wrong-Answer Log

<!-- markdownlint-disable MD013 MD060 -->

**Last revised:** 2026-07-25

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
[Exam-Prep Revision Hub](README.md). Review Cycle 2 is preserved in the
[four-question retention test](wrong-answer-review-cycle-2-blind-attempt-20260715.md),
including its initial drafts, corrected final submission, and assessment. The
clean 25-question mixed Block 2 remains separate practice evidence. Full mock
001 and its two new narrow traps are recorded below.

## Quick Remediation Index

| Weak area | Durable entry |
|---|---|
| Hybrid private DNS | [Hybrid DNS Private Resolution](#2026-06-19-hybrid-dns-private-resolution) |
| Urgent data-centre exit | [Urgent Data-Centre Exit](#2026-06-19-urgent-data-centre-exit) |
| Private multi-VPC hybrid architecture | [Private Hybrid Network Architecture](#2026-07-01-private-hybrid-network-architecture) |
| Replayable ordered event ingestion | [Replayable Ordered Event Ingestion](#2026-07-01-replayable-ordered-event-ingestion) |
| ECS blue/green deployment and rollback | [ECS Blue/Green Deployment Versus Patch Manager](#2026-07-21-ecs-bluegreen-deployment-versus-patch-manager) |
| SCP scope versus permissions boundary | [SCP Versus IAM Permissions Boundary](#2026-07-23-scp-versus-iam-permissions-boundary) |
| DynamoDB write sharding and read fan-out | [DynamoDB Write Sharding and Read Fan-Out](#2026-07-23-dynamodb-write-sharding-and-read-fan-out) |
| DynamoDB lifecycle, change processing, and recovery | [DynamoDB Lifecycle and Recovery Feature Mapping](#2026-07-25-dynamodb-lifecycle-and-recovery-feature-mapping) |
| Time-series history versus latest state | [Time-Series History Versus Latest-State Lookup](#2026-07-25-time-series-history-versus-latest-state-lookup) |

## Review Status

| Item | Status |
|---|---|
| Current through practice block | Non-relational database diagnostic submitted 2026-07-25 at 15/18 in 40 minutes; full mock 001 remains the latest full simulation |
| Source-backed carry-forward review | Completed 2026-07-09; Review Cycle 1 completed 2026-07-15 and reviewed-and-corrected Review Cycle 2 completed 2026-07-18 |
| First review cycle evidenced | Completed 2026-07-15: 4/4 blind recall |
| Second review cycle evidenced | Completed 2026-07-18: corrected final submission scored 4/4; initial saved drafts contained material gaps, so this is not recorded as an unchanged clean blind pass |
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
| Mixed practice Block 2 | 25 / 25 | 100% | Explicitly submitted 2026-07-18; all 10 multi-select questions correct; timing and question text not supplied; separate from Review Cycle 2. |
| Timed mixed diagnostic - 30 questions | 29 / 30 | 96.7% | Explicitly submitted 2026-07-21 in 67/72 minutes; all 12 multiple-response questions correct; missed ECS blue/green deployment versus Patch Manager. |
| Full mock 001 - 75 questions | 73 / 75 | 97.3% | Completed 2026-07-23 in 133/180 minutes; 48/48 single choice and 25/27 multiple response; missed SCP scope versus permissions boundaries and DynamoDB write-sharding read fan-out. |
| SCP versus permissions boundary retest | 6 / 6 | 100% | Completed 2026-07-24 in 18 minutes; all three uncertain responses were correct and the original trap did not recur; attempted before the preferred spacing date, so treat as immediate remediation rather than spaced-recall proof. |
| Non-relational database diagnostic | 15 / 18 | 83.3% | Completed 2026-07-25 in 40 minutes; 9/9 single-choice and 6/9 multiple-response; misses on questions 4, 8, and 17; six-question spaced retest due no earlier than 2026-07-28. |

## Artifact Evidence

| Artifact | Scope | Repository Handling |
|---|---|---|
| `sap-c02-completed-exercises-003-to-006.zip` | Completed review logs for Blocks 003 through 006 | Binary archive retained outside the public repo; sanitized manifest recorded at `docs/exam-prep/artifacts/sap-c02-completed-exercises-003-to-006-manifest.md`. |
| `sap-c02-mixed-diagnostic-30q-20260720.md` and `sap-c02-mixed-diagnostic-30q-review-20260721.md` | Timed 30-question submission and answer-bearing assessment | Retained separately so the frozen learner answers precede the review; 29/30 in 67 minutes. |
| `sap-c02-non-relational-databases-diagnostic-18q-20260724.md` and `sap-c02-non-relational-databases-diagnostic-review-20260725.md` | Closed-book database submission and answer-bearing assessment | Frozen answers retained separately from the review; 15/18 in 40 minutes; exact-match misses limited to questions 4, 8, and 17. |

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
as a practice exam, and did not by itself complete the required second review
cycle.
The broader Route 53 consolidation lesson is recorded in
`docs/exam-prep/route-53-sap-c02-key-lessons-20260715.md`.

## Mixed Practice Block 2 Evidence - 2026-07-18

The learner explicitly submitted a separate 25-question mixed practice block.
The supplied marking result was **25/25 (100%)**, including 15/15 single-choice
and 10/10 multi-select questions. The exact submitted choices, reported
coverage, and evidence limitations are preserved in
`docs/exam-prep/sap-c02-mixed-practice-block-2-submission-20260718.md`.

The result is not timed evidence because timing was not supplied, and it cannot
be independently re-marked because the question text and answer key are not in
the repository. It creates no new wrong-answer entries. It did not complete or
supersede the targeted four-question Review Cycle 2 test.

## Review Cycle 2 Evidence - 2026-07-18

The learner explicitly submitted revised final answers to the four-question
retention test. The final answers scored **4/4 (100%)** and correctly state the
durable decision rules for hybrid DNS, rehost/MGN, private multi-VPC hybrid
routing, and replayable ordered ingestion.

The file also preserves earlier saved draft answers. Those drafts contained
material gaps: an incomplete DNS-versus-transport explanation, an MGN service
name conflation, PrivateLink selected for hybrid transport, and imprecise
partition/shard plus SQS-throughput reasoning. Because the explicit final
answers materially correct those drafts, this is recorded as a completed
review-and-correction cycle, not an unchanged clean blind pass.

No new wrong-answer themes are added; the corrections reinforce the existing
four entries. The result is untimed and does not count as full-exam or booking
evidence.

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
the second review cycle and timed-practice evidence were still required at that
point. The mixed Block 2 clean pass and corrected Review Cycle 2 were later
recorded on 2026-07-18; timed-practice evidence remains required.

## Decision Rules To Drill

| Pattern | Preferred Decision Rule |
|---|---|
| Hybrid DNS between AWS and on-premises | Use Route 53 Resolver inbound/outbound endpoints and forwarding rules. |
| Urgent data-centre exit with minimal change | Rehost first with AWS Application Migration Service, then optimize. |
| Private multi-VPC hybrid connectivity | Use Direct Connect Gateway, Transit Gateway, and Route 53 Resolver when private routing and hybrid DNS are stated. |
| Replayable high-throughput event ingestion with multiple consumers | Use Kinesis Data Streams with partition keys and independent consumers; use SQS FIFO for ordered queueing, not stream replay. |
| ECS application revision with ALB traffic shifting and rollback | Use an ECS blue/green deployment strategy with health/metric validation and rollback; Patch Manager patches managed nodes rather than deploying container application revisions. |
| Organization-wide preventive restriction with account-class exceptions | Apply an SCP at the appropriate OU; place differently governed security accounts in a separate OU. Use a permissions boundary only to cap permissions for a specific IAM identity. |
| Concentrated DynamoDB writes on a few entity keys | Add calculated shard suffixes to distribute writes; query all relevant shards in parallel and merge/order the results on read. Capacity mode does not repair a poor key distribution. |
| DynamoDB expiry, change processing, and recovery | TTL ages items out; Streams emits item-change records; PITR restores earlier table state; a GSI only supplies another query access path. |
| Time-series history plus latest keyed state | Use Timestream for timestamp-centred history and time-window analysis; use DynamoDB for known-key, millisecond latest-state lookup. |

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
Review status: Review Cycle 1 correct from memory on 2026-07-15; Review Cycle 2 corrected final answer accepted on 2026-07-18, with the initial draft gap preserved.
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
Review status: Review Cycle 1 correct from memory on 2026-07-15; Review Cycle 2 corrected final answer accepted on 2026-07-18, with the initial service-name and sequencing gaps preserved.
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
Review status: Review Cycle 1 correct from memory on 2026-07-15; Review Cycle 2 corrected final answer accepted on 2026-07-18, with the initial PrivateLink transport error preserved.
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
Review status: Review Cycle 1 correct from memory on 2026-07-15; Review Cycle 2 corrected final answer accepted on 2026-07-18, with the initial partition/shard and SQS reasoning gaps preserved.
```

### 2026-07-21: ECS Blue/Green Deployment Versus Patch Manager

```text
Question theme: ECS application deployment with side-by-side validation, ALB traffic shifting, and automatic rollback
SAP-C02 domain: Domain 2 / deployment strategy
My answer pattern: Selected AWS Systems Manager Patch Manager.
Correct answer pattern: Use an ECS blue/green deployment strategy with test and production listeners plus health or metric alarms that can trigger rollback.
Why correct: Blue/green deployment keeps the original task set available while the replacement is validated, shifts load-balancer traffic deliberately, and can restore traffic to the original revision after failure or an alarm.
Why my answer was wrong: Patch Manager automates operating-system and supported application patching on managed nodes; it does not own ECS task-set replacement, ALB traffic shifting, application rollout validation, or revision rollback.
Exam trap: Selecting a general operations tool because the scenario says update, while ignoring that the decisive requirements are application-version traffic shifting and rollback.
Service comparison: ECS blue/green deployment / CodeDeploy versus AWS Systems Manager Patch Manager.
Action: Complete one spaced free-response deployment-strategy retest after 2026-07-28 without opening this log first.
Review status: New miss from the 2026-07-21 timed 30-question diagnostic; remediation recorded, spaced recall pending.
```

### 2026-07-23: SCP Versus IAM Permissions Boundary

```text
Question theme: Organization-wide Region restriction for workload accounts with different requirements for security accounts
SAP-C02 domain: Domain 1 - Design Solutions for Organizational Complexity
Question number: 1
My answer: B, C
Correct answer: C, D
My answer pattern: Used an IAM identity-level control where the scenario required an organization-wide preventive restriction.
Correct answer pattern: Apply a Region-restriction SCP to the workload OU and keep security accounts in a separate OU that does not inherit that restriction.
Why correct: An SCP defines the maximum permissions available in affected member accounts and can enforce the restriction across an OU. Separate OU placement preserves a different policy-inheritance boundary for security accounts.
Why my answer was wrong: An IAM permissions boundary limits one IAM user or role; it does not impose the required account-wide or OU-wide preventive control.
Exam trap: Selecting an identity-scoped maximum-permissions control for an organization-scoped requirement.
Service comparison: AWS Organizations SCP versus IAM permissions boundary.
Error category: Scope or policy-evaluation error; multi-select exact-match error.
Action: Continue recurrence monitoring in independent full mocks; retain the role-ARN versus role-session-ARN resource-policy boundary distinction.
Review status: Focused remediation passed 6/6 on 2026-07-24 in 18 minutes, with all three learner-marked uncertain items correct. The attempt preceded the preferred spacing date, so independent-mock transfer evidence remains required.
```

### 2026-07-23: DynamoDB Write Sharding and Read Fan-Out

```text
Question theme: Concentrated writes on a small number of very high-volume DynamoDB partition keys
SAP-C02 domain: Domain 3 - Continuous Improvement for Existing Solutions
Question number: 13
My answer: B, D
Correct answer: A, D
My answer pattern: Treated capacity mode as the repair for poor key distribution and omitted the read consequence of write sharding.
Correct answer pattern: Add a calculated shard suffix to distribute writes across partition keys, then query every relevant shard in parallel and merge and order the results.
Why correct: Shard suffixes spread concentrated writes across partitions, but the original entity's records then span several partition keys and require fan-out reads.
Why my answer was wrong: Changing capacity mode can alter capacity management, but it does not repair a hot-key access pattern or remove the need to distribute writes.
Exam trap: Focusing on provisioned versus on-demand capacity while ignoring partition-key distribution and the resulting read-path trade-off.
Service comparison: DynamoDB key-design write sharding versus capacity-mode selection.
Error category: Missed architectural consequence; multi-select exact-match error.
Action: Reconstruct the complete read path—query every relevant shard, merge the results, then establish cross-shard order—and retain that GSI reads are eventually consistent only; complete the spaced exact-match retest no earlier than 2026-07-28.
Review status: Partial recurrence on diagnostic question 4 on 2026-07-25. The learner selected fan-out query/merge but selected a strongly consistent GSI instead of explicit cross-shard ordering. Spaced retest and independent-mock transfer remain pending.
```

### 2026-07-25: DynamoDB Lifecycle and Recovery Feature Mapping

```text
Question theme: Automatic expiry, asynchronous item-change processing, and restoration after accidental writes
SAP-C02 domain: Domain 2 / Domain 3
Question number: 8
My answer: A, C, D
Correct answer: A, B, C
My answer pattern: Correctly chose TTL and PITR but selected a GSI rather than DynamoDB Streams for reacting to item changes.
Correct answer pattern: Use TTL for asynchronous expiry, DynamoDB Streams for item-change events, and PITR for restoration to an earlier table state.
Why correct: Each capability maps independently to one requirement: data ageing, change capture, and recovery.
Why my answer was wrong: A GSI creates another query access path; it does not publish insert, update, or delete events to a consumer.
Exam trap: Treating every DynamoDB auxiliary feature as an indexing option instead of mapping the requested function precisely.
Service comparison: DynamoDB TTL versus Streams versus PITR versus GSI.
Error category: Feature-mapping knowledge gap; multiple-response requirement-decomposition error.
Action: Reproduce the four-part mapping from memory and complete the six-question spaced retest no earlier than 2026-07-28.
Review status: New gap demonstrated by the 2026-07-25 closed-book diagnostic; learner marked the question uncertain; spaced recall pending.
```

### 2026-07-25: Time-Series History Versus Latest-State Lookup

```text
Question theme: Historical time-window telemetry analysis plus millisecond latest-state lookup by known device ID
SAP-C02 domain: Domain 2 / Domain 3
Question number: 17
My answer: A
Correct answer: A, B
My answer pattern: Correctly selected Timestream for historical analysis but omitted DynamoDB for the separately stated latest-state access pattern.
Correct answer pattern: Use Timestream for timestamp-centred history and time-window queries, and DynamoDB for known-key latest-state retrieval.
Why correct: The two stores serve distinct access patterns rather than forcing operational and analytical requests into one model.
Why my answer was wrong: The answer satisfied only the historical-analysis clause; a Choose TWO response also required a service for the operational latest-state clause.
Exam trap: Stopping after finding one valid service in a multi-requirement, multiple-response question.
Service comparison: Amazon Timestream versus DynamoDB in a purpose-built multi-store design.
Error category: Scenario-decomposition and answer-completeness error; exact-match multiple-response error.
Action: Count required selections and map one selected component to every explicit requirement before submission; complete the spaced retest no earlier than 2026-07-28.
Review status: New gap demonstrated by the 2026-07-25 closed-book diagnostic; not marked uncertain; spaced recall pending.
```
