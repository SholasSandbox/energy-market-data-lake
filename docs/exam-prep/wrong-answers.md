# SAP-C02 Wrong-Answer Log

This log is the durable exam-prep companion to the tracker wrong-answer table.
It is seeded from the Lakehouse repository tracker and is not Lakehouse
implementation evidence from any external tutorial workspace.

## Review Status

| Item | Status |
|---|---|
| Current through practice block | 006 |
| First review cycle evidenced | Not yet |
| Second review cycle evidenced | Not yet |
| Controlling tracker reference | `docs/planning/sap-c02-readiness-tracker.md` |

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
Review status: Open.
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
Review status: Open.
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
Review status: Open.
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
Review status: Open.
```
