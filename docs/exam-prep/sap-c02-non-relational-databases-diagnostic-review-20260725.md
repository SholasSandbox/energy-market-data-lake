# SAP-C02 Non-Relational Databases Diagnostic Review - 2026-07-25

<!-- markdownlint-disable MD013 MD060 -->

**Last revised:** 2026-07-25

## Result

| Field | Result |
|---|---|
| Questions | 18 |
| Exact-match score | **15/18 (83.3%)** |
| Start / end | 17:20 / 18:00 |
| Elapsed time | 40 minutes |
| Single-choice | **9/9 (100%)** |
| Multiple-response | **6/9 (66.7%)** |
| Learner-marked uncertainty | Questions 8, 10, and 18 |
| Uncertain answers correct | **2/3**: questions 10 and 18 |
| Misses | Questions 4, 8, and 17 |

The learner froze the answer block before marking. Compliance with the
closed-book instruction is learner-attested and is not independently
observable. The attempt met the 40-minute target exactly.

This is a strong bounded-topic pass, but it does not close the gap completely.
All three misses were exact-match multiple-response failures. Question 4 is a
partial recurrence of the DynamoDB write-shard read-path trap from full mock
001; questions 8 and 17 reveal narrower feature-mapping and scenario-
decomposition gaps.

## Exact-Match Marking

| Question | Learner answer | Correct answer | Result |
|---:|---|---|---|
| 1 | B | B | Correct |
| 2 | A, D | A, D | Correct |
| 3 | C | C | Correct |
| 4 | A, B | A, C | **Incorrect** |
| 5 | B | B | Correct |
| 6 | A, B | A, B | Correct |
| 7 | A | A | Correct |
| 8 | A, C, D | A, B, C | **Incorrect** |
| 9 | B | B | Correct |
| 10 | A, C | A, C | Correct |
| 11 | B | B | Correct |
| 12 | A, C | A, C | Correct |
| 13 | A, C | A, C | Correct |
| 14 | A | A | Correct |
| 15 | B | B | Correct |
| 16 | A | A | Correct |
| 17 | A | A, B | **Incorrect** |
| 18 | A, B, C | A, B, C | Correct |

## Question-by-Question Review

### 1 - DynamoDB access path

**B.** A partition key made from participant and date targets one relevant item
collection, while settlement interval as the sort key provides the required
ordering. DAX or on-demand capacity would not convert a Scan into a key-based
Query, and exporting before every operational read is unnecessary.

### 2 - GSI and LSI constraints

**A and D.** A GSI may use `status` as a different partition key and can be
added after table creation. An LSI retains the base-table partition key, can
provide a different sort order inside that item collection, and supports
strongly consistent reads. GSIs are eventually consistent only; LSIs must be
created with the table.

### 3 - Concentrated writes

**C.** Random or calculated suffixes expand the partition-key space and spread
the writes. DAX, Streams, and a billing-mode change do not repair traffic
concentrated on one logical key.

### 4 - Write-shard read path

**A and C.** Random sharding means the application must query the relevant
shard keys, then merge and order the returned result sets. The learner selected
the fan-out/merge action but replaced the necessary cross-shard ordering with a
strongly consistent GSI read.

Option B fails twice: a GSI supports only eventually consistent reads, and an
index does not automatically reconstruct global timestamp order across the
randomly sharded partition-key values. A Scan would abandon the targeted
access pattern, and DAX does not infer an application's sharding scheme.

**Classification:** recurring architectural-consequence and service-capability
error; exact-match multiple-response error.

### 5 - Filter expressions

**B.** A key or index that supports Query avoids the broad read. A filter is
applied after DynamoDB reads the candidate items, so increasing filter
complexity or parallelizing a Scan does not remove the underlying read work.

### 6 - Idempotent ingestion

**A and B.** `attribute_not_exists` prevents a duplicate initial write, while a
version-based condition prevents a stale transition from overwriting newer
state. TTL, DAX, and capacity mode solve different problems.

### 7 - DAX suitability

**A.** DAX is designed for repeated DynamoDB reads where eventual consistency
is acceptable and microsecond latency is valuable. Strong reads bypass the DAX
cache, and neither sharding nor Neptune matches the stated read optimisation.

### 8 - DynamoDB lifecycle and recovery

**A, B, and C.** TTL expires old items asynchronously, DynamoDB Streams emits
item-change records for asynchronous consumers, and PITR provides continuous
backup recovery points for restoration after accidental writes.

The learner correctly selected TTL and PITR but selected a GSI instead of
Streams. A GSI supports another query access pattern; it does not emit change
events. The uncertainty flag was well placed, but the three requirements were
not mapped independently to three features.

**Classification:** feature-mapping knowledge gap plus multiple-response
requirement-decomposition error.

### 9 - Cross-Region correctness

**B.** MRSC is the global-table mode that can provide strongly consistent reads
across participating Regions, accepting the additional coordination latency
and feature restrictions. MREC can briefly expose an older value.

### 10 - MREC trade-off

**A and C.** MREC uses asynchronous replication, so replication lag and a
non-zero recovery point are possible. The selected answer was correct despite
the learner's uncertainty.

### 11 - Highly available feature-rich cache

**B.** ElastiCache for Valkey or Redis OSS supplies rich data structures,
replication groups, read replicas, Multi-AZ failover, and sharding options.
Memcached is the simpler ephemeral object-cache engine.

### 12 - Memcached fit

**A and C.** Memcached fits a simple, rebuildable, multithreaded object cache
and can scale by adding nodes with suitable client distribution. It does not
provide Valkey/Redis-style replication groups or become a durable source of
truth.

### 13 - Cache failure design

**A and C.** Correctness comes from falling back to the durable database.
Expiry/invalidation and controlled repopulation reduce stale data and cache-
stampede risk. A cache-only truth or uncontrolled database surge would weaken
the design.

### 14 - MongoDB-compatible migration

**A.** DocumentDB is the first service to evaluate for a managed document
database where MongoDB compatibility and limited application change drive the
decision. Compatibility still requires workload testing.

### 15 - Cassandra-compatible migration

**B.** Keyspaces retains the Cassandra/CQL-oriented access model while removing
self-managed cluster operations. A general NoSQL label is not enough to make
DynamoDB the lower-change migration.

### 16 - Relationship traversal

**A.** Neptune is purpose-built for flexible multi-hop graph traversal across
highly connected data. Memcached and Timestream solve cache and time-series
requirements, respectively.

### 17 - Time-series selection

**A and B.** Timestream owns the timestamp-centred history and time-window
analysis. DynamoDB separately serves the known-key, millisecond latest-state
lookup by device ID.

The learner identified the historical store but omitted the service required
by the second access pattern. Because the question explicitly requested a
two-service design, selecting only A could never be an exact match.

**Classification:** scenario-decomposition and answer-completeness error rather
than broad service-selection ignorance.

### 18 - Authoritative store and derived search

**A, B, and C.** DocumentDB is the durable MongoDB-compatible document store,
OpenSearch is a rebuildable search index, and ElastiCache is a rebuildable
acceleration layer. The selected answer was correct despite the uncertainty.

## Demonstrated Gap Register

| Question | Demonstrated gap | Durable rule | Action |
|---:|---|---|---|
| 4 | Random write-shard read path and GSI consistency | Query every relevant shard, merge the results, then establish cross-shard order; a GSI is eventual-only and does not perform the merge | Complete the spaced exact-match retest and monitor recurrence in full mock 002 |
| 8 | DynamoDB lifecycle/recovery feature mapping | TTL ages data out; Streams reacts to change; PITR rewinds table state; a GSI provides another query path | Reconstruct the three-feature mapping closed book before the retest |
| 17 | Separating history from latest-state access | Timestream serves time-window history; DynamoDB serves known-key latest state | Decompose every clause in a multiple-response scenario and map one selected component to each requirement |

## Uncertainty and Calibration

- Marked uncertain and incorrect: question 8.
- Marked uncertain and correct: questions 10 and 18.
- Not marked uncertain but incorrect: questions 4 and 17.

The main calibration concern is not excessive uncertainty; it is two
unflagged incomplete exact-match answers. Before submitting a `Choose TWO` or
`Choose THREE` response, count the selected options and confirm that each
stated requirement has a mapped answer.

## Three Mental Models to Retain

```text
random write shards
    -> query each relevant shard
    -> merge results
    -> establish cross-shard order

TTL     -> age data out
Streams -> react to item changes
PITR    -> restore earlier table state
GSI     -> support another query path

time-window history -> Timestream
known-key latest state -> DynamoDB
```

## Retest Decision

The recurring sharding consequence plus two adjacent exact-match gaps warrants
a short, fresh retest. Use
[the six-question question-only retest](sap-c02-non-relational-databases-spaced-retest-6q-20260728.md)
no earlier than **2026-07-28**. Keep this review closed during that attempt.
The retest is not a replacement for full mock 002.

## Official AWS References

- [DynamoDB write sharding](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-sharding.html)
- [DynamoDB read consistency](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html)
- [DynamoDB Streams](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.CoreComponents.html#HowItWorks.CoreComponents.Streams)
- [DynamoDB TTL](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html)
- [DynamoDB point-in-time recovery](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Point-in-time-recovery.html)
- [Choosing an AWS database service](https://docs.aws.amazon.com/databases-on-aws-how-to-choose/)
