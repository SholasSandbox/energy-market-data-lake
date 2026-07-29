# AWS Non-Relational Databases — SAP-C02 Key Lessons

<!-- markdownlint-disable MD013 MD060 -->

**Date:** 2026-07-24<br>
**Last revised:** 2026-07-28<br>
**Purpose:** Close the identified revision gap without restarting database study from first principles.<br>
**Evidence boundary:** This is a source-backed lesson, not proof of closed-book recall. Complete the separate diagnostic before recording mastery.

## Learning outcome

By the end of this lesson, you should be able to choose between DynamoDB, an in-memory cache, and the main purpose-built non-relational databases from a scenario's access pattern, consistency, availability, migration, and operational requirements.

Use this decision sequence:

```text
data shape
    -> access pattern
    -> consistency and durability
    -> scale and latency
    -> availability and recovery
    -> migration and operating model
```

Do not begin with a preferred service and force the scenario into it.

## Current SAP-C02 scope

The current SAP-C02 in-scope database list includes:

- Amazon Aurora and Aurora Serverless
- Amazon DocumentDB (with MongoDB compatibility)
- Amazon DynamoDB
- Amazon ElastiCache
- Amazon Keyspaces (for Apache Cassandra)
- Amazon Neptune
- Amazon RDS
- Amazon Redshift
- Amazon Timestream

This lesson concentrates on the non-relational services and the relational/search contrasts most likely to appear as distractors.

Amazon MemoryDB is useful awareness for service-selection questions, but it is not named in the current SAP-C02 in-scope list. Amazon QLDB should not be a revision priority: AWS ended QLDB support on 31 July 2025.

## The rapid service chooser

| Requirement signal | First service to evaluate | Why | Common wrong turn |
| --- | --- | --- | --- |
| Known key-value/document access patterns at very large scale | DynamoDB | Serverless, single-digit millisecond performance, managed scaling | Choosing it for ad hoc joins or flexible relational queries |
| Repeated eventually consistent DynamoDB reads need microsecond latency | DAX | DynamoDB-compatible, write-through cache | Treating DAX as a fix for a hot partition key |
| Rich in-memory structures, replication, failover, pub/sub or sorted sets | ElastiCache for Valkey or Redis OSS | Feature-rich cache with replication and clustering options | Using cache as the only durable system of record |
| Simplest distributed object cache with a multithreaded engine | ElastiCache for Memcached | Simple key-value cache that scales by adding nodes | Expecting Redis-style replication, failover, or data structures |
| MongoDB-compatible document workload or migration | DocumentDB | Managed document database with MongoDB compatibility | Assuming complete MongoDB feature compatibility without testing |
| Cassandra/CQL workload or migration | Keyspaces | Managed, serverless-compatible Cassandra service | Redesigning immediately for DynamoDB when CQL compatibility is required |
| Highly connected data and graph traversals | Neptune | Purpose-built graph database | Modelling multi-hop relationships as repeated DynamoDB scans |
| Time-stamped measurements, retention tiers and time-window analysis | Timestream | Purpose-built serverless time-series database | Using a generic document store for time-series analytics |
| Search, relevance, log exploration and aggregations | OpenSearch Service | Search and analytics engine | Treating it as the authoritative transactional database |

The table identifies the first service to evaluate, not an automatic final answer. SAP-C02 scenarios often add a constraint that changes the winner.

## DynamoDB: access-pattern-first design

### Start with the requests, not the entities

DynamoDB design begins by listing the requests the application must answer:

- Which item or item collection is retrieved?
- Which attributes form the known key?
- Must results be ordered or range-filtered?
- How many reads and writes hit each logical key?
- Is eventual consistency acceptable?
- Is a cross-Region write strategy required?

The partition key determines distribution. A composite primary key adds a sort key, which groups related items and enables ordered or range queries inside one partition-key value.

```text
Known partition key + optional sort-key condition
                       |
                       v
                    Query

No usable key condition
                       |
                       v
                    Scan  -> reads broadly, then filters
```

A filter expression does not turn a Scan into a targeted read. It removes results after DynamoDB has read them, so it does not eliminate the consumed read work.

### Physical partitions, item collections, and hot-key limits

DynamoDB stores table and GSI data in automatically managed physical
partitions. A physical partition is replicated across multiple Availability
Zones and has these high-value design limits:

- up to **10 GB** of storage;
- up to **3,000 read units per second**; and
- up to **1,000 write units per second**.

```text
request partition-key value
          |
          v
internal hash function
          |
          v
automatically managed physical partition(s)
          |
          +-- 10 GB maximum per physical partition
          +-- 3,000 read units/second per partition
          +-- 1,000 write units/second per partition
```

These are partition-level ceilings, not promises that every logical key always
occupies exactly one physical partition forever. With a composite primary key,
items sharing a partition-key value form an **item collection** and are ordered
by sort key. If the table has no LSI, DynamoDB can split a large or busy item
collection across partitions. If the table has an LSI, that item collection
cannot be split and the combined base-table plus LSI data for the partition-key
value is limited to 10 GB.

Adaptive capacity can direct more of the table's available throughput to a hot
partition and can isolate frequently accessed items. It cannot exceed the
physical partition ceiling for one isolated item. Continue to design for high-
cardinality, well-distributed keys rather than relying on adaptive capacity as
the primary scaling strategy.

Sort-key conditions make the item collection useful for range access. Common
conditions include equality, `<`, `<=`, `>`, `>=`, `BETWEEN`, and
`begins_with`. A Query requires the partition-key value and may add one of
these sort-key conditions.

### GSI versus LSI

| Dimension | Global secondary index (GSI) | Local secondary index (LSI) |
| --- | --- | --- |
| Partition key | May differ from the base table | Same as the base table |
| Sort key | Optional and may differ | Must differ from the base table sort key |
| Creation | Can be added after table creation | Must be defined when the table is created |
| Read consistency | Eventually consistent only | Eventual or strongly consistent |
| Scaling/capacity | Separate partition structure; separate provisioned throughput when using provisioned mode | Shares base-table throughput and partition-key locality |
| Item-collection size | No LSI-style 10 GB item-collection limit | 10 GB per partition-key item collection, including projected LSI data |
| Main use | A new access pattern across the table | Another ordering/range view within the same item collection |

Two exam traps follow from this:

1. If the question requires a strongly consistent alternate-key read, a GSI cannot provide it.
2. A low-cardinality GSI partition key can become hot even when the base table key is well distributed.
3. When a table has an LSI, each partition-key item collection—including its
   index entries—has a 10 GB size limit.

### GSI backpressure: diagnose before changing capacity

Base-table writes asynchronously update every affected GSI. If an index cannot
accept those updates, DynamoDB can throttle the **base-table write** even when
the base table itself has spare capacity. Inspect the throttling reason and the
resource ARN before choosing the remedy:

| Throttling reason | Root cause | Appropriate response |
| --- | --- | --- |
| `IndexWriteProvisionedThroughputExceeded` | The provisioned GSI write capacity is insufficient | Increase or auto scale the GSI WCU; for provisioned designs, keep index write capacity at least aligned with the base-table write demand; consider on-demand for unpredictable volume |
| `IndexWriteMaxOnDemandThroughputExceeded` | A configured maximum for the on-demand GSI was reached | Raise or remove the configured maximum after checking cost and account limits |
| `IndexWriteKeyRangeThroughputExceeded` | One GSI key range is hot | Redesign the GSI partition key or shard the index-writing pattern |

The third case is the exam trap: switching to on-demand or adding total WCU
does not fix a low-cardinality GSI key such as `status=ACTIVE`. Table and GSI
key distributions are independent, so a well-distributed base table can still
produce a hot index.

### Capacity mode does not repair a poor key

On-demand capacity is valuable for unpredictable traffic, and provisioned capacity with auto scaling is useful for more predictable demand and cost control. Neither changes how one partition-key value concentrates traffic.

```text
high request volume
        |
        +-- spread across many key values -> capacity mode can scale effectively
        |
        +-- concentrated on one key value -> hot-key design problem remains
```

Adaptive capacity helps uneven workloads, but it is not permission to use an indefinitely hot key.

### Capacity-unit calculations

Provisioned tables use RCUs and WCUs; on-demand tables use equivalently sized
read and write request units. Round item size up before multiplying by the
operation rate.

| Operation | Units for one operation per second |
| --- | ---: |
| Standard write, item up to 1 KB | 1 WCU |
| Transactional write, item up to 1 KB | 2 WCUs |
| Strongly consistent read, item up to 4 KB | 1 RCU |
| Eventually consistent read, item up to 4 KB | 0.5 RCU |
| Transactional read, item up to 4 KB | 2 RCUs |

```text
standard WCU/second
  = writes/second × ceil(item size / 1 KB)

strong RCU/second
  = reads/second × ceil(item size / 4 KB)

eventual RCU/second
  = reads/second × ceil(item size / 4 KB) × 0.5

transactional reads or writes
  = corresponding standard strong-read/write units × 2
```

For Query and Scan, capacity is based on the data read before a filter removes
items, not merely the smaller result returned to the caller.

Use provisioned capacity with auto scaling for steady, predictable workloads
where capacity control and cost optimisation matter. Use on-demand for new,
variable, or unpredictable workloads. On-demand can instantly accommodate up
to twice the table's previous peak throughput, so it should not be modelled as
unlimited instantaneous scaling. Neither mode removes per-partition or hot-key
limits.

### Write sharding and its read cost

When a naturally low-cardinality key would concentrate writes, add a suffix to distribute them:

```text
meter#GB
   |
   +-- meter#GB#0
   +-- meter#GB#1
   +-- meter#GB#2
   +-- meter#GB#3
```

- A random suffix maximizes distribution but normally requires reads to query all shards and merge the results.
- A calculated suffix, derived from a known attribute, may let a reader identify one shard when that attribute is known.
- More shards improve write distribution but increase read fan-out and aggregation complexity.

The full write-sharding, fan-out, and hot-GSI model is already captured in [SAP-C02 Hidden-Gap Model Review](sap-c02-hidden-gap-model-review-20260725.md). Reuse that model; do not create a second competing version.

### Correctness controls

| Requirement | DynamoDB mechanism |
| --- | --- |
| Create an item only if it does not already exist | Conditional write, such as `attribute_not_exists` |
| Prevent a stale client from overwriting a newer version | Version attribute plus conditional update |
| Increment a numeric value without a read-before-write race | Atomic update expression such as `ADD` or `SET counter = counter + :value` |
| Apply all-or-nothing changes across multiple items | DynamoDB transactions |
| Process an item change asynchronously | DynamoDB Streams plus a consumer |
| Expire old items without an application delete job | Time to Live (TTL) |
| Restore a table to a point within the recovery window | Point-in-time recovery (PITR) |

TTL deletion is asynchronous; do not promise deletion at the exact expiry second. Streams provide an ordered sequence of item-level changes within the relevant stream partition, not a relational query engine or a way to redistribute writes.

Conditional writes support optimistic concurrency and idempotent creation, but
the application must handle `ConditionalCheckFailedException`. Atomic counters
avoid a read-before-write race, yet a retried non-idempotent increment can be
applied twice; use an idempotency or deduplication design when duplicate
delivery is possible.

### Read consistency

| Read surface | Eventual read | Strong read |
| --- | --- | --- |
| Base table | Yes | Yes |
| LSI | Yes | Yes |
| GSI | Yes | No |
| DynamoDB Streams | Yes | No |

Eventually consistent reads can be cheaper and scale well when a brief delay is acceptable. Use strongly consistent reads only where the business invariant actually requires the latest committed value.

### Global tables: MREC versus MRSC

DynamoDB global tables now have two consistency modes. Older notes that describe all global-table replication as asynchronous are incomplete.

| Dimension | Multi-Region eventual consistency (MREC) | Multi-Region strong consistency (MRSC) |
| --- | --- | --- |
| Replication | Asynchronous, normally within seconds | Synchronous to another Region before success |
| Strong read against a replica | Valid on the replica table, but guarantees only the latest locally committed value; it can still be stale relative to a recent write in another Region | A strong read on any available replica returns the latest committed item across the MRSC global table |
| Write/read latency | Lower | Higher because of cross-Region coordination |
| Recovery point potential | Non-zero replication lag is possible | RPO zero is possible for supported designs |
| Feature constraints | Broader feature compatibility | Has additional topology and feature restrictions |
| Best fit | Active-active applications that tolerate brief convergence | Cross-Region applications whose correctness requires strong consistency |

MREC resolves concurrent updates to the same item by **last writer wins**, using
internal write timestamps. Its replication latency is normally low but has no
latency SLA, so do not promise a fixed sub-second RPO.

Current Streams behaviour is mode-specific:

- MREC uses Streams for replication; Streams are enabled automatically on its
  replicas and cannot be disabled.
- MRSC does not use Streams for replication; Streams are not enabled by
  default, although they can be enabled for an application consumer.

Therefore “manually enable `NEW_AND_OLD_IMAGES` before creating any global
table” is not a current universal prerequisite. Also retain that MREC
transactions are atomic only in the Region where invoked; MRSC does not support
transaction APIs or TTL. For MRSC, check the current Region, topology, and
feature restrictions during design. The exam trade remains: stronger cross-
Region correctness costs latency and flexibility.

Do not memorize that a strong read against an MREC replica fails with
`ValidationException`. The replica is a table and accepts a strongly consistent
table read; the limitation is that asynchronous cross-Region replication can
leave the local replica behind a recent remote write. `ValidationException` is
the familiar result when strong consistency is requested on an unsupported
surface such as a GSI—not merely because a table is a global-table replica.

### DAX: a narrow optimisation

DynamoDB Accelerator (DAX) is a managed, in-memory, DynamoDB-compatible cache.
The application changes from the DynamoDB client to a DAX client and cluster
endpoint; this is normally a small integration change, not literally zero
application change.

Choose DAX when:

- the application repeatedly reads the same DynamoDB items;
- eventually consistent reads are acceptable; and
- microsecond read latency materially helps.

Do not choose DAX to:

- repair a hot partition key;
- accelerate write-heavy traffic with no repeated reads;
- provide strongly consistent cached reads; or
- replace DynamoDB durability.

DAX has two distinct caches:

- the **item cache** stores `GetItem` and `BatchGetItem` results by primary key;
- the **query cache** stores `Query` and `Scan` result sets by request
  parameters.

```text
eventually consistent read
application -> DAX hit -> microsecond response
                  |
                  +-> miss -> DynamoDB -> populate DAX -> response

write through DAX
application -> DAX -> DynamoDB succeeds -> update DAX item cache -> success
```

Strongly consistent and transactional reads pass through to DynamoDB and are
not cached. Writes sent through DAX update the item cache after DynamoDB
succeeds, but they do not immediately invalidate matching query-cache result
sets; query-cache TTL therefore remains a staleness decision. DAX is strongest
for repeated eventually consistent reads, not write-heavy workloads.

### DynamoDB scenario decision matrix

| Scenario signal | First decision |
| --- | --- |
| Repeated eventually consistent DynamoDB reads need microsecond latency | Add DAX and use the DAX client |
| Base-table writes are throttled with `IndexWriteProvisionedThroughputExceeded` | Increase/auto scale GSI WCU or evaluate on-demand capacity |
| Base-table writes are throttled with `IndexWriteKeyRangeThroughputExceeded` | Redesign or shard the hot GSI key; total capacity alone will not fix it |
| Continuous recovery from accidental writes | Enable PITR with a configured 1–35 day recovery period |
| Prevent lost updates or duplicate creation | Use conditional expressions and version/idempotency attributes |
| Multi-Region writes tolerate convergence and possible replication lag | Use an MREC global table and understand last-writer-wins conflict resolution |
| Multi-Region reads require the latest committed value and RPO zero | Evaluate MRSC and accept its latency/topology/feature constraints |

## In-memory choices: DAX, Valkey/Redis OSS, Memcached, MemoryDB

### The durable system of record remains explicit

```text
application
    |
    +-- cache hit  -> return cached value
    |
    +-- cache miss -> read durable database
                         |
                         +-> populate cache
```

This common cache-aside pattern improves latency and reduces database load. The design still needs expiry, invalidation, cache-miss, and failure behaviour. Unless the scenario explicitly chooses a durable in-memory database, a cache should not be the only copy of business data.

### Cache/service comparison

| Requirement | DAX | ElastiCache for Valkey/Redis OSS | ElastiCache for Memcached | MemoryDB awareness |
| --- | --- | --- | --- | --- |
| Primary relationship | DynamoDB-specific cache | General-purpose cache/data structures | General-purpose object cache | Durable Valkey-compatible in-memory database |
| API | DynamoDB-compatible client | Valkey/Redis commands | Memcached protocol | Valkey-compatible commands |
| Data structures | DynamoDB items/query results | Strings, hashes, lists, sets, sorted sets and more | Simple key-value objects | Rich Valkey structures |
| Replication/failover | Managed cluster | Replication groups and Multi-AZ failover | No Redis-style native replication/failover | Multi-AZ durable database design |
| Horizontal partitioning | Managed DAX cluster behaviour | Cluster mode sharding | Add nodes and client-side distribution | Managed sharding/replication options |
| Strong-read cache | No; strong reads bypass cache | Application-defined behaviour | Application-defined behaviour | Database consistency depends on service semantics |
| Best clue | Repeated DynamoDB reads | Rich cache features or HA | Simple, multithreaded ephemeral object cache | Durable in-memory primary database explicitly required |

### Valkey/Redis OSS versus Memcached

Choose ElastiCache for Valkey or Redis OSS when the scenario needs one or more of:

- read replicas and automatic failover;
- Multi-AZ resilience;
- rich data structures;
- pub/sub, counters, sorted sets, or atomic operations;
- cluster-mode sharding.

Choose Memcached when:

- the requirement is a simple ephemeral object cache;
- a multithreaded engine is valuable;
- scaling by adding/removing cache nodes is acceptable; and
- native replication, failover, persistence, and rich structures are not required.

The phrase “simplest possible cache” is not enough by itself. Check whether the scenario also requires Multi-AZ automatic failover or replicas; if it does, Valkey/Redis OSS is normally the better answer.

### Cache deployment and Region choices

| Requirement | First design option to evaluate |
| --- | --- |
| Variable or unpredictable cache demand with minimal capacity planning | ElastiCache Serverless |
| Explicit control of node types, replicas, shards and maintenance behaviour | Node-based ElastiCache |
| High availability in one Region | Valkey/Redis OSS replication group with replicas and Multi-AZ automatic failover |
| Dataset larger than one node or high aggregate throughput | Valkey/Redis OSS cluster mode with multiple shards |
| Cross-Region local reads and disaster-recovery copy | ElastiCache Global Datastore for Valkey/Redis OSS |

Global Datastore uses asynchronous cross-Region replication from a primary
Region to secondary clusters. Treat the secondary as a read/local-recovery
copy, not as a multi-Region write architecture. Promotion is a recovery action,
so the design must consider replication lag and failover operations.

## Purpose-built non-relational databases

### Amazon DocumentDB

DocumentDB is the first service to evaluate for a managed JSON-like document workload that requires MongoDB compatibility.

Good signals:

- migration from MongoDB with limited application change;
- nested document structures;
- queries over document fields; and
- managed replication, backups, and scaling.

Important boundary: compatibility is not identity. Assess supported MongoDB APIs, indexes, drivers, and behavioural differences before a migration. DynamoDB remains stronger when the workload has known key-based access patterns and needs serverless scale; DocumentDB is often a better fit when document query compatibility drives the decision.

### Amazon Keyspaces

Keyspaces is the first service to evaluate for Cassandra-compatible workloads using CQL.

Good signals:

- an existing Cassandra data model or client estate;
- CQL compatibility is a migration constraint;
- wide-column access patterns; and
- a desire to remove cluster management.

Do not choose it merely because the data is “NoSQL.” If Cassandra/CQL compatibility is not required, compare the access pattern directly with DynamoDB and the other purpose-built services.

### Amazon Neptune

Neptune is designed for graph workloads where relationships and multi-hop traversals are central.

Good signals:

- fraud-ring and identity-link analysis;
- knowledge graphs;
- social or recommendation relationships; and
- network/dependency topology.

A DynamoDB adjacency-list pattern can serve known graph-like access patterns, but Neptune becomes compelling when flexible traversal across many relationships is the core query.

### Amazon Timestream

Timestream is purpose-built for time-series data.

Good signals:

- measurements are naturally timestamped;
- the workload queries time windows and trends;
- recent and historical data need different retention/storage treatment; and
- ingestion scale and time-series functions matter.

Typical examples include IoT telemetry, application metrics, and market or operational measurements. DynamoDB may still be appropriate for key-based latest-state lookups, while Timestream serves time-window analysis; one architecture may use both for different access patterns.

### OpenSearch as a distractor and companion

OpenSearch Service is the right first choice for full-text search, relevance ranking, faceting, and exploratory log analytics. It is commonly a derived search index fed from an authoritative store.

Do not choose OpenSearch solely because a scenario says “JSON documents,” and do not make it the transactional system of record when the requirement is document durability and database semantics.

## Composite architecture patterns

SAP-C02 questions often test a combination rather than one database.

### Latest state plus historical analysis

```text
ingestion
   |
   +-> DynamoDB: latest state and keyed operational lookup
   |
   +-> Timestream or data lake: history and time-window analytics
```

### Durable store plus low-latency cache

```text
application -> ElastiCache -> durable database
                   |              |
              transient copy   source of truth
```

### Transactional document store plus search

```text
application -> DocumentDB -> change/integration path -> OpenSearch
                    |                                  |
              source of truth                    search index
```

The architecture should state which store owns the truth and how derived stores are rebuilt or reconciled.

## High-value exam traps

1. **On-demand solves a hot key.** It does not; redesign the key or shard the workload.
2. **A filter makes Scan efficient.** It filters after reading; design a key/index for the access pattern.
3. **A GSI supports strong reads.** It does not.
4. **An LSI can be added later.** It must be created with the table.
5. **DAX accelerates strong reads.** Strong reads bypass the cache.
6. **DAX redistributes writes.** It does not change partition-key distribution.
7. **All global tables are eventually consistent.** Current global tables also support MRSC, subject to restrictions.
8. **Memcached provides Redis-style replicas and automatic failover.** It does not.
9. **A cache is automatically durable.** ElastiCache is normally a performance layer, not the only system of record.
10. **Every JSON workload means DocumentDB.** DynamoDB may be better for known key access; OpenSearch may be better for search.
11. **Every relationship can be handled with a key-value store.** Deep, flexible graph traversal points to Neptune.
12. **Every timestamped item needs Timestream.** Choose it when time-window ingestion, retention, and analysis are central—not merely because an item has a timestamp.
13. **Any GSI backpressure means add WCU.** Inspect the throttling reason: insufficient provisioned capacity and a hot GSI key require different remedies.
14. **DAX requires zero application change.** It minimizes query-model change, but the application must use a DAX client and endpoint.
15. **Every global table requires manually enabled `NEW_AND_OLD_IMAGES` Streams.** MREC manages Streams for replication; MRSC replication does not use Streams.
16. **A strong read on an MREC secondary replica throws `ValidationException`.** It is a valid local table read, but it cannot make asynchronous cross-Region replication current; GSIs remain eventual-only.

## Bounded revision route

This is a gap-closing pass, not a fresh database course.

1. **35 minutes — DynamoDB:** reconstruct physical limits, capacity arithmetic, primary keys, GSI/LSI and backpressure, consistency, hot keys, sharding, concurrency, DAX, and MREC/MRSC.
2. **20 minutes — caching:** reproduce the DAX versus Valkey/Redis OSS versus Memcached table from memory.
3. **20 minutes — purpose-built selection:** explain the one decisive trigger for DocumentDB, Keyspaces, Neptune, Timestream, and OpenSearch.
4. **15 minutes — closed-book reconstruction:** complete the checklist below without looking up the lesson.
5. **40 minutes — blind diagnostic:** complete the separate 18-question diagnostic and freeze the submission.

## Closed-book reconstruction checklist

Before attempting the diagnostic, close this file and answer aloud or on paper:

- Why can on-demand capacity still throttle around a poor partition key?
- What are the 10 GB, 3,000-read-unit, and 1,000-write-unit physical-partition limits, and when can an item collection split?
- When does a GSI beat an LSI, and which one can provide a strong read?
- How do `IndexWriteProvisionedThroughputExceeded` and `IndexWriteKeyRangeThroughputExceeded` lead to different repairs?
- How many units are required for standard, eventual, strong, and transactional reads/writes after size rounding?
- What read penalty follows random write sharding?
- What do conditional writes solve?
- Why is an atomic counter not automatically idempotent?
- Which DynamoDB read surfaces are eventual-only?
- What changed in the global-tables model with MRSC?
- How does Streams behaviour differ between MREC and MRSC?
- Why does DAX not solve write concentration?
- Which requirement moves a cache choice from Memcached to Valkey/Redis OSS?
- Which service is the first choice for MongoDB compatibility? Cassandra/CQL? Graph traversal? Time series?
- In a multi-store design, which service owns the durable truth?

If any answer is vague, revisit only that subsection before attempting the diagnostic.

## Official references

- [SAP-C02 in-scope AWS services](https://docs.aws.amazon.com/aws-certification/latest/solutions-architect-professional-02/sap-02-in-scope-services.html)
- [AWS Certified Solutions Architect - Professional exam guide](https://docs.aws.amazon.com/aws-certification/latest/solutions-architect-professional-02/solutions-architect-professional-02.html)
- [Choosing an AWS database service](https://docs.aws.amazon.com/databases-on-aws-how-to-choose/)
- [AWS NoSQL database decision guide](https://docs.aws.amazon.com/whitepapers/latest/choosing-an-aws-nosql-database/decision-making.html)
- [DynamoDB read consistency](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html)
- [DynamoDB partitions and data distribution](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.Partitions.html)
- [DynamoDB constraints and capacity-unit sizes](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Constraints.html)
- [DynamoDB partition-key design and per-partition throughput](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-design.html)
- [DynamoDB GSI write throttling and backpressure](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/gsi-throttling.html)
- [Choosing DynamoDB capacity mode](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CostOptimization_TableCapacityMode.html)
- [DynamoDB global table consistency modes](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-global-table-design.html)
- [DAX consistency behaviour](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DAX.consistency.html)
- [DAX item and query caches](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DAX.concepts.html)
- [Choosing an ElastiCache engine](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/SelectEngine.html)
- [Valkey/Redis OSS replication groups](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/Replication.Redis.Groups.html)
- [Scaling ElastiCache Serverless](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/Scaling-serverless.html)
- [ElastiCache Global Datastore for Valkey or Redis OSS](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/Redis-Global-Datastores-Console.html)
- [Amazon QLDB end-of-support notice](https://docs.aws.amazon.com/qldb/latest/developerguide/getting-started-step-7.html)
