# SAP-C02 Non-Relational Databases — Closed-Book Diagnostic (18 Questions)

<!-- markdownlint-disable MD013 MD060 -->

**Date prepared:** 2026-07-24<br>
**Last revised:** 2026-07-25<br>
**Status:** Frozen learner submission; the answer key is isolated in the separate review.<br>
**Purpose:** Test transfer across DynamoDB, caching, and purpose-built non-relational database decisions after the bounded lesson.

## Attempt rules

- Complete closed book.
- Target time: **40 minutes**.
- Unless the question says otherwise, choose the single best answer.
- For `Choose TWO` and `Choose THREE`, exact-match scoring applies: every required option and no extra options.
- Do not research or inspect the lesson during the attempt.
- Record start time, end time, and one answer per line in the frozen-submission block.
- Do not edit the answer block after declaring the submission frozen. Corrections belong in the later review.

## Questions

### 1. DynamoDB access path

An electricity-market platform stores settlement records in DynamoDB. The application must retrieve all records for a known participant and settlement date, ordered by settlement interval. The table currently uses a randomly generated record ID as its only partition key, so the application scans the table and filters the results.

Which redesign is the most appropriate?

A. Retain the random record ID and enable DynamoDB Accelerator (DAX).<br>
B. Use participant-and-date as the partition key and settlement interval as the sort key.<br>
C. Retain the key and change the table from provisioned capacity to on-demand capacity.<br>
D. Export the table to Amazon S3 before each query and use Amazon Athena.

### 2. GSI and LSI constraints — Choose TWO

A team has a DynamoDB table with `customerId` as the partition key and `orderTime` as the sort key. It needs a new access pattern that retrieves orders by `status` across all customers. It also needs strongly consistent reads for an alternate ordering within one customer's item collection.

Which statements correctly guide the design? Choose TWO.

A. A GSI can use `status` as a different partition key and can be added to the existing table.<br>
B. A GSI can provide strongly consistent reads when the caller sets `ConsistentRead=true`.<br>
C. An LSI can use `status` as a different partition key across all customers.<br>
D. An LSI retains `customerId` as the partition key and can support strongly consistent reads.<br>
E. An LSI can be added to the existing table without recreating or migrating it.

### 3. Concentrated writes

A telemetry table receives 80 percent of its writes under the partition-key value `region#eu-west-2`. The table uses on-demand capacity. During bursts, requests for that key are throttled even though total table traffic appears acceptable.

Which change most directly addresses the root cause?

A. Add DAX in front of the table.<br>
B. Change to provisioned capacity with target-tracking auto scaling.<br>
C. Add a calculated or random suffix to distribute writes across more partition-key values.<br>
D. Add a DynamoDB Stream and process it with AWS Lambda.

### 4. Write-shard read path — Choose TWO

A team changes a hot key from `market#GB` to `market#GB#0` through `market#GB#15` using a random suffix. The application must return all events for `market#GB` in timestamp order.

Which two consequences must the read design handle? Choose TWO.

A. Query the relevant shard keys and merge the result sets.<br>
B. Use a strongly consistent read against a GSI to reconstruct global order.<br>
C. Sort or otherwise reconcile results across shards.<br>
D. Replace all queries with one table Scan using a filter expression.<br>
E. Expect DAX to identify the correct shard automatically.

### 5. Filter expressions

A DynamoDB Scan reads 10 GB of items and a filter expression returns only 1 percent of them. The team wants the lowest-latency and lowest-read-work design for a frequent request.

What should a solutions architect recommend?

A. Increase the filter expression complexity so DynamoDB eliminates items before reading them.<br>
B. Model a primary key or secondary index that supports a targeted Query.<br>
C. Use a parallel Scan because it consumes no read capacity when filters reject items.<br>
D. Enable strongly consistent Scan operations.

### 6. Idempotent ingestion — Choose TWO

An at-least-once event source can deliver the same market trade more than once. A Lambda function writes each trade to DynamoDB. Duplicate trades must not overwrite the original, and a later state transition must not replace a version written by a newer process.

Which two controls best satisfy these requirements? Choose TWO.

A. Use `attribute_not_exists` in a conditional put for the initial trade.<br>
B. Use a version attribute in a conditional update for later transitions.<br>
C. Enable TTL on the table.<br>
D. Put DAX in front of the write path.<br>
E. Change the table to on-demand capacity.

### 7. DAX suitability

A product catalogue uses DynamoDB. Ninety percent of traffic consists of repeated, eventually consistent reads of a small set of items. The business requires microsecond response times and minimal application change.

Which solution is the best fit?

A. Add a DynamoDB Accelerator cluster and use a DAX-compatible client.<br>
B. Add a write-sharding suffix to every catalogue key.<br>
C. Send strongly consistent reads through DAX.<br>
D. Replace DynamoDB with Amazon Neptune.

### 8. DynamoDB lifecycle and recovery — Choose THREE

A compliance team requires old session items to expire automatically, an asynchronous processor to react to item changes, and the ability to restore the table to an earlier point after accidental writes.

Which three capabilities address the requirements? Choose THREE.

A. Time to Live (TTL)<br>
B. DynamoDB Streams<br>
C. Point-in-time recovery (PITR)<br>
D. A global secondary index<br>
E. DynamoDB Accelerator<br>
F. A filter expression

### 9. Cross-Region correctness

A payment-token lookup must remain writable across Regions. After a successful write, a read in another participating Region must be able to return the latest committed value. The business accepts additional cross-Region latency and can operate within the feature restrictions of the consistency mode.

Which DynamoDB design best matches the requirement?

A. A global table using multi-Region eventual consistency (MREC)<br>
B. A global table using multi-Region strong consistency (MRSC)<br>
C. A single-Region table with DAX in each Region<br>
D. A table with a strongly consistent GSI replicated by DynamoDB Streams

### 10. MREC trade-off — Choose TWO

A globally distributed application tolerates a brief period in which readers in another Region see an older value. It prioritizes lower latency and broad DynamoDB feature compatibility over RPO zero.

Which two statements support choosing MREC? Choose TWO.

A. Replication between Regions is asynchronous.<br>
B. Every cross-Region read is strongly consistent.<br>
C. Replication lag can produce a non-zero recovery point.<br>
D. A write must synchronously reach another Region before it succeeds.<br>
E. GSI reads become strongly consistent under MREC.

### 11. Highly available feature-rich cache

An application needs a shared in-memory cache with sorted sets, read replicas, Multi-AZ automatic failover, and optional sharding across nodes.

Which service and engine should the architect select first?

A. ElastiCache for Memcached<br>
B. ElastiCache for Valkey or Redis OSS<br>
C. DynamoDB Accelerator<br>
D. Amazon Keyspaces

### 12. Memcached fit — Choose TWO

A web tier needs a simple, ephemeral object cache. It benefits from a multithreaded cache engine and can tolerate cache loss because every object can be rebuilt from the database. It does not require native replication, automatic failover, or rich data structures.

Which two statements are correct? Choose TWO.

A. ElastiCache for Memcached is a suitable first choice.<br>
B. The cache should be treated as the authoritative durable system of record.<br>
C. Adding/removing Memcached nodes can scale the cache horizontally with suitable client distribution.<br>
D. Memcached replication groups provide Redis-style read replicas and automatic failover.<br>
E. DAX is required even though the durable database is not DynamoDB.

### 13. Cache failure design — Choose TWO

A critical API uses ElastiCache in front of a durable database. The cache cluster becomes unavailable. The API must continue to serve correct data, although with degraded latency, and it must avoid overwhelming the database when the cache recovers.

Which two design measures are most appropriate? Choose TWO.

A. Fall back to the durable database on cache misses or cache failure.<br>
B. Make the cache the only copy so the database is not on the request path.<br>
C. Use expiry/invalidation plus controlled cache repopulation to avoid stale data and a cache stampede.<br>
D. Convert all database reads to full table scans during the outage.<br>
E. Disable database throttling and connection controls permanently.

### 14. MongoDB-compatible migration

A company operates a MongoDB application with nested JSON-like documents and document-field queries. It wants a managed AWS database and the smallest practical application migration, subject to compatibility testing.

Which service should be evaluated first?

A. Amazon DocumentDB (with MongoDB compatibility)<br>
B. Amazon Keyspaces<br>
C. Amazon Neptune<br>
D. Amazon Timestream

### 15. Cassandra-compatible migration

A company wants to retire self-managed Cassandra clusters. Its applications use CQL and the existing wide-column data model must be retained with minimal client change.

Which service is the best first choice?

A. Amazon DynamoDB with DAX<br>
B. Amazon Keyspaces (for Apache Cassandra)<br>
C. Amazon DocumentDB<br>
D. Amazon OpenSearch Service

### 16. Relationship traversal

A fraud platform must traverse many levels of relationships among accounts, devices, addresses, and transactions. Analysts do not know every traversal path in advance.

Which service best matches the core access pattern?

A. Amazon Neptune<br>
B. ElastiCache for Memcached<br>
C. Amazon Timestream<br>
D. Amazon S3 Glacier Flexible Retrieval

### 17. Time-series selection — Choose TWO

An industrial platform ingests timestamped sensor measurements. It needs high-rate ingestion, queries over time windows, and different handling for recent versus historical measurements. A separate operational service needs millisecond lookup of the latest state by device ID.

Which two-service design best separates the access patterns? Choose TWO.

A. Use Amazon Timestream for time-window measurement analysis.<br>
B. Use DynamoDB for latest-state lookup by device ID.<br>
C. Use Amazon Neptune for all measurements because time is a relationship.<br>
D. Use ElastiCache as the only durable history store.<br>
E. Use DocumentDB only because the readings can be represented as JSON.

### 18. Authoritative store and derived search — Choose THREE

A product platform requires durable document storage, MongoDB-compatible application access, full-text relevance search, and a low-latency cache for popular product responses. Search results and cache entries may be rebuilt after failure.

Which three architecture decisions are most appropriate? Choose THREE.

A. Use Amazon DocumentDB as the authoritative document store, after compatibility testing.<br>
B. Maintain a derived Amazon OpenSearch Service index for full-text search.<br>
C. Use ElastiCache as a rebuildable acceleration layer.<br>
D. Make OpenSearch the only authoritative transaction store.<br>
E. Make the cache the only durable copy of popular products.<br>
F. Choose Amazon Neptune solely because products have related attributes.

## Frozen submission

Copy or complete this block exactly. Once you declare it frozen, do not change it.

```text
Start: 17:20
End: 18:00
Uncertain:8,10,18
1:B
2:AD
3:C
4:AB
5:B
6:AB
7:A
8:ACD
9:B
10:AC
11:B
12:AC
13:AC
14:A
15:B
16:A
17:A
18:ABC

Submission status: FROZEN
```

## Post-attempt boundary

The submission was frozen on 2026-07-25. The answer-bearing assessment is now
available in
[the separate diagnostic review](sap-c02-non-relational-databases-diagnostic-review-20260725.md).
It records:

- exact-match score;
- question-by-question rationale;
- distractor analysis;
- error classification as knowledge, reasoning, reading, or confidence/calibration;
- a short targeted retest only if the evidence warrants one; and
- tracker/wrong-answer-log updates based on demonstrated gaps rather than assumed weakness.
