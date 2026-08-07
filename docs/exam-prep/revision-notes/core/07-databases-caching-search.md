# 07 - Databases, Caching, and Search

**Last revised:** 2026-07-28

SAP-C02 database questions are usually about access pattern, consistency, scaling model, HA/DR, operational burden, and migration risk.

## Quick chooser

| Requirement | Service |
|---|---|
| Relational SQL, transactions, managed engine | RDS |
| Cloud-optimized relational, high availability, read scaling | Aurora |
| Global relational read scaling / DR | Aurora Global Database |
| Serverless relational capacity | Aurora Serverless |
| Key-value/document NoSQL at scale | DynamoDB |
| Multi-region NoSQL active-active | DynamoDB Global Tables |
| In-memory cache | ElastiCache Redis/Memcached |
| Data warehouse | Redshift |
| Search/log analytics | OpenSearch Service |
| Graph relationships | Neptune |
| Time-series | Timestream |
| Ledger/immutable history | QLDB |

## RDS

Choose RDS when:

- standard relational database engines are required
- SQL compatibility matters
- managed backups/patching/Multi-AZ are useful
- app migration expects MySQL/PostgreSQL/MariaDB/Oracle/SQL Server

High-yield features:

- Multi-AZ for high availability.
- Read replicas for read scale and offloading.
- Automated backups and snapshots.
- Performance Insights and Enhanced Monitoring.
- Storage autoscaling.
- IAM database authentication for supported engines.
- RDS Proxy for connection pooling.

Traps:

- Read replicas are asynchronous and primarily for scale, not synchronous HA.
- Multi-AZ standby is not used for read traffic in classic RDS patterns.
- Backups protect data but do not make an application multi-region.
- RDS Proxy helps connection storms but does not fix bad queries.

## Aurora

Choose Aurora when:

- higher availability/performance than standard RDS is required
- storage auto-scaling and multi-AZ distributed storage help
- read scaling with Aurora Replicas is required
- faster failover than many classic RDS patterns is desired
- MySQL/PostgreSQL compatibility is useful

Aurora patterns:

```text
Application -> Aurora cluster endpoint -> writer
Application -> Aurora reader endpoint -> replicas
```

Aurora Global Database:

```text
Primary Region: Aurora writer
  -> Cross-region replication
Secondary Region: read-only Aurora cluster
  -> promote during DR
```

Traps:

- Aurora Global Database is excellent for DR/read-locality, but write topology still matters.
- Cross-region replication is not the same as zero RPO.
- Application connection handling must understand writer/reader endpoints and failover.

## DynamoDB

Choose DynamoDB when:

- access patterns are key-value/document
- predictable low-latency access at high scale
- schema flexibility is useful
- serverless operational model is required
- events via DynamoDB Streams are useful
- global active-active NoSQL is needed

Key design points:

| Concept | Importance |
|---|---|
| Partition key | Drives data distribution |
| Sort key | Enables range/query patterns |
| GSI | Alternate query pattern |
| LSI | Alternate sort key within same partition key |
| WCU/RCU or on-demand | Capacity/cost model |
| Conditional writes | Idempotency and optimistic locking |
| TTL | Expire items automatically |
| Streams | React to table item changes |
| Global Tables | Multi-region active-active |

### DynamoDB global-table consistency

| Mode | Strong-read rule |
|---|---|
| Multi-Region eventual consistency (MREC) | A strongly consistent read against a replica table is valid and returns the latest locally committed value, but it can be stale relative to a recent write in another Region because replication is asynchronous. |
| Multi-Region strong consistency (MRSC) | A strongly consistent read on any available replica returns the latest committed item across the global table, subject to MRSC topology and feature constraints. |

Do not memorize that a strongly consistent read on an MREC replica throws `ValidationException`. Strong reads are unsupported on GSIs and Streams; the fact that a table is an MREC replica does not itself make a table-level strong read invalid.

Traps:

- You design DynamoDB around access patterns, not normalized relational modeling.
- Hot partitions can throttle a table even if total capacity seems sufficient.
- Scans are usually a bad primary access pattern.
- GSIs are eventually consistent.
- Global Tables require conflict-aware application design.
- Distinguish MREC local strong-read semantics from MRSC cross-Region strong consistency; neither is the same as requesting a strong read from a GSI.
- DAX caches reads but does not fix poor key design.

## ElastiCache

### Redis

Choose Redis when:

- sub-millisecond cache is required
- sorted sets/lists/pub-sub/advanced structures are useful
- session store or leaderboard style workload exists
- read-heavy database offload is needed

### Memcached

Choose Memcached when:

- simple distributed cache is needed
- object caching is simple
- no persistence/advanced Redis structures are needed

Traps:

- Cache invalidation is an application design problem.
- Caching can reduce read load but can introduce stale data.
- ElastiCache is not a durable system of record.
- Redis cluster mode, replication, Multi-AZ, and backups matter for resilience.

## Redshift

Choose Redshift for:

- structured analytics
- data warehouse workloads
- BI dashboards
- large-scale SQL analytics
- integration with S3 via Spectrum

Trap: Do not choose Redshift for OLTP app transactions. Use RDS/Aurora/DynamoDB.

## OpenSearch

Choose OpenSearch for:

- full-text search
- log analytics
- near-real-time indexing/search
- dashboards and observability style querying

Trap: OpenSearch is not a relational database or durable queue. It is a search/analytics engine.

## Database migration mapping

| Source/target | Tool |
|---|---|
| Homogeneous database migration | DMS |
| Heterogeneous schema conversion | SCT + DMS |
| Server lift-and-shift including DB server | MGN |
| File/object data movement | DataSync/Snow |
| Mainframe modernization | specialized migration/modernization tooling and partner patterns |

## Exam traps

| Trap | Correction |
|---|---|
| “Need SQL joins, choose DynamoDB” | Use RDS/Aurora unless access pattern is NoSQL. |
| “Need single-digit ms key-value globally, choose RDS read replicas” | DynamoDB Global Tables may fit better. |
| “Use cache as database” | Cache is not primary durable storage. |
| “Read replica solves write scaling” | It scales reads, not writes. |
| “DMS converts schemas automatically” | SCT handles heterogeneous schema conversion; DMS moves data. |
| “DAX solves all DynamoDB performance issues” | Bad partition design remains bad. |
