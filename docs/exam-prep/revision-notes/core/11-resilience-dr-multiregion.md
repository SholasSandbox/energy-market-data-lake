# 11 - Resilience, Disaster Recovery, and Multi-Region Design

**Last revised:** 2026-08-09

SAP-C02 frequently tests reliability beyond single-service features. You must reason about **RTO**, **RPO**, state replication, traffic failover, and operational complexity.

## Terms

| Term | Meaning |
|---|---|
| RTO | Recovery Time Objective: how quickly service must be restored |
| RPO | Recovery Point Objective: how much data loss is acceptable |
| HA | High Availability: continued operation despite component failure |
| DR | Disaster Recovery: recover from major failure such as regional outage |
| Multi-AZ | Resilience within one region |
| Multi-region | Resilience across regions |

## DR strategies

| Strategy | Cost | RTO/RPO | Notes |
|---|---:|---:|---|
| Backup and restore | Low | High | Cheapest; slowest recovery |
| Pilot light | Low-medium | Medium | Core components always on |
| Warm standby | Medium | Low-medium | Scaled-down full environment |
| Active-active | High | Lowest | Highest complexity/cost |

## Multi-AZ vs multi-region

| Requirement | Design |
|---|---|
| Survive AZ failure | Multi-AZ load balancers, Auto Scaling, RDS Multi-AZ/Aurora |
| Survive regional failure | Multi-region app/data/traffic strategy |
| Low RTO/RPO regional failover | Warm standby or active-active |
| Lowest cost DR | Backup/restore or pilot light |

Trap: Multi-AZ does not solve regional outage.

## Traffic failover choices

| Requirement | Service |
|---|---|
| DNS active-passive | Route 53 failover |
| Fast global endpoint failover less dependent on DNS TTL | Global Accelerator |
| Edge origin failover for HTTP content | CloudFront origin failover |
| Application-level multi-region routing | App logic + data replication + routing layer |

CloudFront origin groups provide read-path failover for `GET`, `HEAD`, and
eligible `OPTIONS` requests. They do not provide write continuity for methods
such as `POST`, `PUT`, `PATCH`, or `DELETE`. A write path needs application-level
routing, retry/idempotency controls, and a data-consistency design.

## Data replication patterns

| Data layer | DR option |
|---|---|
| S3 | CRR/SRR replicate objects; MRAP routes requests and controls failover; Versioning and Object Lock preserve recoverable history and immutability |
| RDS | Snapshots, read replicas, cross-region replicas, Multi-AZ |
| Aurora | Aurora replicas, Aurora Global Database |
| DynamoDB | Global Tables with default MREC or optional MRSC, PITR, backups |
| EFS | Backup, replication |
| FSx | Backups/replication features depending file system |
| Redshift | Snapshots/cross-region snapshot copy |
| OpenSearch | Snapshots/cross-cluster patterns depending requirement |

### Aurora PostgreSQL Global Database managed RPO

When a PostgreSQL-compatible application needs cross-Region DR with a stated
RPO and minimum application change, Aurora PostgreSQL Global Database can set
an upper RPO bound with the `rds.global_db_rpo` cluster parameter. Valid values
start at 20 seconds, so a 30-second target is supported.

Aurora monitors secondary-cluster lag. If every secondary exceeds the target,
it can block primary commits until at least one secondary returns inside the
RPO window. This protects the target but can reduce primary write availability,
so “managed RPO” is a consistency-versus-availability trade-off, not free
replication.

A standard cross-Region RDS PostgreSQL read replica is asynchronous and does
not expose this managed RPO control. An RDS Multi-AZ standby stays within one
Region; it cannot place its standby in another Region.

### Aurora global write forwarding and DR testing

Global write forwarding lets an application connect to a secondary Aurora
cluster and forward eligible writes to the primary while keeping reads local.
For read-after-write behavior, use the appropriate consistency mode—`SESSION`
ensures that subsequent reads in the same secondary session see that session's
forwarded writes; stronger global consistency waits longer.

For a routine DR exercise while both Regions are healthy, use an Aurora global
database **switchover**, formerly called managed planned failover. Aurora waits
for synchronization before changing primary and secondary roles, targeting an
RPO of zero. Use failover for an actual unplanned outage, where asynchronous
replication can produce non-zero RPO.

Therefore, the low-operations combination for local latency, read-after-write
consistency and regular DR testing is secondary-cluster read/write access with
write forwarding plus planned switchover—not manual unplanned failover.

### Fastest RDS active-passive Regional recovery

A continuously maintained cross-Region RDS read replica is already much closer
to a usable database than a backup stored in another Region. For fastest
recovery, promote the replica and use Route 53 failover routing to direct users
to the prepared secondary stack. Restoring a new DB instance from AWS Backup
takes longer, while weighted routing describes active-active traffic splitting
rather than an active-passive DR cutover.

## Stateless vs stateful

Stateless tiers are easier to recover:

```text
CloudFront/Route 53/GA
  -> ALB
  -> ECS/Lambda/EC2 stateless app
  -> replicated stateful services
```

Stateful tiers require explicit replication, backup, consistency, and conflict handling.

### DynamoDB global-table consistency boundary

- **MREC** replicates asynchronously and supports broader features. A strongly
  consistent read of a replica is a valid local-table read, but it cannot make
  an asynchronously replicated remote write current.
- **MRSC** coordinates writes across Regions so a strong read at any available
  replica can return the latest committed value, at the cost of additional
  latency and topology/feature restrictions.
- MRSC does **not** support DynamoDB TTL or transaction APIs. A witness Region
  does not remove those restrictions.

Do not retain the older blanket rule that all global tables are eventually
consistent, or the false rule that a strong read on an MREC replica necessarily
throws `ValidationException`.

## Reliability controls by layer

| Layer | Controls |
|---|---|
| DNS/global | Route 53 health checks, GA endpoint health |
| Edge | CloudFront origin failover, WAF, Shield |
| Load balancer | Multi-AZ, health checks, cross-zone settings |
| Compute | Auto Scaling, multiple AZs, immutable deploys |
| Queue/stream | DLQ, retention, replay, consumer lag alarms |
| Database | Multi-AZ, backups, replicas, failover testing |
| Storage | versioning, replication, object lock, lifecycle |
| Operations | runbooks, game days, alarms, synthetic canaries |

## Common architecture patterns

### Regional HA web app

```text
Route 53
  -> ALB across 2+ AZs
  -> Auto Scaling group/ECS service across 2+ AZs
  -> RDS Multi-AZ or Aurora
```

### Active-passive multi-region

```text
Route 53 failover or Global Accelerator
  -> Primary Region active stack
  -> Secondary Region warm stack
  -> asynchronous data replication
  -> runbook/promote/failback process
```

### Automated pilot-light failover is a coordinated workflow

A backup Auto Scaling group with minimum and maximum capacity set to zero plus
an asynchronous cross-Region database read replica is a pilot-light design,
not active-active. Automatic recovery must coordinate two distinct outcomes:

```text
Route 53 application health check becomes unhealthy
  -> failover record makes the secondary ALB eligible
  -> CloudWatch alarm publishes through SNS
  -> recovery Lambda raises backup ASG capacity
  -> recovery Lambda promotes the cross-Region RDS read replica
```

Route 53 controls application traffic; it does not promote the database or
start compute. Conversely, a Lambda that starts compute and promotes the
replica does not redirect users unless the DNS or global traffic layer also
fails over.

For an RTO below 15 minutes without active-active cost, pre-created networking,
ALB, launch configuration, permissions and read replica remove provisioning
from the critical path. Validate that instance launch, health checks, database
promotion, application database-endpoint selection and DNS TTL fit inside the
RTO. Read-replica promotion is asynchronous DR and can lose unreplicated data.

### Event-driven resilience

```text
Producer
  -> SQS queue
  -> Workers
  -> DLQ
  -> CloudWatch alarms on queue depth and DLQ
```

### Stream resilience

```text
Producer retries with backoff
  -> Kinesis Data Streams
  -> Consumer checkpointing
  -> alarm on iterator age
  -> S3 archive for long-term replay
```

## Exam traps

| Trap | Correction |
|---|---|
| “Backups equal HA” | Backups support recovery, not live high availability. |
| “Read replica is synchronous DR” | Usually asynchronous; RPO matters. |
| “Route 53 failover is instant” | DNS TTL/resolvers affect cutover. |
| “Multi-region is automatically active-active” | Data/write conflict and app routing must be designed. |
| “Queues remove the need for idempotency” | At-least-once delivery still requires idempotency. |
| “DLQ is recovery” | DLQ is isolation; redrive needs process. |
| “Auto Scaling fixes database bottlenecks” | Scaling stateless compute does not scale stateful DB writes. |
| “CloudFront origin failover protects application writes” | It is a read-path mechanism; design write routing and state consistency separately. |
| “All DynamoDB global tables are eventually consistent” | Distinguish default MREC from optional MRSC and retain MRSC feature restrictions such as no TTL. |

## Additional references

- CloudFront origin failover: https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/high_availability_origin_failover.html
- DynamoDB global-table consistency modes: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/V2globaltables_HowItWorks.html
