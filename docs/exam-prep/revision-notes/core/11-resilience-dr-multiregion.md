# 11 - Resilience, Disaster Recovery, and Multi-Region Design

**Last revised:** 2026-07-26

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

## Data replication patterns

| Data layer | DR option |
|---|---|
| S3 | CRR/SRR replicate objects; MRAP routes requests and controls failover; Versioning and Object Lock preserve recoverable history and immutability |
| RDS | Snapshots, read replicas, cross-region replicas, Multi-AZ |
| Aurora | Aurora replicas, Aurora Global Database |
| DynamoDB | Global Tables, PITR, backups |
| EFS | Backup, replication |
| FSx | Backups/replication features depending file system |
| Redshift | Snapshots/cross-region snapshot copy |
| OpenSearch | Snapshots/cross-cluster patterns depending requirement |

## Stateless vs stateful

Stateless tiers are easier to recover:

```text
CloudFront/Route 53/GA
  -> ALB
  -> ECS/Lambda/EC2 stateless app
  -> replicated stateful services
```

Stateful tiers require explicit replication, backup, consistency, and conflict handling.

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
