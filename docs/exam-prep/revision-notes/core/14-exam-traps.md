# 14 - SAP-C02 Exam Traps

These traps are written in “wrong answer vs correct reasoning” form.

## Streaming and messaging

| Trap | Correct reasoning |
|---|---|
| Use SQS for a replayable stream with multiple consumers | Use Kinesis Data Streams. |
| Use Firehose for custom real-time consumers | Use Kinesis Data Streams; Firehose is delivery-focused. |
| Use Kinesis for a simple one-worker background queue | Use SQS. |
| Assume Kinesis ordering is global | Ordering is per shard/partition key. |
| Ignore hot partition keys | Partition-key design is central to stream scaling. |
| Use SNS alone for durable fanout | Use SNS to SQS queues for durable subscriber isolation. |
| Use EventBridge for strict ordered processing | EventBridge is routing, not ordered stream processing. |
| Use Step Functions for high-volume stream analytics | Use Kinesis/Flink; Step Functions orchestrates workflows. |

## Load balancing and routing

| Trap | Correct reasoning |
|---|---|
| Route 53 caches HTTP content | CloudFront caches HTTP content; Route 53 is DNS. |
| Route 53 failover is instant | DNS TTL/resolver caching affects cutover. |
| ALB provides regional static IPs | NLB provides regional static IP/EIP pattern; Global Accelerator provides global anycast IPs. |
| NLB does path routing | ALB does Layer 7 host/path routing. |
| CloudFront and Global Accelerator are interchangeable | CloudFront caches HTTP; Global Accelerator accelerates traffic to endpoints. |
| Use WAF for raw TCP | WAF is HTTP(S) Layer 7. |

## Containers and compute

| Trap | Correct reasoning |
|---|---|
| Containers automatically mean EKS | ECS is simpler unless Kubernetes is required. |
| Fargate removes networking design | Tasks still need subnets, security groups, routing, endpoints/NAT. |
| Lambda is always best for async work | Long-running or heavy workers may fit ECS/Fargate/Batch. |
| ECS task role and execution role are identical | Task role is app permissions; execution role is platform operations. |
| EKS Fargate supports every Kubernetes workload | Node-level/DaemonSet/GPU patterns need EC2 nodes. |

## Security

| Trap | Correct reasoning |
|---|---|
| SCP grants permissions | SCP only restricts maximum permissions. |
| IAM policy alone controls encrypted object access | KMS key policy may also be required. |
| GuardDuty prevents attacks | GuardDuty detects; response controls are separate. |
| Security Hub is a scanner | It aggregates and prioritizes findings. |
| Shield replaces WAF | Shield is DDoS; WAF filters web requests. |
| Public S3 is safe with “obscure” object names | Use Block Public Access, bucket policies, IAM, OAC/OAI, KMS where required. |

## Databases

| Trap | Correct reasoning |
|---|---|
| Read replicas solve write bottlenecks | They scale reads. |
| Multi-AZ is same as read scaling | Multi-AZ is HA; read replicas/Aurora readers scale reads. |
| DynamoDB can be modeled like relational tables | Model by access pattern. |
| DAX fixes bad partition design | It is a cache, not a partition design fix. |
| Global Tables remove conflict concerns | App must tolerate/handle conflict semantics. |
| Cache is durable system of record | Cache is not primary storage. |

## Networking

| Trap | Correct reasoning |
|---|---|
| VPC peering is transitive | It is not. |
| PrivateLink connects two networks fully | It exposes a service endpoint. |
| NAT Gateway is always required for private subnets | VPC endpoints can remove many AWS API/S3/DynamoDB NAT paths. |
| Direct Connect is encrypted by default | It is private connectivity; encryption must be designed. |
| Security Groups are stateless | Security Groups are stateful; NACLs are stateless. |
| Subnet name determines public/private | Route table determines it. |

## Migration

| Trap | Correct reasoning |
|---|---|
| DMS migrates whole servers | MGN rehosts servers; DMS migrates databases. |
| SCT moves data | SCT converts/assesses schema; DMS moves data. |
| Snowball is continuous replication | It is offline/edge transfer; use DataSync/replication for continuous. |
| Migration Hub performs migration | It tracks migration. |
| Rehost is always the correct migration strategy | It is fast but may preserve technical debt. |

## Resilience

| Trap | Correct reasoning |
|---|---|
| Backup equals high availability | Backup is recovery, not live failover. |
| Multi-AZ equals multi-region DR | Multi-AZ protects against AZ failure; regional outage needs multi-region design. |
| Queue removes duplicate risk | At-least-once processing still needs idempotency. |
| DLQ solves failure | DLQ isolates; root cause and redrive process still required. |
| Auto Scaling fixes all bottlenecks | It only scales the layer configured; DB/network/state may still bottleneck. |
