# 01 - Service Scenario Index

**Last revised:** 2026-08-08

This file is the quick recognition layer. Use it before deep service notes.

## Common trigger phrases and likely services

| Scenario phrase | Usually points to | Watch out for |
|---|---|---|
| Real-time clickstream, IoT telemetry, ordered per user/device, replayable stream | Kinesis Data Streams | Not SQS if multiple consumers need replay; not Firehose if custom real-time consumers are required |
| Stream data directly to S3/Redshift/OpenSearch with minimal ops | Amazon Data Firehose | Firehose is delivery, not a general-purpose replayable stream |
| SQL/windowed analytics on streaming events | Managed Service for Apache Flink | Older materials may call this Kinesis Data Analytics |
| Decouple web request from background processing | SQS + worker/Lambda/ECS | Standard SQS is at-least-once and best-effort order |
| Broadcast same event to many subscribers | SNS fanout, often SNS -> SQS | SQS alone is one queue, not broadcast |
| Route AWS/SaaS/application events to targets | EventBridge | EventBridge is routing; Step Functions is orchestration |
| Multi-step stateful workflow with retries and human/manual branches | Step Functions | Do not use Lambda chaining as workflow engine |
| HTTP host/path routing to microservices | ALB | NLB is Layer 4; Route 53 is DNS only |
| TCP/UDP/TLS service, static regional IPs, PrivateLink provider | NLB | ALB has Layer 7 features but not UDP/static per-AZ IP pattern |
| Active-passive DNS failover | Route 53 failover + health checks | Cutover is affected by DNS TTL and resolver behavior |
| Improve global latency via edge cache | CloudFront | Global Accelerator does not cache content |
| Global static anycast entry point to regional ALB/NLB | AWS Global Accelerator | Route 53 gives DNS answers, not anycast proxying |
| Private S3 and public ALB origins must be reachable only through CloudFront | OAC + distribution-scoped S3 bucket policy; CloudFront origin custom header + validation at the ALB boundary | WAF on CloudFront does not stop a direct request to the ALB; S3 ACL is not the distribution-scoped control |
| Private application origin must not be internet-facing | CloudFront VPC origin to an internal ALB/NLB/EC2 origin where supported | A secret header protects a public origin only while the secret remains secret |
| Containerized web app, no server management | ECS + Fargate + ALB | EKS only if Kubernetes requirement exists |
| Kubernetes ecosystem, CRDs/operators/platform standard | EKS | Higher operational complexity than ECS |
| Long-running container job from queue/event | ECS RunTask on Fargate or AWS Batch | Lambda has execution duration and runtime constraints |
| Source code or container image to a public web service with the least platform management | App Runner | Elastic Beanstalk exposes more EC2/environment control; ECS offers more orchestration control |
| Managed application platform but retain EC2, Auto Scaling, load-balancer, platform, and deployment configuration | Elastic Beanstalk | App Runner is the narrower, more abstract web-service path |
| Tightly coupled EC2 nodes need low-latency inter-node networking | Cluster placement group | Adding instances or using EC2 Fleet does not control proximity |
| MPI/NCCL/Libfabric HPC or ML traffic needs OS-bypass networking | EFA, normally with supported instances and a cluster placement group | ENA is conventional enhanced IP networking; EFA requires application/library compatibility |
| General EC2 enhanced IP networking, higher PPS and lower CPU overhead | ENA | ENA does not place instances close together |
| NoSQL key-value at massive scale, single-digit ms latency | DynamoDB | Hot partitions and access pattern design matter |
| Relational database, SQL, transactions | RDS/Aurora | DynamoDB is not relational |
| Global relational low-latency reads / DR | Aurora Global Database | Cross-region writes require careful design; Global DB is not magic active-active for all writes |
| Global NoSQL multi-region writes | DynamoDB Global Tables | Conflict handling and regional application design matter |
| Shared POSIX file system for Linux compute | EFS | S3 is object storage, not a mounted POSIX file system |
| Windows file shares | FSx for Windows File Server | EFS is Linux/NFS-oriented |
| Lustre/HPC scratch linked to S3 | FSx for Lustre | Not EFS for high-performance HPC scratch |
| Move large files repeatedly between on-prem and AWS | DataSync | DMS is database migration; Snow is offline bulk transfer |
| Hybrid low-latency private connectivity | Direct Connect, VPN backup | VPN alone may not meet consistent bandwidth/latency |
| Central network hub across many VPCs/accounts | Transit Gateway | VPC peering does not scale cleanly as a mesh |
| Private service exposure across VPC/account without opening full network | PrivateLink | Not transitive; endpoint service pattern |
| Restrict accounts centrally | Organizations SCP | SCP sets permissions guardrails but does not grant permissions |
| Encrypt data and control key usage | KMS key policy + IAM + grants | Key policy matters; IAM alone may not be enough |
| Detect threats/account compromise | GuardDuty | Not a prevention service |
| Interactive instance shell without bastion, SSH keys, or inbound port 22 | Systems Manager Session Manager | Run Command is fleet command execution, not an interactive shell |
| Execute the same command across a fleet without an interactive login | Systems Manager Run Command | Session Manager is interactive access |
| Existing ActiveMQ/RabbitMQ/JMS/protocol-dependent application needs managed brokers with minimal rewrite | Amazon MQ | SQS/SNS are cloud-native APIs, not drop-in protocol-compatible brokers |
| Managed GraphQL API with subscriptions and real-time client updates | AWS AppSync | API Gateway is the general REST/HTTP/WebSocket front door; AppSync supplies GraphQL semantics |
| Move supported SaaS application data to or from AWS without custom integration code | Amazon AppFlow | DataSync moves file/object storage, not SaaS application records |
| Find, subscribe to, or share third-party datasets through AWS | AWS Data Exchange | It is a data entitlement/product service, not an ETL engine |
| Web app Layer 7 protection | AWS WAF | Shield is DDoS; Security Groups are network stateful filters |
| Apply WAF, Shield Advanced, Network Firewall, DNS Firewall, SG, or NACL policies across an organization | AWS Firewall Manager | The underlying controls filter traffic; Firewall Manager distributes and audits policy at scale |
| Download AWS compliance reports and agreements | AWS Artifact | Audit Manager collects evidence about the customer workload and controls |
| Continuously collect customer-environment audit evidence against frameworks | AWS Audit Manager | It assists evidence collection; it does not certify compliance |
| Single-tenant HSM control or application needs PKCS #11/JCE/CNG interfaces | AWS CloudHSM | KMS is the usual managed-key default and integrates directly with AWS services |
| Customer application sign-up/sign-in and user tokens | Amazon Cognito | IAM Identity Center is the workforce multi-account access front door |
| Heterogeneous DB migration | SCT + DMS | DMS moves data; SCT converts schema/code |
| Lift-and-shift servers | Application Migration Service (MGN) | Not DMS unless database data migration |
| Track portfolio migration | Migration Hub | It is tracking/orchestration visibility, not the migration engine |

## “Wrong answer” indicators

| Wrong service | Why it may be wrong |
|---|---|
| SQS for event stream replay | SQS is a queue, not a replayable ordered stream with independent consumers |
| Firehose for custom low-latency processing | Firehose is managed delivery; use Kinesis Data Streams for custom consumers/replay |
| Route 53 for content caching | Route 53 is DNS, not an HTTP cache or proxy |
| ALB for UDP | Use NLB for UDP/TCP/TLS Layer 4 |
| NLB for path routing | Use ALB for host/path/header-based routing |
| EKS because “containers” | ECS/Fargate is simpler unless Kubernetes is required |
| Lambda for long-running workers | Use ECS/Fargate, Batch, or EC2 for sustained jobs |
| SCP to grant permissions | SCP only limits maximum permissions; IAM/resource policies still grant |
| Multi-AZ for regional DR | Multi-AZ handles AZ failure, not full regional outage |
| Read replica for automatic DB failover | Read replicas are primarily scale/read/offload unless promoted; Multi-AZ/Aurora replicas handle HA differently |
| Client VPN when the requirement is audited instance shell commands | VPN supplies network connectivity; it does not record commands inside SSH. Use Session Manager for the interactive access requirement. |
| EFA merely because a question says “high performance” | EFA is for compatible HPC/ML communication stacks; use placement, instance, storage, database, or ENA remediation according to the measured bottleneck. |

## Scenario construction pattern

Most SAP-C02 scenarios combine several services:

```text
Users
  -> Route 53 / Global Accelerator / CloudFront
  -> ALB/NLB/API Gateway
  -> ECS/Fargate/Lambda/EC2
  -> SQS/EventBridge/Kinesis
  -> DynamoDB/RDS/S3/Redshift
  -> CloudWatch/X-Ray/CloudTrail/Config
```

Do not answer with a single service unless the scenario genuinely asks for only one service.
