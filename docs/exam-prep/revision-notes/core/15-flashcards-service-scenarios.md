# 15 - Flashcards: Service Scenarios

Use these as active recall prompts.

## Kinesis and events

Q: A clickstream needs replay and three independent consumers.
A: Kinesis Data Streams. Consider enhanced fan-out if consumers need dedicated throughput.

Q: Logs must be delivered to S3 with minimal operations.
A: Kinesis Data Firehose.

Q: A workload needs rolling 5-minute streaming aggregations.
A: Managed Service for Apache Flink with Kinesis source.

Q: Each background job should be processed by one worker with retries.
A: SQS + worker + DLQ.

Q: Same event must be delivered to billing, shipping, and notifications.
A: SNS topic fanout to separate SQS queues/subscribers.

Q: SaaS event should trigger Lambda based on event pattern.
A: EventBridge rule.

Q: Multi-step workflow needs retries, branching, and audit trail.
A: Step Functions.

## ALB/NLB/Route 53/CloudFront/GA

Q: Route `/orders/*` and `/payments/*` to separate services.
A: ALB.

Q: UDP service requires load balancing.
A: NLB.

Q: Public web content should be cached near global users.
A: CloudFront.

Q: DNS active-passive failover is required.
A: Route 53 failover routing with health checks.

Q: Global static anycast IPs and fast regional failover are required.
A: AWS Global Accelerator.

Q: Private SaaS service exposed from provider VPC to consumer VPCs.
A: PrivateLink with provider NLB.

## Containers

Q: Containerized app, no Kubernetes requirement, minimal ops.
A: ECS on Fargate.

Q: Kubernetes CRDs/operators are required.
A: EKS.

Q: Worker runs for 45 minutes processing SQS messages.
A: ECS/Fargate or Batch, not Lambda.

Q: Container needs host-level privileged access and daemon agents.
A: ECS/EKS on EC2, not Fargate.

Q: ECS task needs AWS API permissions. Which role?
A: Task role.

Q: ECS task must pull ECR image and write logs. Which role?
A: Execution role.

## Storage and analytics

Q: Query Parquet files in S3 using SQL without servers.
A: Athena + Glue Data Catalog.

Q: Shared Linux file system for many EC2 instances.
A: EFS.

Q: Windows SMB file share with AD integration.
A: FSx for Windows File Server.

Q: HPC scratch file system linked to S3.
A: FSx for Lustre.

Q: Move 200 TB from on-prem NAS to S3 over network repeatedly.
A: DataSync.

Q: Move petabytes when network is too slow.
A: Snow Family.

Q: Govern fine-grained data lake permissions.
A: Lake Formation.

## Databases

Q: SQL transactions and joins are required.
A: RDS/Aurora.

Q: Massive key-value workload with single-digit millisecond latency.
A: DynamoDB.

Q: Active-active multi-region NoSQL.
A: DynamoDB Global Tables.

Q: Relational DR with low-latency global reads.
A: Aurora Global Database.

Q: App has too many Lambda connections to RDS.
A: RDS Proxy and connection pooling.

Q: Read-heavy cacheable data causes DB load.
A: ElastiCache or DAX depending DB.

## Networking

Q: Many VPCs across accounts need hub-and-spoke connectivity.
A: Transit Gateway.

Q: Two VPCs need simple direct private connectivity.
A: VPC peering.

Q: Private subnet workloads need S3 access without NAT.
A: S3 Gateway VPC endpoint.

Q: Expose one service privately to another VPC without full routing.
A: PrivateLink.

Q: On-prem DNS must resolve private hosted zones.
A: Route 53 Resolver inbound endpoint.

Q: VPC must resolve on-prem names.
A: Route 53 Resolver outbound endpoint and forwarding rule.

## Security and governance

Q: Prevent member accounts from disabling CloudTrail.
A: SCP explicit deny + central logging.

Q: Give app temporary cross-account access.
A: AssumeRole into target account.

Q: Encrypted S3 object access fails despite S3 permissions.
A: Check KMS key policy/IAM permissions.

Q: Detect suspicious account activity.
A: GuardDuty.

Q: Aggregate security findings.
A: Security Hub.

Q: Find sensitive data in S3.
A: Macie.

Q: Protect HTTP app from SQL injection.
A: AWS WAF.

## Migration

Q: Lift-and-shift servers to AWS.
A: MGN.

Q: Migrate Oracle to Aurora PostgreSQL with conversion.
A: SCT + DMS.

Q: Track migration waves.
A: Migration Hub.

Q: On-prem app must keep NFS but store data in S3.
A: Storage Gateway File Gateway.

Q: Discover server dependencies.
A: Application Discovery Service.

## DR

Q: Cheapest DR, high RTO acceptable.
A: Backup and restore.

Q: Core DR components running but app mostly off.
A: Pilot light.

Q: Full environment scaled down in another region.
A: Warm standby.

Q: Both regions serve traffic.
A: Active-active.
