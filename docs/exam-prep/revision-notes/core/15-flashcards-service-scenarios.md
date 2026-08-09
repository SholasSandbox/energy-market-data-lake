# 15 - Flashcards: Service Scenarios

**Last revised:** 2026-08-09

Use these as active recall prompts.

## Kinesis and events

Q: A clickstream needs replay and three independent consumers.
A: Kinesis Data Streams. Consider enhanced fan-out if consumers need dedicated throughput.

Q: Logs must be delivered to S3 with minimal operations.
A: Amazon Data Firehose (formerly Kinesis Data Firehose).

Q: A workload needs rolling 5-minute streaming aggregations.
A: Managed Service for Apache Flink with Kinesis source.

Q: Each background job should be processed by one worker with retries.
A: SQS + worker + DLQ.

Q: Unchanged clients time out after 10 seconds, while each request takes 90 seconds to process.
A: API Gateway direct integration to SQS for durable acceptance, then scalable asynchronous workers.

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

Q: Global interactive game sessions send significant UDP traffic.
A: Deploy Regional endpoints and use Global Accelerator; CloudFront is not a UDP session accelerator.

Q: Private SaaS service exposed from provider VPC to consumer VPCs.
A: PrivateLink with provider NLB.

Q: Private S3 assets and an internet-facing ALB must both be accessible only through CloudFront.
A: OAC plus a distribution-scoped bucket policy for S3; CloudFront custom origin header plus validation at the ALB boundary for the ALB.

Q: Global static site needs different object paths for mobile and desktop viewers, and EC2 origin load is high.
A: Put static assets in S3 behind CloudFront and use edge request logic such as Lambda@Edge to classify the device and rewrite the object URI; align the cache key.

Q: Geography chooses a default S3 website version, but named users can override it without changing domains.
A: One CloudFront distribution with origin-request Lambda@Edge selecting the origin from country and user/cookie attributes.

## Containers

Q: Containerized app, no Kubernetes requirement, minimal ops.
A: ECS on Fargate.

Q: Kubernetes CRDs/operators are required.
A: EKS.

Q: Worker runs for 45 minutes processing SQS messages.
A: ECS/Fargate or Batch, not Lambda.

Q: Container needs host-level privileged access and daemon agents.
A: ECS/EKS on EC2, not Fargate.

Q: A monthly restartable AWS Batch job requires a custom AMI and lower cost.
A: Managed EC2 compute environment using the custom AMI and Spot capacity.

Q: ECS task needs AWS API permissions. Which role?
A: Task role.

Q: ECS task must pull ECR image and write logs. Which role?
A: Execution role.

Q: Server-rendered JavaScript UI, Python API, and MySQL must be replatformed with minimal development and host operations.
A: Containerize both compute tiers on ECS Fargate behind an ALB; use RDS for MySQL Multi-AZ. Do not treat the server-rendered UI as static S3 content.

Q: Complex Linux applications must lose host-administration work without being redesigned.
A: Containerize on ECS Fargate and retain MySQL semantics with RDS for MySQL.

Q: A PHP developer knows LAMP, traffic is stable and bundled predictable pricing matters most.
A: Lightsail with a preconfigured LAMP instance and Lightsail object storage.

Q: Existing EC2 instances boot too slowly after month-long shutdowns.
A: Launch supported replacements with encrypted roots and hibernation enabled at launch; it cannot be enabled later.

Q: Grid-compute nodes have inter-instance network timeouts; adding nodes did not help.
A: Relaunch compatible instances together in a cluster placement group.

Q: HPC application explicitly uses MPI/Libfabric and needs OS-bypass networking.
A: EFA on supported instances, normally combined with a cluster placement group.

Q: A choose-three HPC item offers EFA, single-AZ deployment, disabled SMT, partition placement, burstable instances, and a PV AMI—but no cluster placement.
A: EFA-capable instances, one Availability Zone, and disabled SMT. EFA/same-AZ are the network architecture; SMT is compatible CPU tuning selected by elimination. Prefer cluster placement if it is offered.

## Storage and analytics

Q: Query Parquet files in S3 using SQL without servers.
A: Athena + Glue Data Catalog.

Q: Shared Linux file system for many EC2 instances.
A: EFS.

Q: Two application instances in different AZs must read and update the same output file.
A: Regional EFS. EBS Multi-Attach is same-AZ only, and DataSync is not a live shared file system.

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

Q: Find sensitive data in millions of S3 objects and apply required encryption to existing objects with minimal application change.
A: Macie for discovery; bucket default encryption for future writes; S3 Inventory/manifest plus S3 Batch Operations for existing objects.

Q: Retain every new S3 object version for five years with no bypass, including root.
A: Enable Versioning and Object Lock on the new or existing general-purpose bucket and configure five-year compliance-mode default retention. Protect any versions that predate the default separately.

Q: Access varies during the first six months; inactive objects may archive at day 180 but must restore within six hours.
A: Intelligent-Tiering with optional Archive Access at 180 days. Deep Archive is too slow for the restore ceiling.

Q: Reproducible videos are read daily for 60 days, infrequently for six months, then rarely but must restore within five hours.
A: S3 Standard -> S3 One Zone-IA -> S3 Glacier Flexible Retrieval.

Q: EC2 in a second Region repeatedly reads a small, rarely changed S3 dataset and transfer cost is rising.
A: Replicate to a bucket in the second Region with CRR and read through a local S3 gateway endpoint.

Q: Legacy on-prem NFS clients and migrated applications must use one S3-backed dataset during a staged migration.
A: S3 File Gateway for on-prem clients and direct S3 access for migrated applications.

Q: Different populations need scalable access to different prefixes in one S3 bucket.
A: Prefix-scoped S3 access-point policies plus bucket delegation; S3 folders cannot have policies.

## Databases

Q: SQL transactions and joins are required.
A: RDS/Aurora.

Q: Massive key-value workload with single-digit millisecond latency.
A: DynamoDB.

Q: Active-active multi-region NoSQL.
A: DynamoDB Global Tables.

Q: Relational DR with low-latency global reads.
A: Aurora Global Database.

Q: PostgreSQL-compatible cross-Region DR must enforce a 30-second RPO with minimal application change.
A: Aurora PostgreSQL Global Database with `rds.global_db_rpo=30`; recognize that primary commits can be blocked if all secondary clusters exceed the target.

Q: An on-prem Redis leaderboard must become a managed, durable database with microsecond reads.
A: MemoryDB with Multi-AZ replicas; import a compatible Redis `.rdb` snapshot from S3. MGN rehosts the application servers, not the managed database.

Q: An RDS read cache must have native Multi-AZ failover.
A: ElastiCache for Redis OSS replication group with Multi-AZ, not Memcached.

Q: App has too many Lambda connections to RDS.
A: RDS Proxy and connection pooling.

Q: Read-heavy cacheable data causes DB load.
A: ElastiCache or DAX depending DB.

Q: DynamoDB receives a predictable bulk load and repeatedly reads a small key set; reduce cost with minimal cache operations.
A: DAX plus provisioned capacity with auto scaling. Savings Plans do not discount DynamoDB capacity.

Q: Newly added Memcached nodes are idle.
A: Configure a compatible client for ElastiCache Auto Discovery or update its node list and consistent-hash ring.

Q: A secondary Aurora Region needs local reads, forwarded writes and same-session read-after-write consistency.
A: Enable global write forwarding and use `SESSION` consistency.

## Networking

Q: Many VPCs across accounts need hub-and-spoke connectivity.
A: Transit Gateway.

Q: Production and development VPCs share one Transit Gateway but must not communicate.
A: Associate and propagate each environment through separate TGW route tables; remove permissive default-table routing.

Q: On-premises and a growing number of VPCs all need mutual connectivity.
A: Site-to-Site VPN attachment and VPC attachments on one Transit Gateway; remove per-VPC virtual private gateways.

Q: Flow logs accept client-to-server traffic but reject the server response to the client ephemeral port.
A: Correct the stateless NACL rule in the rejected direction; security groups already allow response traffic statefully.

Q: Two VPCs need simple direct private connectivity.
A: VPC peering.

Q: Private subnet workloads need S3 access without NAT.
A: S3 Gateway VPC endpoint.

Q: A second-Region VPC infrequently reads a large S3 dataset privately and duplicating the dataset is not economical.
A: Reach an S3 interface endpoint through inter-Region VPC peering or Transit Gateway. A gateway endpoint cannot serve the remote VPC.

Q: Private subnets in two AZs currently share one NAT Gateway and must survive an AZ failure.
A: Deploy a NAT Gateway in each AZ and route each private subnet to its local-AZ NAT.

Q: Expose one service privately to another VPC without full routing.
A: PrivateLink.

Q: On-prem DNS must resolve private hosted zones.
A: Route 53 Resolver inbound endpoint.

Q: VPC must resolve on-prem names.
A: Route 53 Resolver outbound endpoint and forwarding rule.

## Security and governance

Q: Prevent member accounts from disabling CloudTrail.
A: SCP explicit deny + central logging.

Q: HR and Recruiting share SCP requirements, but Recruiting needs additional restrictions.
A: Make Recruiting a child OU under HR's common-policy OU and attach the extra SCP to the child; move accounts rather than removing them from the organization.

Q: Deploy centrally protected Config rules to almost every organization account while excluding a few.
A: AWS Config organization conformance pack with excluded accounts, trusted access and delegated administration.

Q: Prove that delivered CloudTrail files were not modified or deleted.
A: Enable CloudTrail log-file integrity validation; encryption alone is not tamper evidence.

Q: Notify an administrator immediately when a KMS key deletion is scheduled.
A: EventBridge rule matching the CloudTrail `ScheduleKeyDeletion` event -> SNS.

Q: On-prem AD will be retired, but managed LDAP/Microsoft AD and MFA are required.
A: AWS Managed Microsoft AD. Simple AD lacks MFA; AD Connector still depends on on-prem AD.

Q: WorkSpaces must use on-prem AD credentials, retain SSO to on-prem files, and store no credentials in AWS.
A: AD Connector in the WorkSpaces VPC over Direct Connect or VPN. It proxies to on-prem AD and does not cache credentials.

Q: Root users in every current/future member account must be denied, but only Dev is restricted to one Region.
A: Attach the root-user deny SCP to the organization root and the Region restriction SCP to the Dev OU.

Q: Developers may self-provision only two approved infrastructure configurations.
A: Publish CloudFormation-backed Service Catalog products with launch constraints.

Q: One CloudFormation template must use different custom AMI IDs in each account and Region.
A: Store the local AMI ID under the same SSM Parameter Store name in every target account/Region and resolve it from the template.

Q: Two private S3 origins have different access populations, but both are served through one CloudFront distribution.
A: Associate OAC with each origin; use bucket policies with the CloudFront service principal constrained by the distribution ARN, and add separate approved principals only to the less-restricted bucket.

Q: CloudWatch alarms across several microservices must trigger controlled multi-Region traffic movement.
A: Use the alarm/Lambda decision path to toggle ARC routing controls; readiness checks do not reroute traffic.

Q: Give app temporary cross-account access.
A: AssumeRole into target account.

Q: One IAM principal has `sts:AssumeRole` permission but cannot enter the target account's role.
A: Add the principal to the target role's trust policy. Cross-account access needs caller-side permission and target-side trust.

Q: Only one named user may assume a cross-account audit role.
A: Name that exact ARN as the trust-policy `Principal`; do not wildcard every user in the source account.

Q: Encrypted S3 object access fails despite S3 permissions.
A: Check KMS key policy/IAM permissions.

Q: Detect suspicious account activity.
A: GuardDuty.

Q: Aggregate security findings.
A: Security Hub.

Q: Aggregate GuardDuty, Macie and Access Analyzer findings from many accounts and Regions.
A: Organizations-integrated Security Hub delegated administrator/member configuration plus a home Region and linked Regions; enable Security Hub in every relevant Region.

Q: Standardize organization tags, correct existing mismatches and require tags on future supported creates.
A: Organizations tag policy plus Resource Groups compliance reporting and service-side correction; add an SCP using supported request-tag conditions to require presence.

Q: Forecast shows EC2 cost will exceed 75% of budget and named instances must stop with minimum overspend risk.
A: Forecasted AWS Budgets alert plus an AWS Budgets action; a notification-only path leaves execution to a human.

Q: Find sensitive data in S3.
A: Macie.

Q: Protect HTTP app from SQL injection.
A: AWS WAF.

Q: Interactive shell without bastion, SSH keys, or inbound port 22, with command logs and a separate port-forwarding capability.
A: Systems Manager Session Manager. Log normal shell sessions; port-forwarding/SSH tunnel contents are not logged.

Q: Apply WAF and Network Firewall policies automatically across current and future organization accounts.
A: AWS Firewall Manager with Organizations and the required underlying protection services.

Q: Enforce SSH only from trusted CIDRs across accounts despite varied security groups, with member accounts unable to weaken the control.
A: Firewall Manager Network Firewall policy with a stateless trusted-CIDR pass rule evaluated before the default/drop path.

Q: Audit and remediate existing security groups that contain disallowed SSH rules.
A: Firewall Manager content audit security group policy using either an allowed-rule or disallowed-rule model.

Q: Most workloads share environment controls but a few require exceptional security policies.
A: Separate security accounts, environment OUs with OU-level SCPs, and an Exceptions OU for unique workloads.

Q: Every present and future member account must take daily EBS backups.
A: Complete Organizations backup policy using AWS Backup, attached at the organization root.

Q: S3 access fails simultaneously in several member accounts after central policy changes.
A: Inspect and correct the shared SCP through an authorized management-account principal; member-account root is also restricted by the SCP.

Q: An SQS queue is intentionally shared within an organization and with outside accounts; identify only outside principals.
A: IAM Access Analyzer with the organization as the zone of trust.

Q: A mobile app has guest users and premium users authenticated by a custom non-SAML/OIDC provider.
A: One Cognito identity pool with unauthenticated guest access and a developer-authenticated provider flow.

Q: Several Lambda functions share an RDS application credential and must remain available during rotation.
A: Retrieve it from Secrets Manager and use alternating-users rotation; do not hardcode it.

Q: Lambda functions use a hardcoded RDS password; the requirement is secure storage and automatic rotation with least development effort.
A: Retrieve the secret from Secrets Manager and use single-user rotation; the earlier hardcoding failure does not itself require alternating users.

Q: A call center needs managed telephony/contact flows, caller-intent recognition and queries to business systems.
A: Amazon Connect + Amazon Lex + AWS Lambda.

Q: Level 3 validated key storage must be highly available, low operations, and unusable outside scheduled hours.
A: KMS customer-managed key with scheduled disable/enable. Level 3 alone no longer selects CloudHSM; require dedicated HSM control or interfaces before accepting its operational burden.

Q: Download AWS SOC or ISO reports versus collect evidence about our own controls.
A: Artifact for AWS reports/agreements; Audit Manager for customer-environment assessment evidence.

## Migration

Q: A runbook defines VPC/EC2/RDS creation and host software installation; deployment must remain easy to change.
A: CloudFormation for infrastructure plus EC2 user data or managed bootstrap configuration.

Q: Transfer Family SFTP files must trigger managed ETL and then a durable completion message.
A: S3-backed Transfer Family -> EventBridge -> Glue -> Glue state-change event -> SQS.

Q: A physical Windows application has no source code and hardcoded OS configuration.
A: Rehost it to EC2 with AWS Transform MGN; DataSync is not a server-migration tool.

Q: A Db2 VM must leave quickly, and an answer offers a supported RDS target with DMS plus SCT.
A: If engine conversion is accepted, use SCT/DMS Schema Conversion for schema and DMS for data; do not reject the intended pairing because “SCT replication agent” is imprecise legacy wording.

Q: A Db2 VM must keep its engine or the business has not accepted conversion risk.
A: Rehost the VM with MGN and assess managed-database modernization separately.

Q: Lift-and-shift servers to AWS.
A: AWS Transform MGN, formerly Application Migration Service.

Q: A 300-TB online transfer completes in about five days; 150 VMs also need minimal-downtime rehosting.
A: S3 Transfer Acceleration for the online data path and AWS Transform MGN for the VMs; do not assume Snowball logistics are faster.

Q: Migrate Oracle to Aurora PostgreSQL with conversion.
A: SCT + DMS.

Q: Track migration waves.
A: Migration Hub.

Q: On-prem app must keep NFS but store data in S3.
A: Storage Gateway File Gateway.

Q: Discover server dependencies.
A: Application Discovery Service.

## DR

Q: A backup Region has an ALB, a zero-capacity ASG and an RDS cross-Region read replica; automatic failover must remain cheaper than active-active.
A: Route 53 failover health check plus alarm/SNS-triggered recovery automation that raises ASG capacity and promotes the replica. Traffic failover and resource activation are separate requirements.

Q: Regularly test Aurora Global Database DR while both Regions are healthy and minimize data loss.
A: Use a switchover, formerly managed planned failover; it synchronizes before changing roles.

Q: A prepared secondary ALB/ASG needs the fastest RDS Regional recovery.
A: Promote a cross-Region read replica and use Route 53 failover routing; backup restoration is slower.

Q: Duplicate transactions require FIFO and queue workers scale late.
A: Create a new FIFO queue—Standard cannot be converted—update the application, and target-track backlog per instance rather than total queue depth.

Q: Cheapest DR, high RTO acceptable.
A: Backup and restore.

Q: Core DR components running but app mostly off.
A: Pilot light.

Q: Full environment scaled down in another region.
A: Warm standby.

Q: Both regions serve traffic.
A: Active-active.
