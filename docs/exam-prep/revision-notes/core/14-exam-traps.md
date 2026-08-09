# 14 - SAP-C02 Exam Traps

**Last revised:** 2026-08-09

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
| Put WAF only at CloudFront to prevent direct ALB access | Direct ALB requests bypass it; validate the CloudFront-added secret at the ALB boundary or use a private VPC origin. |
| Use Route 53 or NLB to route by `User-Agent` | DNS and Layer 4 components do not inspect HTTP headers. Use CloudFront edge logic or ALB. |
| Use Route 53 geolocation when individual users need location overrides | DNS cannot inspect identity/cookies; use Lambda@Edge dynamic origin selection. |
| Read `CloudFront-Viewer-Country` in a viewer-request function | CloudFront adds it after viewer request; use an origin-request Lambda@Edge trigger. |
| Use CloudFront to accelerate interactive UDP sessions | Use multi-Region endpoints with Global Accelerator for UDP. |

## Containers and compute

| Trap | Correct reasoning |
|---|---|
| Containers automatically mean EKS | ECS is simpler unless Kubernetes is required. |
| Fargate removes networking design | Tasks still need subnets, security groups, routing, endpoints/NAT. |
| Lambda is always best for async work | Long-running or heavy workers may fit ECS/Fargate/Batch. |
| ECS task role and execution role are identical | Task role is app permissions; execution role is platform operations. |
| EKS Fargate supports every Kubernetes workload | Node-level/DaemonSet/GPU patterns need EC2 nodes. |
| Put a server-rendered JavaScript UI in an S3 static website without changing it | Server-side execution must remain on compute; containerize it for a bounded replatform. |
| Add EC2 instances to fix tightly coupled inter-node latency | Capacity count does not fix placement; use a cluster placement group. |
| Configure EFA for any network timeout | EFA requires compatible HPC/ML libraries and instances; it is not a transparent general TCP fix. |
| Use a partition placement group for tightly coupled HPC | Partition placement isolates failure domains; use cluster placement for proximity and EFA for a compatible OS-bypass workload. |
| Treat disabled hyper-threading as the HPC network mechanism | One thread per core may improve compute KPIs, but EFA and placement provide the network path. |
| Choose Fargate for an AWS Batch job that requires a custom AMI | Use a managed EC2 compute environment; Fargate does not provide custom host AMIs. |
| Use Spot for the required website baseline | Use commitment discounts for steady baseline, On-Demand for service peaks, and Spot for interruptible queued workers. |
| Scale on ALB request count when request cost varies greatly | Scale on the constrained resource, such as CPU; use step scaling for different breach magnitudes when target tracking is not offered. |
| Use CPU target tracking when peaks do not change CPU metrics | Use ALB request count per target when it is the proportional demand signal. |
| Use scheduled or predictive scaling for unexpected, non-recurring peaks | Use dynamic target tracking; scheduled needs known times and predictive needs forecastable history. |
| Add Memcached nodes and assume clients automatically use them | The client must use Auto Discovery or an updated endpoint/hash-ring configuration. |
| Enable hibernation after an existing instance has launched | Hibernation is selected at launch and requires a supported configuration plus an encrypted, sufficiently large root volume. |

## Security

| Trap | Correct reasoning |
|---|---|
| SCP grants permissions | SCP only restricts maximum permissions. |
| IAM policy alone controls encrypted object access | KMS key policy may also be required. |
| GuardDuty prevents attacks | GuardDuty detects; response controls are separate. |
| Security Hub is a scanner | It aggregates and prioritizes findings. |
| Shield replaces WAF | Shield is DDoS; WAF filters web requests. |
| Public S3 is safe with “obscure” object names | Use Block Public Access, bucket policies, IAM, OAC/OAI, KMS where required. |
| Inspector discovers sensitive fields in S3 | Macie classifies sensitive S3 data; Inspector finds software vulnerabilities/exposure. |
| Enabling S3 default encryption rewrites existing objects | It controls future writes; use Inventory/manifest plus Batch Operations for existing objects. |
| Client VPN automatically records SSH commands | It supplies network access, not shell-command auditing. Use Session Manager normal shell logging. |
| Session Manager logs a port-forwarding or SSH tunnel’s contents | It logs normal shell sessions when configured; tunnel contents are not available for logging. |
| Governance mode means nobody can delete a retained object | Principals with bypass permission can override governance retention; use compliance mode when even root must be unable to shorten or bypass retention. |
| Object Lock can only be selected when creating a bucket | Current S3 supports enabling it on an existing versioned general-purpose bucket; protect pre-existing versions separately. |
| FIPS Level 3 automatically selects CloudHSM | Current KMS HSMs also meet Level 3; CloudHSM needs a dedicated-HSM/control/interface requirement. |
| Member-account root can repair or bypass an SCP | SCPs apply to member root; use an authorized management-account principal to repair organization policy. |
| Duplicate the same SCP in sibling OUs when one group is a stricter subset | Put the common SCP on a parent OU and the extra restriction on the child OU. |
| An account-zone Access Analyzer identifies only principals outside the organization | Its trust boundary is the account, so same-organization accounts can be external findings; use an organization-zone analyzer. |
| Use a Cognito user pool for any custom authentication system | A non-SAML/OIDC custom backend can use developer-authenticated identities in an identity pool; the same pool can also support guests. |
| A prior hardcoded-password failure automatically requires alternating-users rotation | Single-user rotation is the simplest default; choose alternating users only for an explicit highest-availability requirement during rotation. |
| Encrypt CloudTrail logs to detect tampering | Encryption protects confidentiality; log-file integrity validation detects modification or deletion. |
| Simple AD satisfies an LDAP-plus-MFA requirement | Simple AD does not support MFA; use AWS Managed Microsoft AD when the on-prem directory will be retired. |
| Query yesterday's CloudTrail logs to catch a scheduled KMS deletion | Match `ScheduleKeyDeletion` through EventBridge and notify via SNS during the waiting period. |
| Treat an OAC ID or CloudFront distribution ARN as the bucket-policy principal | Use the CloudFront service principal and restrict it with `AWS:SourceArn` equal to the distribution ARN. |
| Caller-side `sts:AssumeRole` permission is enough for cross-account access | The target role trust policy must also allow the caller as a principal. |
| Convert an inline policy to a managed policy to fix AssumeRole | Policy packaging does not change authorization; repair the missing trust or permission statement. |
| Deploy AD Connector on premises for WorkSpaces | AD Connector is an AWS Directory Service proxy in the WorkSpaces VPC; it forwards authentication to on-prem AD without storing credentials. |
| A tag policy automatically requires every resource to have the defined tag | Basic tag-policy enforcement validates supplied keys/values on supported operations; use an SCP or approved provisioning control to require presence. |
| Security Hub aggregation automatically enables every linked Region | Integrate with Organizations and use a delegated administrator; enable/configure relevant Regions, then aggregate into the home Region. |
| One Firewall Manager policy type fits every SSH-CIDR requirement | Use Network Firewall policy for organization-wide packet enforcement; use content audit only when the stem explicitly governs existing SG rule content. |
| Put exceptional workloads in normal environment OUs and rely on hidden account exceptions | Use an Exceptions OU so deviations remain visible; keep shared controls at OU level. |
| Ask every account administrator to launch the daily backup configuration | Use an Organizations backup policy backed by AWS Backup for inherited, centrally governed plans. |

## Databases

| Trap | Correct reasoning |
|---|---|
| Read replicas solve write bottlenecks | They scale reads. |
| Multi-AZ is same as read scaling | Multi-AZ is HA; read replicas/Aurora readers scale reads. |
| DynamoDB can be modeled like relational tables | Model by access pattern. |
| DAX fixes bad partition design | It is a cache, not a partition design fix. |
| Savings Plans reduce DynamoDB capacity charges | Savings Plans discount eligible compute usage, not DynamoDB read/write capacity. |
| Global Tables remove conflict concerns | App must tolerate/handle conflict semantics. |
| Cache is durable system of record | Cache is not primary storage. |
| ElastiCache Redis is the most durable managed destination for a Redis system of record | Use MemoryDB when Redis-compatible data itself must be durable; ElastiCache is primarily a cache. |
| Memcached provides native Multi-AZ replication and automatic failover | It does not; use a Redis OSS replication group with Multi-AZ when native cache failover is required. |
| Secondary Aurora reads immediately see writes sent to the primary | Use write forwarding and an appropriate consistency mode when local read-after-write behavior is required. |

## Networking

| Trap | Correct reasoning |
|---|---|
| VPC peering is transitive | It is not. |
| PrivateLink connects two networks fully | It exposes a service endpoint. |
| NAT Gateway is always required for private subnets | VPC endpoints can remove many AWS API/S3/DynamoDB NAT paths. |
| One NAT Gateway makes a multi-AZ design fully AZ-resilient | A NAT Gateway is AZ-scoped; use one per AZ and local-AZ routes. |
| Direct Connect is encrypted by default | It is private connectivity; encryption must be designed. |
| Security Groups are stateless | Security Groups are stateful; NACLs are stateless. |
| Subnet name determines public/private | Route table determines it. |
| Tags on Transit Gateway attachments isolate traffic | Tags do not enforce routes by themselves; use TGW route-table association and propagation. |
| An S3 gateway endpoint can be used through inter-Region VPC peering | Gateway endpoints cannot; use a billed S3 interface endpoint or replicate to a local bucket according to traffic economics. |
| Create interface/gateway endpoints to connect entire VPCs | Endpoints expose services, not full VPC routing; use Transit Gateway or peering. |
| An accepted TCP request means a stateless NACL permits the response | NACLs evaluate the reverse tuple separately, including the client's ephemeral destination port. |

## Storage availability

| Trap | Correct reasoning |
|---|---|
| EBS Multi-Attach gives cross-AZ shared block storage | Multi-Attach instances must be in the same AZ; use Regional EFS for shared Linux files across AZs. |
| DataSync keeps two application volumes as one live shared file system | DataSync schedules transfers; it does not provide coherent concurrent file access. |
| Daily DataSync maintains one authoritative hybrid file namespace | It creates another synchronized copy; S3 File Gateway presents NFS/SMB access to S3-backed objects. |
| Attach policies to S3 folders | Folders are key prefixes, not policy resources; use prefix-scoped IAM or access-point policies. |

## Storage cost and retrieval

| Trap | Correct reasoning |
|---|---|
| Choose Deep Archive when retrieval must finish within six hours | Standard Deep Archive retrieval can take about 12 hours; Archive Access/Flexible Retrieval is typically 3-5 hours. |
| Apply one age-based lifecycle class when early access varies by object | Intelligent-Tiering follows observed per-object access and can save during the mixed-access period. |
| Avoid One Zone-IA even when the data is explicitly reproducible | Re-creatable data can accept the AZ-loss trade-off when lowest cost is required. |

## Migration

| Trap | Correct reasoning |
|---|---|
| DMS migrates whole servers | MGN rehosts servers; DMS migrates databases. |
| SCT moves data | SCT converts/assesses schema; DMS moves data. |
| Snowball is continuous replication | It is offline/edge transfer; use DataSync/replication for continuous. |
| Pick Snowball from dataset size alone | Compare full elapsed time; a five-day online transfer can beat appliance logistics. |
| Containerize highly customized legacy servers when no modernization is possible | Rehost first with AWS Transform MGN; containerization changes the runtime and requires more migration effort. |
| Use DataSync to migrate a bootable physical Windows server | DataSync moves storage data; MGN rehosts the server. |
| Reject a valid Db2-to-RDS answer solely because it says “SCT replication agent” | Treat the label as legacy/imprecise: SCT converts schema and DMS replicates data; validate the source/target pair and intended architecture. |
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
| A standard cross-Region RDS PostgreSQL read replica guarantees a configured 30-second RPO | Aurora PostgreSQL Global Database exposes `rds.global_db_rpo`; ordinary asynchronous read replicas do not. |
| API Gateway -> synchronous Lambda is durable asynchronous acceptance | If processing exceeds the client timeout, integrate API Gateway with SQS and process later. |
| ARC readiness checks move production traffic | Readiness checks assess recovery preparedness; routing controls change Route 53 health-check state and traffic eligibility. |
| Route 53 failover alone starts dormant compute and promotes an RDS replica | DNS moves traffic only; recovery automation must also scale compute and promote/update the data tier. |
| A recovery Lambda alone completes Regional failover | It can activate resources, but a Route 53 or Global Accelerator policy must redirect users. |
| Test healthy Aurora global DR by forcing an unplanned failover | Use a switchover/planned operation to synchronize first and target zero data loss. |
| Restore RDS from cross-Region backup for the fastest recovery | Promote an already-running cross-Region read replica; restore is the slower recovery path. |
| Change an existing SQS Standard queue into FIFO | Queue type cannot be converted; create a new FIFO queue and update the application. |
| Scale queue workers on total visible messages | Use backlog per instance against acceptable processing delay; raw depth is not proportional to worker count. |
