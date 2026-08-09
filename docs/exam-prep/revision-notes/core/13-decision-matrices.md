# 13 - Decision Matrices

**Last revised:** 2026-08-09

Use these tables to answer SAP-C02 scenario questions faster.

## Event and integration services

| Requirement | Best answer |
|---|---|
| Durable work queue | SQS |
| Ordered work queue | SQS FIFO |
| Broadcast event to many subscribers | SNS |
| Event bus/routing/filtering | EventBridge |
| Multi-step workflow | Step Functions |
| Real-time ordered stream/replay | Kinesis Data Streams |
| Stream delivery to S3/OpenSearch/Redshift | Firehose |
| Stateful streaming analytics | Managed Service for Apache Flink |
| Kafka compatibility | MSK |
| Existing ActiveMQ/RabbitMQ/JMS or standard broker protocols | Amazon MQ |
| Managed GraphQL API and subscriptions | AppSync |
| Same short operational event from many accounts, one central owner | Member rules -> central EventBridge bus -> central Lambda |
| Managed SFTP upload, event-driven ETL and durable completion message | Transfer Family -> S3 -> EventBridge -> Glue -> EventBridge -> SQS |

## Compute

| Requirement | Best answer |
|---|---|
| Short event-driven code | Lambda |
| HTTP request must be durably accepted within seconds; processing is long/asynchronous | API Gateway service integration -> SQS -> scalable workers |
| Container web service, simple ops | ECS Fargate |
| Kubernetes required | EKS |
| Host-level control/special hardware | EC2/ECS on EC2/EKS nodes |
| Batch job queue | AWS Batch |
| Restartable Batch job requires a custom AMI and lower cost | Managed EC2 compute environment + Spot |
| Simple PaaS app deployment | Elastic Beanstalk / App Runner depending app |
| Familiar LAMP app, steady traffic, bundled predictable price and least learning | Lightsail preconfigured LAMP instance + Lightsail object storage |
| Edge function lightweight HTTP manipulation | CloudFront Functions / Lambda@Edge |
| Source/image directly to a managed public web service | App Runner |
| Managed application environment with EC2 configuration access | Elastic Beanstalk |
| Low-latency tightly coupled EC2 nodes | Cluster placement group |
| MPI/NCCL/Libfabric OS-bypass networking | EFA, usually with cluster placement |
| Resume an infrequently used EC2 application faster than a cold boot | Launch supported replacements with encrypted roots and hibernation enabled |

## Load balancing and edge

| Requirement | Best answer |
|---|---|
| DNS routing | Route 53 |
| HTTP path/host routing | ALB |
| TCP/UDP/TLS static regional IP | NLB |
| Appliance insertion | GWLB |
| HTTP content caching | CloudFront |
| Global anycast static IP acceleration | Global Accelerator |
| API auth/throttle/usage plans | API Gateway |
| Private S3 origin reachable only through CloudFront | OAC + distribution-scoped bucket policy |
| Public ALB origin reachable only through CloudFront | CloudFront secret origin header + ALB-side validation |
| Device-specific static objects at edge | S3 + CloudFront + edge URI/header logic |

## Storage

| Requirement | Best answer |
|---|---|
| Object storage/data lake | S3 |
| Shared Linux file system | EFS |
| Shared mutable Linux files across AZs | Regional EFS; EBS Multi-Attach is same-AZ only |
| Windows SMB file share | FSx for Windows |
| HPC scratch | FSx for Lustre |
| Hybrid NFS/SMB/iSCSI/tape | Storage Gateway |
| Online file transfer | DataSync |
| Offline bulk transfer | Snow Family |
| SFTP/FTPS/FTP managed transfer | Transfer Family |
| AS2 partner B2B/EDI file exchange | Transfer Family AS2 |
| Encrypt millions of existing S3 objects | S3 Inventory/manifest + S3 Batch Operations; configure default encryption for future writes |
| Five-year WORM retention that even root cannot bypass | Versioning + Object Lock compliance-mode default retention |
| Mixed/unknown early access; archive inactive objects after 180 days; restore within six hours | Intelligent-Tiering with optional Archive Access at 180 days |
| Reproducible objects: daily 60 days, then infrequent, then rare with five-hour restore | Standard -> One Zone-IA -> Glacier Flexible Retrieval |
| Repetitive/high-volume S3 reads from compute in a second Region | CRR to a local bucket + local S3 gateway endpoint |
| Infrequent/low-volume private reads of a large S3 dataset from another Region | S3 interface endpoint reachable through inter-Region peering/TGW; avoid full data duplication |
| Legacy on-prem NFS/SMB clients and migrated apps must share one S3-backed dataset | S3 File Gateway for legacy clients; direct S3 for migrated apps |
| One bucket, several prefix-specific populations, easy future expansion | S3 access points with prefix-scoped policies and bucket delegation |
| Cross-Region protection for supported S3/EBS/EFS/RDS/Storage Gateway data | AWS Backup plan with copy action to a destination-Region vault |

## Databases

| Requirement | Best answer |
|---|---|
| SQL OLTP | RDS/Aurora |
| Cloud-optimized relational | Aurora |
| Global relational DR/read scaling | Aurora Global Database |
| Aurora PostgreSQL cross-Region DR with a 20-second-or-greater managed RPO | Aurora Global Database + `rds.global_db_rpo` |
| Key-value/document massive scale | DynamoDB |
| Global active-active NoSQL | DynamoDB Global Tables |
| Cache/session store | ElastiCache |
| Durable Redis-compatible primary database with microsecond reads | MemoryDB |
| Fault-tolerant RDS read cache with native Multi-AZ failover | ElastiCache for Redis OSS replication group, not Memcached |
| New Memcached nodes remain idle | Enable client Auto Discovery or update the node list/hash ring |
| Local secondary reads and forwarded writes with same-session read-after-write | Aurora Global Database write forwarding with `SESSION` consistency |
| Repeated eventually consistent reads of a limited DynamoDB key set | DAX; consider provisioned capacity + auto scaling for known load |
| Data warehouse | Redshift |
| Search/log analytics | OpenSearch |
| Graph | Neptune |

## Networking

| Requirement | Best answer |
|---|---|
| Many VPCs/accounts hub | Transit Gateway |
| Isolate production and development on one Transit Gateway | Separate TGW route tables and controlled association/propagation |
| Two/few VPCs direct private connection | VPC peering |
| Private AWS API access | VPC endpoints |
| AZ-resilient outbound internet for private subnets | One NAT Gateway per AZ; each subnet routes to its local NAT |
| Private service exposure | PrivateLink |
| Dedicated hybrid connection | Direct Connect |
| Encrypted internet tunnel | Site-to-Site VPN |
| Remote client VPN | AWS Client VPN |
| Audited interactive shell without bastion/port 22 | Systems Manager Session Manager |
| Fleet command execution | Systems Manager Run Command |
| Hybrid DNS | Route 53 Resolver endpoints/rules |
| Scalable on-prem VPN plus many mutually connected VPCs | Site-to-Site VPN attachment + VPC attachments on Transit Gateway; remove per-VPC VGWs |
| Same-Region Transit Gateways no longer require isolation | Consolidate VPC attachments and routes onto one Transit Gateway |
| TCP request accepted but reverse ephemeral-port tuple rejected | Fix the stateless NACL direction; do not change the stateful SG response path |

## Security/governance

| Requirement | Best answer |
|---|---|
| Multi-account guardrails | SCP |
| Shared OU guardrail plus extra restrictions for one population | Parent OU for common SCP; child OU for additional SCP |
| Centralized account baseline | Control Tower |
| Human federation | IAM Identity Center |
| Temporary cross-account access | IAM role assumption |
| One named principal may assume one cross-account role | Caller identity policy allows `sts:AssumeRole`; target role trust policy names that principal |
| Encryption key control | KMS |
| Secret rotation | Secrets Manager |
| API activity audit | CloudTrail |
| Config compliance | AWS Config |
| Threat detection | GuardDuty |
| Security findings aggregation | Security Hub |
| Multi-account, multi-Region security-finding reporting | Organizations-integrated Security Hub delegated administrator + members + home/linked Regions |
| Sensitive S3 data discovery | Macie |
| Web request filtering | WAF |
| DDoS protection | Shield |
| Organization-wide firewall policy rollout | Firewall Manager |
| Enforce approved SSH CIDRs at the organization network layer despite varied security groups | Firewall Manager Network Firewall policy + ordered stateless allow/default-drop logic |
| Audit/remediate disallowed SSH rules in existing security groups | Firewall Manager content audit SG policy using one allowed- or disallowed-rule model |
| S3 sensitive-data discovery | Macie |
| AWS compliance reports/agreements | Artifact |
| Customer-control audit evidence collection | Audit Manager |
| Customer application sign-up/sign-in | Cognito user pools |
| Guest AWS credentials plus a non-SAML/OIDC custom authenticated tier | One Cognito identity pool with guest and developer-authenticated flows |
| Find resource-policy access from outside the organization | IAM Access Analyzer with the organization as zone of trust |
| Least-complexity automatic rotation of a shared application database credential | Secrets Manager single-user rotation; retrieve the secret at runtime |
| Highest availability during database-secret rotation | Secrets Manager alternating-users rotation |
| Explicit single-tenant HSM/application cryptographic interface | CloudHSM |
| Managed Level 3 key service with scheduled cryptographic availability | KMS customer-managed key + scheduled disable/enable |
| Same service denied simultaneously across several member accounts after central policy work | Repair the common SCP through an authorized management-account principal |
| Centrally enforce Config rules with account exclusions | Organization conformance pack + trusted access/delegated administration |
| Detect alteration/deletion of CloudTrail files | CloudTrail log-file integrity validation |
| Notify an administrator when KMS deletion is scheduled | EventBridge match on `ScheduleKeyDeletion` -> SNS |
| Retire on-prem AD; LDAP/Microsoft AD and MFA remain required | AWS Managed Microsoft AD |
| WorkSpaces uses on-prem AD credentials without storing them in AWS | AD Connector in the WorkSpaces VPC over DX/VPN |
| Deny member-account root actions everywhere; Region-limit only development | Root-level root-user SCP + Dev-OU Region SCP |
| Standardize existing tags and require future tag presence | Organizations tag policy + Resource Groups compliance + service-side correction + SCP for supported create requests |
| Forecasted budget threshold must automatically stop named EC2 instances | Forecasted AWS Budget alert + Budgets action |
| Separate common environments and exceptional workloads at organization scale | Environment OUs with OU SCPs; separate security accounts; Exceptions OU for unique controls |
| Enforce daily EBS backup across current/future accounts | Complete Organizations backup policy using AWS Backup, attached at root |

## Operations and infrastructure automation

| Requirement | Best answer |
|---|---|
| Repeatable VPC/EC2/RDS plus initial application installation from a runbook | CloudFormation resources + EC2 user data or managed configuration bootstrap |
| Future infrastructure changes with dependency handling and rollback | CloudFormation stack updates/change sets, not manual console instructions |

## Disaster recovery coordination

| Requirement | Best answer |
|---|---|
| Low-cost automatic Regional failover from zero-capacity backup compute and an RDS read replica | Route 53 failover health check + alarm/SNS recovery Lambda that scales compute and promotes the replica |
| Traffic moves but backup compute/database are dormant | Incomplete: add recovery orchestration |
| Backup compute/database activate but users remain on the failed endpoint | Incomplete: add Route 53/Global Accelerator traffic failover |
| Fastest full-capacity RDS Regional recovery from a prepared secondary stack | Cross-Region read replica promotion + Route 53 failover; backup restore is slower |
| Routine Aurora global DR exercise with healthy Regions and minimum data loss | Switchover, formerly managed planned failover |

## Migration

| Requirement | Best answer |
|---|---|
| VMware inventory/utilization with no guest agent | Application Discovery Service Agentless Collector VMware module |
| VMware connection dependencies without installed agents | Agentless Collector plus Network Data Collection module; WinRM/SNMP access is still required |
| Physical/mixed servers with detailed processes and TCP connections | Application Discovery Agent |
| Import an existing server inventory | Migration Hub import |
| Group applications and track connected migration tools | Migration Hub |
| Rehost servers | MGN |
| Highly customized interdependent legacy servers, no modernization possible | AWS Transform MGN rehost |
| Physical Windows app has no source and hardcoded OS configuration | AWS Transform MGN rehost to EC2 |
| Db2 VM can change engine and should use a managed relational target | SCT/DMS Schema Conversion for schema + DMS for data/CDC to the supported RDS target |
| Db2 engine must remain unchanged or conversion risk is not accepted | Rehost the VM with MGN; assess managed database replatform separately |
| Migrate databases | DMS |
| Convert heterogeneous database schema | SCT |
| Move files/objects online with incremental copy and verification | DataSync |
| Move data offline | Snow Family |
| Hybrid protocol access | Storage Gateway |
