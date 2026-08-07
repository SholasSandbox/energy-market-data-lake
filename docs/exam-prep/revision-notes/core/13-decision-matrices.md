# 13 - Decision Matrices

**Last revised:** 2026-07-28

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

## Compute

| Requirement | Best answer |
|---|---|
| Short event-driven code | Lambda |
| Container web service, simple ops | ECS Fargate |
| Kubernetes required | EKS |
| Host-level control/special hardware | EC2/ECS on EC2/EKS nodes |
| Batch job queue | AWS Batch |
| Simple PaaS app deployment | Elastic Beanstalk / App Runner depending app |
| Edge function lightweight HTTP manipulation | CloudFront Functions / Lambda@Edge |

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

## Storage

| Requirement | Best answer |
|---|---|
| Object storage/data lake | S3 |
| Shared Linux file system | EFS |
| Windows SMB file share | FSx for Windows |
| HPC scratch | FSx for Lustre |
| Hybrid NFS/SMB/iSCSI/tape | Storage Gateway |
| Online file transfer | DataSync |
| Offline bulk transfer | Snow Family |
| SFTP/FTPS/FTP managed transfer | Transfer Family |

## Databases

| Requirement | Best answer |
|---|---|
| SQL OLTP | RDS/Aurora |
| Cloud-optimized relational | Aurora |
| Global relational DR/read scaling | Aurora Global Database |
| Key-value/document massive scale | DynamoDB |
| Global active-active NoSQL | DynamoDB Global Tables |
| Cache/session store | ElastiCache |
| Data warehouse | Redshift |
| Search/log analytics | OpenSearch |
| Graph | Neptune |

## Networking

| Requirement | Best answer |
|---|---|
| Many VPCs/accounts hub | Transit Gateway |
| Two/few VPCs direct private connection | VPC peering |
| Private AWS API access | VPC endpoints |
| Private service exposure | PrivateLink |
| Dedicated hybrid connection | Direct Connect |
| Encrypted internet tunnel | Site-to-Site VPN |
| Remote client VPN | AWS Client VPN |
| Hybrid DNS | Route 53 Resolver endpoints/rules |

## Security/governance

| Requirement | Best answer |
|---|---|
| Multi-account guardrails | SCP |
| Centralized account baseline | Control Tower |
| Human federation | IAM Identity Center |
| Temporary cross-account access | IAM role assumption |
| Encryption key control | KMS |
| Secret rotation | Secrets Manager |
| API activity audit | CloudTrail |
| Config compliance | AWS Config |
| Threat detection | GuardDuty |
| Security findings aggregation | Security Hub |
| Sensitive S3 data discovery | Macie |
| Web request filtering | WAF |
| DDoS protection | Shield |

## Migration

| Requirement | Best answer |
|---|---|
| VMware inventory/utilization with no guest agent | Application Discovery Service Agentless Collector VMware module |
| VMware connection dependencies without installed agents | Agentless Collector plus Network Data Collection module; WinRM/SNMP access is still required |
| Physical/mixed servers with detailed processes and TCP connections | Application Discovery Agent |
| Import an existing server inventory | Migration Hub import |
| Group applications and track connected migration tools | Migration Hub |
| Rehost servers | MGN |
| Migrate databases | DMS |
| Convert heterogeneous database schema | SCT |
| Move files/objects online with incremental copy and verification | DataSync |
| Move data offline | Snow Family |
| Hybrid protocol access | Storage Gateway |
