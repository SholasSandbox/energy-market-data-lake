# 10 - Migration and Modernization

**Last revised:** 2026-07-28

Domain 4 is 20% of SAP-C02 and heavily scenario-based. The exam tests whether you can map workload constraints to the correct migration strategy and tool.

## The 7 Rs

| Strategy | Meaning | Example |
|---|---|---|
| Retire | Decommission | Unused app removed |
| Retain | Keep as-is for now | Legacy app with dependency risk |
| Rehost | Lift and shift | Move VM to EC2 with minimal change |
| Replatform | Minor optimization | Move database to RDS, app mostly unchanged |
| Repurchase | Replace with SaaS | CRM to SaaS |
| Refactor/Re-architect | Significant code/architecture change | Monolith to microservices/serverless |
| Relocate | Move VMware workloads to VMware Cloud on AWS | Hypervisor-level move |

## Tool chooser

| Requirement | Tool/service |
|---|---|
| Discover on-prem servers/apps/dependencies | Application Discovery Service |
| Track migration portfolio | Migration Hub |
| Lift-and-shift servers | Application Migration Service (MGN) |
| Database migration | Database Migration Service (DMS) |
| Heterogeneous DB schema conversion | Schema Conversion Tool (SCT) |
| Large file/object transfer online | DataSync |
| Offline bulk data transfer | Snow Family |
| SFTP/FTPS/FTP managed endpoint | Transfer Family |
| Hybrid file access to cloud-backed storage | Storage Gateway |
| Mainframe modernization | AWS Mainframe Modernization and partner tooling |
| Container modernization | ECS/EKS/App Runner depending requirements |

## Application Discovery Service

| Environment or evidence need | Discovery choice |
|---|---|
| Detailed process and inbound/outbound TCP connection data from physical servers or VMs | Install AWS Application Discovery Agent on each target server |
| VMware inventory, profile, and utilization with no guest-agent installation | Agentless Collector VMware vCenter module |
| VMware source/destination IP and port dependencies with no installed agent | VMware module plus Network Data Collection module; allow its remote WinRM or SNMP collection path |
| Supported database/analytics inventory and target recommendations | Agentless Collector database and analytics module |
| Existing inventory when collectors cannot be deployed | Migration Hub import |
| Group discovered servers into applications and visualize/export relationships | Application Discovery Service data through Migration Hub and supported data-exploration views |

For a question that explicitly says **physical servers** and requires detailed host-level network dependencies, the Discovery Agent is the safest exam answer. It captures running processes and TCP connections from the operating system.

For VMware with an agent prohibition, deploy Agentless Collector as an OVA appliance. Its vCenter module collects inventory/profile/utilization through VMware metadata. Connection-level dependency mapping is a separate module: Network Data Collection uses the VMware-discovered inventory, then collects source/destination IP and port evidence through WinRM for Windows or SNMP for Linux. No guest software is installed, but credentials, ports, and least-privilege remote access are still required.

Therefore the answer “Agentless Collector” is correct for the supplied VMware/no-agent scenario, but “maps dependencies via hypervisor APIs” is incomplete. The hypervisor-facing module supplies inventory; the network module supplies connection dependencies. If WinRM/SNMP is also prohibited, do not claim full connection-level mapping.

Terminology matters: the older **Agentless Discovery Connector** is not the current **Agentless Collector**. Choose from the named tool, environment, permitted access, and required evidence depth.

## AWS Migration Hub

Migration Hub is the portfolio view, not the migration engine. It can centralize discovered servers, group them into applications, visualize available connection evidence, and track status reported by connected AWS or partner migration tools.

- Select one Migration Hub home Region before discovery/write operations.
- Discovery and tracking data is stored in the home Region; workloads may migrate to other Regions supported by the selected tool.
- Discovery Agent, Agentless Collector, and Migration Hub import can populate inventory.
- Connect/authorize MGN, DMS, or another supported tool before assuming its status will appear.
- Strategy Recommendations helps select migration/modernization paths.

Trap: Migration Hub can organize and track a migration wave, but MGN, DMS, DataSync, or another engine performs the move.

## DMS

Choose DMS when:

- migrating relational/document/noSQL data between supported sources and targets
- continuous replication/CDC is required
- minimal downtime database migration is needed
- homogeneous or heterogeneous database migration is required

DMS migration types:

| Type | Use |
|---|---|
| Full load | One-time initial copy |
| Full load + CDC | Initial copy plus ongoing changes for low downtime |
| CDC only | Replicate changes after separate initial load |

Trap: DMS does not magically convert incompatible schemas or application SQL. Use SCT for heterogeneous schema conversion.

## SCT

Choose Schema Conversion Tool when:

- converting Oracle to PostgreSQL/Aurora PostgreSQL
- converting SQL Server to MySQL/PostgreSQL
- assessing schema conversion complexity
- converting stored procedures/functions where supported

Trap: SCT converts/assesses schema; DMS moves data.

## MGN

Choose Application Migration Service when:

- rehosting servers to AWS
- block-level replication is desired
- minimal application change is required
- cutover testing and launch templates are part of migration

Trap: MGN is not database-specific logical migration. DMS is for data/database migration.

## DataSync

Choose DataSync when:

- file/object datasets must move repeatedly or on schedule
- on-prem NAS to S3/EFS/FSx migration
- AWS-to-AWS storage transfer
- validation and metadata preservation matter

Core model:

```text
agent when required -> source location -> task -> destination location
task execution -> prepare -> transfer -> verify
```

An agent is normally required for on-premises/self-managed NFS, SMB, HDFS, or object storage. Same-account AWS-to-AWS transfers normally need no agent, and some S3/other-cloud combinations are agentless. DataSync does not always require an agent.

Tasks can schedule incremental transfers, preserve supported metadata, filter paths, limit bandwidth, control destination deletions, encrypt traffic with TLS, and verify checksums/metadata. It does not lock files; rerun the task if an actively written file fails verification.

Basic mode supports all DataSync location combinations but has dataset-item quotas and sequential preparation/transfer/verification. Enhanced mode supports a narrower current set, processes phases in parallel, and supports virtually unlimited object counts. Task mode cannot be changed after task creation.

Trap: DataSync is not DMS, MGN, Snow Family, or Storage Gateway. It moves storage data online; it does not convert schemas, rehost bootable servers, solve an inadequate network path, or provide the ongoing hybrid protocol endpoint.

## Snow Family

Choose Snow when:

- network bandwidth is insufficient
- massive one-time transfer is needed
- edge/disconnected compute is needed
- physically secure device workflow is acceptable

Trap: Snow is not for low-latency continuous replication.

## Storage Gateway

Choose Storage Gateway when:

- on-prem applications must keep using NFS/SMB/iSCSI/tape protocols
- data should be backed by AWS storage
- hybrid access/caching is required during transition

## Migration architecture checklist

| Area | Questions |
|---|---|
| Identity | How will users/service accounts authenticate after migration? |
| Network | VPC CIDRs, DNS, routing, firewall, hybrid links, latency |
| Data | Size, change rate, consistency, cutover, rollback |
| App | Dependencies, hardcoded IPs, licensing, OS support |
| Security | Encryption, key ownership, logging, compliance |
| Operations | Monitoring, backup, patching, runbooks, support model |
| Cost | Transfer, duplicate run period, licensing, right-sizing |
| DR | RTO/RPO before, during, and after migration |

## Modernization patterns

| Existing state | Modernization path |
|---|---|
| VM-hosted stateless app | Containerize to ECS/Fargate |
| Cron jobs | EventBridge Scheduler + Lambda/ECS/Batch |
| Monolith with async work | Extract queue-backed workers using SQS |
| File uploads processed synchronously | S3 event -> SQS/Lambda/ECS |
| Legacy API | API Gateway/ALB front door, strangler pattern |
| Self-managed DB | RDS/Aurora/DynamoDB depending model |
| Batch ETL | Glue/EMR/Batch |
| Kafka dependency | MSK |
| Clickstream | Kinesis Data Streams/Firehose/Flink |

## Exam traps

| Trap | Correction |
|---|---|
| “Use DMS for server migration” | Use MGN for servers; DMS for databases. |
| “Use SCT alone for database migration” | SCT converts schema; DMS migrates data. |
| “Snowball for continuous sync” | Use DataSync/replication/Direct Connect. |
| “Rehost is always best” | It is fastest but may preserve technical debt. |
| “Refactor during every migration” | Higher risk and time; choose based on business drivers. |
| “Migration Hub migrates workloads” | It tracks migration progress. |
| “Agentless discovery can never map network dependencies” | The current Agentless Collector has a network module for supported VMware-discovered servers; use Discovery Agent for detailed host-level process/TCP evidence and physical-server coverage. |
