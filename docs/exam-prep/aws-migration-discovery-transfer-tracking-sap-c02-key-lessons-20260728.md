# AWS Migration Discovery, Server Rehosting, Data Transfer, and Tracking - SAP-C02 Key Lessons

<!-- markdownlint-disable MD013 MD060 -->

**Date:** 2026-07-28<br>
**Last revised:** 2026-08-05<br>
**Document role:** source-backed Domain 4 lesson covering Application Discovery Service, AWS Application Migration Service, AWS DataSync, AWS Migration Hub, and AWS Transform.<br>
**Evidence boundary:** this is revision material, not proof of blind recall, a completed migration wave, or deployed AWS resources.

## One End-to-End Mental Model

```text
discover estate and dependencies
    -> Application Discovery Service collectors, agents, or import
group servers into applications and plan waves
    -> Migration Hub home Region
move each workload with the appropriate engine
    -> AWS Application Migration Service (AWS MGN) for server rehosting
    -> DMS/SCT for databases
    -> DataSync for files, objects, and directories
track status across tools
    -> Migration Hub
accelerate selected migration and modernization paths
    -> AWS Transform for migrations, mainframe, or .NET
validate, cut over, and retire source
```

The five service families in this lesson have different jobs:

| Service | Mental shortcut | Does | Does not |
|---|---|---|---|
| Application Discovery Service | Learn the source estate | Collects server inventory, configuration, utilization, process, and connection evidence according to the collection method | Move servers or data |
| AWS Application Migration Service (AWS MGN; current documentation also calls it AWS Transform MGN) | Rehost servers | Continuously replicates supported source-server disks into an AWS staging area, then launches test and cutover EC2 instances | Refactor the application, convert a database schema, or provide ongoing DR by itself |
| AWS DataSync | Move storage data online | Copies files, objects, directories, metadata, and permitted changes between supported storage locations | Rehost a server or perform database-schema conversion |
| AWS Migration Hub | Portfolio control tower | Centralizes discovery views, application grouping, planning context, and migration status from connected tools | Perform the underlying server, database, or file transfer by itself |
| AWS Transform | AI-assisted transformation workspace | Analyses supported source estates or codebases, proposes plans, and orchestrates supported migration or modernization workflows with human review | Make every workload compatible, replace specialist transfer engines, or prove the transformed application is production-ready |

## 1. AWS Application Discovery Service

### Choose the Collection Method From the Constraint

| Requirement or source environment | Preferred method | Evidence depth and caveat |
|---|---|---|
| Physical servers or mixed hypervisors; detailed processes and TCP connections required | AWS Application Discovery Agent | Install on each target host; collects the deepest host-level evidence |
| VMware vCenter; guest agents prohibited; inventory, profile, and utilization required | Agentless Collector VMware vCenter module | Deploy one OVA appliance; uses VMware metadata and does not install software in each guest |
| VMware vCenter; guest agents prohibited; source/destination IP and port dependencies required | Agentless Collector VMware module **plus Network Data Collection module** | No guest agent is installed, but the network module needs remote access and credentials: WinRM for Windows and SNMP for Linux |
| Database and analytics inventory plus target recommendations | Agentless Collector database and analytics module | Collects supported database metadata/utilization and integrates with AWS DMS recommendations |
| Collectors cannot be deployed but an inventory already exists | Migration Hub import | Imports supplied server data; fidelity is limited to the imported fields |

### Discovery Agent

Install Discovery Agent on each supported physical server or VM when the
scenario requires detailed operating-system evidence. It can collect:

- system configuration and performance time series;
- running processes; and
- TCP network connections used to identify server dependencies.

This is the strongest answer for a mixed physical/virtual estate or when
process-level dependency evidence is explicit. The trade-offs are per-host
deployment, privileges, security approval, and lifecycle management.

### Agentless Collector for VMware

Agentless Collector is deployed as an OVA virtual appliance in the VMware
vCenter environment. The VMware module supplies inventory, profile, and
utilization information for discovered VMs without installing an agent in each
guest operating system.

Do not merge two separate capabilities:

```text
VMware vCenter module
    -> inventory, VM profile, utilization, VMware metadata

Network Data Collection module
    -> dependencies expressed as source IP -> destination IP/port
    -> uses the VMware-discovered server inventory
    -> uses WinRM for Windows or SNMP for Linux
```

The network module remains agentless because it installs no guest software,
but “agentless” does not mean “no credentials, ports, or access to guest
telemetry.” Security policy must allow the required least-privilege remote
collection path.

### Review of the Supplied VMware Question

**Best answer: A, with a corrected explanation.** Deploy Agentless Collector
when VMware vCenter is present and guest-agent installation is prohibited. The
VMware module satisfies the inventory requirement. For dependency mapping,
enable and configure the separate Network Data Collection module.

The supplied phrase “maps dependencies via hypervisor APIs” is too broad. The
vCenter module itself collects VMware inventory/profile/utilization data. The
current network module connects to the vCenter-discovered servers and uses
WinRM or SNMP to collect connection information. If security policy also
forbids those protocols, credentials, or ports, the architecture can collect
vCenter inventory but cannot claim the same connection-level dependency map.

### Discovery Data Destination

Choose the Migration Hub home Region before registering collectors. Discovery
Agent and Agentless Collector send discovery data to that home Region, where
servers can be reviewed and grouped into applications. Migration Hub import is
the alternative when discovery data already exists.

## 2. AWS Application Migration Service (AWS MGN)

### Clean Mental Model

AWS Application Migration Service is the server **rehost** engine. AWS commonly
abbreviates it as **AWS MGN**, and current documentation and console text also
use **AWS Transform MGN**. For SAP-C02, treat these names as the same underlying
server-migration capability, not as the whole AWS Transform service.

```text
physical, virtual, or cloud source server
    -> continuous block-level replication
AWS staging area
    -> replication server and staging EBS volumes
launch settings
    -> test EC2 instance
    -> validate application and dependencies
    -> mark ready for cutover
    -> launch cutover EC2 instance
    -> validate, finalize, and archive
```

The staging area keeps migration infrastructure separate from the final target
instance. MGN converts the replicated disks into launchable snapshots and uses
the configured launch settings to create EC2 test and cutover instances. The
source can continue running while block changes are replicated, reducing the
final cutover window.

### Lifecycle and Control Points

```text
Not ready
    -> initial sync completes and replication becomes healthy
Ready for testing
    -> launch a non-disruptive test instance
Test in progress
    -> validate boot, application, networking, security, and dependencies
Ready for cutover
    -> confirm replication remains healthy and launch the cutover instance
Cutover in progress
    -> validate the target before finalization
Cutover complete
    -> replication is stopped; archive the source-server record when appropriate
```

Testing does not by itself end continuous replication from the source. Mark the
server ready for cutover only after the test is accepted. Finalize the cutover
only after the target instance has passed the agreed validation: finalization
disconnects replication and cleans up the AWS replication resources while
leaving launched test or cutover instances in place.

Before finalization, MGN can return a server to an earlier testing or cutover
state when another launch is required. Do not treat finalization as a routine
button press before rollback and acceptance decisions are complete.

### Replication Settings Versus Launch Settings

| Settings | Control |
|---|---|
| Replication settings | Staging-area subnet, replication-server choices, EBS staging volumes, bandwidth and routing/security path used for replication |
| Launch settings and EC2 launch template | Target subnet, instance type/right-sizing choice, security groups, disks, licensing choices, tags, and related target-instance configuration |
| Post-launch actions | Optional Systems Manager-based actions and operational steps applied after a test or cutover instance launches |

The migration is not validated merely because an EC2 instance launches. Test
the application, identity and secrets, DNS, load balancer registration,
database connectivity, monitoring, backup, performance, and business behavior.

### MGN Decision Boundaries

| Requirement | Choose | Why |
|---|---|---|
| Rehost physical, VMware, Hyper-V, or cloud servers on EC2 with minimal application change and a short cutover | AWS MGN | Replicates server disks continuously and provides test/cutover launch workflow |
| Discover inventory, processes, utilization, and network dependencies | Application Discovery Service | Discovery evidence rather than the rehost engine |
| Convert database schemas and replicate full load or CDC | AWS SCT or applicable schema-conversion tooling plus AWS DMS | Database-aware conversion and replication |
| Copy files, objects, and directories without migrating a bootable server | DataSync | Storage-data transfer rather than server rehosting |
| Track portfolio progress across MGN, DMS, and other connected tools | Migration Hub | Central tracking rather than execution |
| Analyse an estate, create application groups and waves, translate network configuration, and guide rehosting | AWS Transform for migrations | Broader assisted planning and orchestration that can use MGN for the rehost step |
| Maintain ongoing recoverability of servers after migration | AWS Elastic Disaster Recovery | DR is a continuing recovery capability; MGN is a migration and cutover service |

### High-Yield Exam Traps

1. **MGN means rehost, not refactor.** It moves the server workload with minimal
   application change; it does not modernize the code or database architecture.
2. **Test before cutover.** A healthy initial sync makes the server ready for
   testing, not automatically production-ready.
3. **A test launch does not stop source replication.** Continue replicating
   changes until the controlled cutover is accepted and finalized.
4. **Launch settings matter.** Replicated disks alone do not select the right
   VPC, subnet, security groups, instance sizing, or operational controls.
5. **MGN is not DataSync or DMS.** Choose by the object being moved: server,
   storage data, or database data/schema.
6. **MGN is not the default ongoing-DR answer.** Use AWS Elastic Disaster
   Recovery when the requirement is sustained recovery readiness rather than a
   finite migration and cutover.

## 3. AWS DataSync

### What DataSync Moves

DataSync is an online storage-transfer service. Common paths include:

- on-premises NFS or SMB to Amazon S3, EFS, or supported FSx services;
- HDFS or S3-compatible object storage to supported AWS storage;
- supported AWS storage services within or across Regions/accounts; and
- supported storage in other clouds to or from AWS.

Use current documentation to confirm the exact source/destination, account,
Region, and task-mode combination; support is not uniform across every pair.

### Four Core Components

```text
agent, when required
    -> reaches self-managed/on-premises storage
source and destination locations
    -> identify storage endpoints and access
task
    -> transfer rules, metadata, verification, filters, bandwidth
task execution
    -> one actual run with metrics, logs, and result
```

An agent is normally required when self-managed or on-premises storage is
involved. Same-account AWS-to-AWS transfers normally do not require one.
Agentless support exists for specific S3 and other-cloud combinations, so do
not memorize “DataSync always requires an agent.”

### Transfer Behaviour

DataSync can:

- compare source and destination and transfer changed data;
- run once or on a schedule;
- preserve supported file/object metadata and permissions;
- include or exclude selected paths;
- throttle bandwidth;
- control treatment of files deleted from the source;
- encrypt transfer traffic with TLS;
- verify integrity using checksums and metadata; and
- publish logs, metrics, and task reports for validation.

It does not lock source files. If a file changes while it is copied,
verification can detect the inconsistency and a later execution may be needed.

### Basic Versus Enhanced Task Mode

| Dimension | Basic mode | Enhanced mode |
|---|---|---|
| Processing | Prepare, transfer, and verify sequentially | List, prepare, transfer, and verify in parallel |
| Dataset size | Item quotas apply | Virtually unlimited object count per execution |
| Location coverage | All supported DataSync location combinations | A narrower current set, including supported S3 and NFS/SMB-to-S3 paths |
| Logs and counters | Fewer counters; unstructured logs | More counters; structured JSON logs |
| Verification default | Can verify the full dataset | Verifies transferred data |

Task mode cannot be changed after task creation. Choose it only after verifying
that the source/destination pair supports it.

### Practical Migration Sequence

```text
1. baseline copy
2. scheduled incremental transfers
3. monitor failures, locked files, metadata, and throughput
4. quiesce writes or define the application cutover boundary
5. final changed-data transfer
6. verify destination and application behavior
7. cut over consumers
8. retain rollback evidence before retiring the source
```

### DataSync Decision Boundaries

| Requirement | Choose | Why DataSync loses or wins |
|---|---|---|
| Rehost complete servers with boot volumes | MGN | DataSync moves storage data, not a bootable server image and launch workflow |
| Database full load and ongoing CDC | DMS | DataSync is file/object transfer, not database-aware change replication |
| Online NFS/SMB/HDFS/object migration with incremental runs and verification | DataSync | Direct fit |
| Offline migration when network capacity is inadequate | Snow Family | DataSync consumes a network path |
| Keep presenting NFS/SMB/iSCSI/tape interfaces during hybrid operation | Storage Gateway | Gateway is ongoing hybrid access; DataSync is transfer |
| Copy an ad hoc object over HTTP into S3 | S3 transfer mechanisms | DataSync can be unnecessary overhead for a simple object upload |

Direct Connect or VPN can provide the network path, but neither performs the
copy, metadata handling, scheduling, or verification.

## 4. AWS Migration Hub

### Core Role

Migration Hub provides a central place to discover servers, group them into
applications, plan and observe migrations, and track progress reported by
connected AWS or partner tools. It can show application-level status even when
different components use different migration engines.

```text
Application Discovery Service / import
    -> discovered servers and dependencies
Migration Hub
    -> application grouping and portfolio view
MGN / DMS / connected tools
    -> perform migration and report status
Migration Hub
    -> aggregated progress and metrics
```

### Home Region

- Select one Migration Hub home Region before discovery or write operations.
- Discovery and migration-tracking data is stored and viewed there.
- Application Discovery Service and Migration Hub API calls must use the home
  Region.
- The workloads can be migrated to other Regions supported by the selected
  migration tool; the home Region controls tracking data, not destination
  placement.

### Tool Authorization and Grouping

Connecting a migration tool authorizes it to send status into Migration Hub.
If it is not connected, do not assume Hub will automatically observe its
progress. Servers can be grouped into logical applications before or while
migration proceeds.

Migration Hub can use discovery/import data to visualize connections and help
plan waves, but it cannot infer evidence that was never collected or imported.

### Related Capabilities

- **Strategy Recommendations:** evaluates portfolio data and proposes viable
  migration or modernization strategies.
- **Migration Hub Journeys:** supports planning, performing, and tracking a
  migration programme through tasks and collaboration.
- **Refactor Spaces:** supports incremental application refactoring patterns;
  it is not the default lift-and-shift answer.

For exam questions, first identify whether the requirement is **discover**,
**recommend**, **move**, **or track**. “Migration Hub” alone is usually wrong
when the scenario asks for the actual server, database, or storage transfer.

## 5. AWS Transform

### Clean Mental Model

```text
source estate or codebase
    -> discover and analyse
AWS Transform
    -> propose grouping, migration, or modernization plan
human review and approvals
    -> run supported transformation workflow
specialist migration/runtime services
    -> rehost, build, test, and cut over
```

AWS Transform is a generative-AI-assisted migration and modernization service.
For SAP-C02, recognize the workload family and the outcome requested; do not
memorize it as a universal replacement for every migration service.

### Current Workload Families

| Workload family | What AWS Transform contributes | Important boundary |
|---|---|---|
| Server-environment migrations, including VMware, Hyper-V, virtual, and bare-metal sources | Discovery-data ingestion, application grouping, migration-wave planning, source-network-to-AWS network mapping, and rehosting to EC2 | The rehost workflow uses AWS Transform MGN capabilities; MGN remains the server replication, test-launch, and cutover engine |
| Mainframe modernization | Codebase analysis, documentation, business-logic extraction, decomposition, planning, and supported code transformation; current capabilities include refactoring supported COBOL workloads toward cloud-optimized Java | Human review remains part of the process; generated artifacts do not prove semantic equivalence, performance, security, or cutover success |
| .NET modernization | Analyses dependencies and helps port supported .NET Framework applications to cross-platform .NET for Linux, with build/test feedback and transformation reports | Unsupported dependencies and Windows-specific behaviour can require manual work; review and testing remain mandatory |

### Transform Versus the Neighbouring Services

| Scenario cue | Choose first | Why |
|---|---|---|
| Replicate an unchanged physical or virtual server and launch it on EC2 with minimal downtime | AWS Application Migration Service / AWS Transform MGN workflow | Rehost with continuous block-level replication, test launch, and controlled cutover |
| Analyse a large source estate, translate network configuration, group applications, create waves, and guide rehosting | AWS Transform for migrations | Broader assisted migration workflow that coordinates planning and rehosting capabilities |
| Collect host inventory, processes, utilization, and connection evidence | Application Discovery Service | Discovery evidence, not transformation |
| Track progress across connected migration tools | Migration Hub | Portfolio tracking, not the transfer engine |
| Convert database schema/code and replicate database changes | AWS SCT or applicable schema-conversion tooling plus AWS DMS | Database-aware conversion and full-load/CDC boundary |
| Copy files, objects, and directories with incremental verification | DataSync | Storage-data transfer boundary |
| Analyse and modernize supported COBOL or .NET code | AWS Transform for the corresponding workload family | Code and application modernization rather than unchanged rehosting |

### Exam Traps

1. **AWS Transform is not just a renamed MGN.** The migration experience can
   use MGN capabilities for server rehosting, while Transform adds discovery,
   planning, network translation, wave orchestration, and review workflows.
2. **Mainframe transformation is not lift-and-shift.** Analysis,
   decomposition, business-rule extraction, and code refactoring indicate a
   modernization path.
3. **Generated code or infrastructure is not acceptance evidence.** Preserve
   human approval, testing, security validation, dependency checks, rollback,
   and cutover controls.
4. **Choose by workload type.** A database CDC requirement still points to
   DMS; a NAS copy still points to DataSync; cross-tool status still points to
   Migration Hub.
5. **Check current support.** Workload types, source formats, Regions, quotas,
   and transformation limits can change; use the scenario's stated support
   constraints rather than assuming universal coverage.

## High-Value Scenario Matrix

| Scenario cue | Best starting answer |
|---|---|
| VMware inventory with no guest agents | Agentless Collector VMware module |
| VMware connection dependencies with no installed agents, but approved WinRM/SNMP access | Agentless Collector plus Network Data Collection module |
| Physical servers with detailed process/TCP dependency evidence | Discovery Agent on each server |
| Existing CMDB spreadsheet and no collector deployment | Migration Hub import |
| Physical, virtual, or cloud servers need minimally changed EC2 rehosting with test and controlled cutover | AWS MGN |
| Ongoing server recovery readiness after migration | AWS Elastic Disaster Recovery |
| Online NAS migration with incremental copy and verification | DataSync |
| Central status across MGN and DMS migration waves | Migration Hub with connected/authorized tools |
| Transformation-path recommendations | Migration Hub Strategy Recommendations |
| AI-assisted VMware/server discovery, network mapping, wave planning, and EC2 rehosting | AWS Transform for migrations |
| Supported COBOL estate needs analysis, decomposition, and code modernization | AWS Transform for mainframe |
| Supported .NET Framework estate must move toward cross-platform .NET on Linux | AWS Transform for .NET |

## Recall Check

Answer closed book:

1. Which Agentless Collector module provides vCenter inventory and utilization?
2. Which module provides source/destination IP and port dependencies, and what
   guest access does it use?
3. When does Discovery Agent beat Agentless Collector?
4. What does MGN replicate, and what does it deliberately not transform?
5. Walk through the MGN lifecycle from initial sync to cutover complete.
6. Why are test launch, cutover launch, and finalize cutover separate actions?
7. When does Elastic Disaster Recovery win over MGN?
8. What are DataSync's agent, location, task, and task-execution roles?
9. When does DataSync require an agent, and when might it not?
10. Why are DataSync, DMS, MGN, Snow Family, and Storage Gateway not synonyms?
11. What does the Migration Hub home Region control?
12. Why must a migration tool be connected to Migration Hub?
13. What can Migration Hub track that it cannot itself migrate?
14. When does AWS Transform for migrations win over selecting MGN alone?
15. What does AWS Transform for mainframe do that a rehost tool does not?
16. Why do Transform output and a successful transformation job not prove a
    production-ready migration?

## Official AWS References

- [Application Discovery Service Agentless Collector](https://docs.aws.amazon.com/application-discovery/latest/userguide/agentless-collector.html)
- [VMware vCenter Agentless Collector module](https://docs.aws.amazon.com/application-discovery/latest/userguide/agentless-collector-gs-data-collection-vcenter.html)
- [Agentless Collector Network Data Collection module](https://docs.aws.amazon.com/application-discovery/latest/userguide/agentless-collector-gs-network-data-collection.html)
- [Network Data Collection credentials and protocols](https://docs.aws.amazon.com/application-discovery/latest/userguide/network-data-module-setup.html)
- [AWS Application Discovery Agent](https://docs.aws.amazon.com/application-discovery/latest/userguide/discovery-agent.html)
- [Monitor the AWS MGN migration lifecycle](https://docs.aws.amazon.com/mgn/latest/ug/migration-dashboard.html)
- [Launch an AWS MGN test instance](https://docs.aws.amazon.com/mgn/latest/ug/starting-test.html)
- [AWS MGN ready-for-cutover indicators](https://docs.aws.amazon.com/mgn/latest/ug/ready-for-cutover.html)
- [FinalizeCutover API behavior](https://docs.aws.amazon.com/mgn/latest/APIReference/API_FinalizeCutover.html)
- [Revert or finalize an AWS MGN cutover](https://docs.aws.amazon.com/mgn/latest/ug/revert-finalize-cutover.html)
- [How AWS DataSync works](https://docs.aws.amazon.com/datasync/latest/userguide/how-datasync-transfer-works.html)
- [When a DataSync agent is required](https://docs.aws.amazon.com/datasync/latest/userguide/do-i-need-datasync-agent.html)
- [DataSync supported transfer combinations](https://docs.aws.amazon.com/datasync/latest/userguide/working-with-locations.html)
- [DataSync Basic and Enhanced task modes](https://docs.aws.amazon.com/datasync/latest/userguide/choosing-task-mode.html)
- [What is AWS Migration Hub?](https://docs.aws.amazon.com/migrationhub/latest/ug/whatishub.html)
- [Migration Hub discovery and home Region](https://docs.aws.amazon.com/migrationhub/latest/ug/home-region-with-discovery.html)
- [Connect migration tools and track status](https://docs.aws.amazon.com/migrationhub/latest/ug/gs-new-user-migration.html)
- [AWS Transform migrations, including VMware](https://docs.aws.amazon.com/transform/latest/userguide/transform-app-vmware.html)
- [AWS Transform server migration workflow](https://docs.aws.amazon.com/transform/latest/userguide/transform-vmware-migrate-servers.html)
- [AWS Transform network migration](https://docs.aws.amazon.com/transform/latest/userguide/transform-vmware-migrate-network.html)
- [AWS Transform for mainframe](https://docs.aws.amazon.com/transform/latest/userguide/transform-app-mainframe.html)
- [AWS Transform for .NET](https://docs.aws.amazon.com/transform/latest/userguide/dotnet.html)
