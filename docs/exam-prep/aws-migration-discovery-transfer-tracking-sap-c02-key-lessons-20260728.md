# AWS Migration Discovery, Data Transfer, and Tracking - SAP-C02 Key Lessons

<!-- markdownlint-disable MD013 MD060 -->

**Date:** 2026-07-28<br>
**Last revised:** 2026-07-28<br>
**Document role:** source-backed Domain 4 lesson covering Application Discovery Service, AWS DataSync, and AWS Migration Hub.<br>
**Evidence boundary:** this is revision material, not proof of blind recall, a completed migration wave, or deployed AWS resources.

## One End-to-End Mental Model

```text
discover estate and dependencies
    -> Application Discovery Service collectors, agents, or import
group servers into applications and plan waves
    -> Migration Hub home Region
move each workload with the appropriate engine
    -> MGN for servers
    -> DMS/SCT for databases
    -> DataSync for files, objects, and directories
track status across tools
    -> Migration Hub
validate, cut over, and retire source
```

The three services in this lesson have different jobs:

| Service | Mental shortcut | Does | Does not |
|---|---|---|---|
| Application Discovery Service | Learn the source estate | Collects server inventory, configuration, utilization, process, and connection evidence according to the collection method | Move servers or data |
| AWS DataSync | Move storage data online | Copies files, objects, directories, metadata, and permitted changes between supported storage locations | Rehost a server or perform database-schema conversion |
| AWS Migration Hub | Portfolio control tower | Centralizes discovery views, application grouping, planning context, and migration status from connected tools | Perform the underlying server, database, or file transfer by itself |

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

## 2. AWS DataSync

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

## 3. AWS Migration Hub

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

## High-Value Scenario Matrix

| Scenario cue | Best starting answer |
|---|---|
| VMware inventory with no guest agents | Agentless Collector VMware module |
| VMware connection dependencies with no installed agents, but approved WinRM/SNMP access | Agentless Collector plus Network Data Collection module |
| Physical servers with detailed process/TCP dependency evidence | Discovery Agent on each server |
| Existing CMDB spreadsheet and no collector deployment | Migration Hub import |
| Online NAS migration with incremental copy and verification | DataSync |
| Central status across MGN and DMS migration waves | Migration Hub with connected/authorized tools |
| Transformation-path recommendations | Migration Hub Strategy Recommendations |

## Recall Check

Answer closed book:

1. Which Agentless Collector module provides vCenter inventory and utilization?
2. Which module provides source/destination IP and port dependencies, and what
   guest access does it use?
3. When does Discovery Agent beat Agentless Collector?
4. What are DataSync's agent, location, task, and task-execution roles?
5. When does DataSync require an agent, and when might it not?
6. Why are DataSync, DMS, MGN, Snow Family, and Storage Gateway not synonyms?
7. What does the Migration Hub home Region control?
8. Why must a migration tool be connected to Migration Hub?
9. What can Migration Hub track that it cannot itself migrate?

## Official AWS References

- [Application Discovery Service Agentless Collector](https://docs.aws.amazon.com/application-discovery/latest/userguide/agentless-collector.html)
- [VMware vCenter Agentless Collector module](https://docs.aws.amazon.com/application-discovery/latest/userguide/agentless-collector-gs-data-collection-vcenter.html)
- [Agentless Collector Network Data Collection module](https://docs.aws.amazon.com/application-discovery/latest/userguide/agentless-collector-gs-network-data-collection.html)
- [Network Data Collection credentials and protocols](https://docs.aws.amazon.com/application-discovery/latest/userguide/network-data-module-setup.html)
- [AWS Application Discovery Agent](https://docs.aws.amazon.com/application-discovery/latest/userguide/discovery-agent.html)
- [How AWS DataSync works](https://docs.aws.amazon.com/datasync/latest/userguide/how-datasync-transfer-works.html)
- [When a DataSync agent is required](https://docs.aws.amazon.com/datasync/latest/userguide/do-i-need-datasync-agent.html)
- [DataSync supported transfer combinations](https://docs.aws.amazon.com/datasync/latest/userguide/working-with-locations.html)
- [DataSync Basic and Enhanced task modes](https://docs.aws.amazon.com/datasync/latest/userguide/choosing-task-mode.html)
- [What is AWS Migration Hub?](https://docs.aws.amazon.com/migrationhub/latest/ug/whatishub.html)
- [Migration Hub discovery and home Region](https://docs.aws.amazon.com/migrationhub/latest/ug/home-region-with-discovery.html)
- [Connect migration tools and track status](https://docs.aws.amazon.com/migrationhub/latest/ug/gs-new-user-migration.html)
