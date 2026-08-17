# 06 - Storage, Data Lake, and Analytics Services

**Last revised:** 2026-08-09

SAP-C02 storage questions usually test access pattern, protocol, durability, lifecycle, migration, governance, and analytics integration.

## Storage service selection

| Requirement | Service |
|---|---|
| Object storage, data lake, static assets | S3 |
| Shared Linux POSIX file system | EFS |
| Windows SMB file shares | FSx for Windows File Server |
| High-performance Lustre/HPC scratch | FSx for Lustre |
| NetApp ONTAP features | FSx for NetApp ONTAP |
| OpenZFS managed file system | FSx for OpenZFS |
| Hybrid file/iSCSI/tape interface to AWS storage | Storage Gateway |
| Repeated online data transfer | DataSync |
| Offline petabyte-scale transfer | Snow Family |
| SFTP/FTPS/FTP endpoint backed by S3/EFS | Transfer Family |
| Query S3 data with SQL | Athena |
| ETL and cataloging | Glue |
| Centralized data lake permissions | Lake Formation |
| Data warehouse | Redshift |

## S3

### Choose S3 when

- object storage is required
- data lake storage is needed
- static web assets are stored
- high durability and elastic scale matter
- event notifications/lifecycle/replication/object lock are useful

### Important S3 features

| Feature | Use |
|---|---|
| Versioning | Recover overwritten/deleted objects |
| Lifecycle policies | Move/delete objects by age |
| Intelligent-Tiering | Unknown/changing access patterns |
| Standard-IA / One Zone-IA | Infrequent access, lower cost |
| Glacier classes | Archive |
| Object Lock | WORM compliance |
| Replication | Cross-region/cross-account copy |
| Event notifications | Trigger Lambda/SQS/SNS/EventBridge |
| Access Points | Manage access at scale |
| Multi-Region Access Points | Global access routing to multi-region buckets |
| Inventory | Object-level reporting |
| Storage Lens | Storage analytics and cost visibility |

### S3 traps

- S3 is not a file system; no POSIX locking/semantics.
- S3 bucket names are global.
- Cross-Region Replication requires versioning.
- Replication is asynchronous.
- Lifecycle transitions optimize cost but can add retrieval cost/latency.
- Do not make S3 public unless deliberately serving public content; prefer CloudFront with private origin for web delivery.

### Object Lock: current creation and retention rules

S3 Object Lock can now be enabled on a **new or existing general-purpose
bucket**. Versioning must be enabled, and after Object Lock is enabled it cannot
be disabled or have Versioning suspended.

| Requirement | Rule |
|---|---|
| No user, including account root, may shorten retention or permanently delete a protected version | Compliance mode |
| Ordinary users must be blocked but specifically authorized administrators may bypass retention | Governance mode plus `s3:BypassGovernanceRetention` |
| Protect every new version for a fixed period such as five years | Configure bucket default retention |
| Protect versions that existed before default retention was configured | Apply retention to those versions explicitly or use an appropriate batch/copy process; the new bucket default is not retroactive |

Object Lock protects individual versions. A same-key `PUT` creates a new
version rather than destroying the retained version, and a simple delete can
create a delete marker without permanently deleting the protected version.

Current-question rule: for a new application with an already-created but empty
default bucket, enable Versioning and Object Lock on that bucket and use
five-year **compliance** retention. Older questions may still assume Object
Lock was creation-time-only and choose a replacement bucket; that assumption is
obsolete. If the stem clearly says objects already exist, make sure the answer
also protects or rewrites those existing versions.

### Intelligent-Tiering versus blanket lifecycle transition

| Access requirement | Decision |
|---|---|
| Unknown or mixed access during the early life of objects | Intelligent-Tiering; it moves each object according to observed access rather than aging every object into the same class |
| Archive after a known age regardless of individual access | S3 Lifecycle transition |
| Restore archived data within six hours | Archive Access / Glacier Flexible Retrieval; standard restoration is typically 3-5 hours |
| Lowest storage cost and restoration within about 12 hours is acceptable | Deep Archive Access / Glacier Deep Archive |

Intelligent-Tiering automatically uses Frequent, Infrequent, and Archive
Instant Access tiers. Optional Archive Access can start after at least 90 days
without access; optional Deep Archive Access can start after at least 180 days.
Therefore, mixed first-six-month access plus a six-hour restore ceiling points
to Intelligent-Tiering with **Archive Access** configured at 180 days—not Deep
Archive Access, and not a blanket lifecycle transition that misses early
per-object savings.

When objects are reproducible and the access pattern is known, availability
requirements can justify the cheaper storage sequence:

```text
daily access             -> S3 Standard
infrequent, recreatable  -> S3 One Zone-IA
rare, restore <= 5 hours -> S3 Glacier Flexible Retrieval
```

One Zone-IA is unsuitable for the only irreproducible copy, but recreation from
source changes that durability trade-off. Glacier Deep Archive loses when the
restore deadline is five hours because its standard retrieval is about twelve
hours.

For repetitive reads from compute in another Region, replicate a small,
infrequently changed dataset to a bucket in the reader Region and use a local
S3 gateway endpoint. CRR removes repeated cross-Region reads; a Lambda copier is
more operationally complex, and routing back to the source Region preserves the
transfer charge.

### Encrypt existing S3 objects at scale

Bucket default encryption governs new writes; it does not retroactively rewrite
existing object versions. For a large existing estate:

1. configure the required bucket default encryption for future writes;
2. use S3 Inventory or a generated manifest to identify existing objects; and
3. use S3 Batch Operations to copy/rewrite the objects in place with the
   required encryption, preserving required metadata and accounting for
   versioning.

Use Macie to discover and report sensitive data in S3 object contents. Amazon
Inspector is a workload/software vulnerability service; it is not the S3
sensitive-data classifier. Creating a replacement bucket and changing every
application is unnecessary when an in-place batch operation satisfies the
requirement.

Current-service nuance: S3 automatically applies SSE-S3 as a baseline to new
uploads. An exam stem that calls objects “unencrypted” may describe legacy
objects or may really require a specified KMS encryption posture. Follow the
stated compliance requirement, but retain the future-default versus
existing-object distinction.

## Additional S3 references

- S3 Object Lock: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html>
- Configure Object Lock on existing buckets: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-configure.html>
- Intelligent-Tiering access tiers: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/intelligent-tiering-overview.html>
- Archive retrieval times: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/restoring-objects-retrieval-options.html>

## Amazon EBS SSD decision boundary

| Signal | Preferred starting point |
|---|---|
| General transactional workload; independently provisioned IOPS/throughput within gp3 limits | `gp3` |
| Existing small `gp2` volume repeatedly exhausts burst credits, but required steady performance fits gp3 | Migrate to `gp3` and provision the required IOPS/throughput |
| Mission-critical database requires sustained IOPS/throughput beyond gp3, consistent sub-millisecond latency, or the higher durability tier | `io2` Block Express |

`gp2` couples baseline IOPS to volume size and smaller volumes use burst credits. `gp3` separates size, IOPS, and throughput and does not use the gp2 I/O-credit model. `io2` Block Express is the highest-performance EBS SSD tier for demanding I/O-intensive databases, but it is not automatically the most cost-effective repair for every depleted `gp2` burst bucket.

Before changing volume type, confirm the required IOPS, average I/O size, throughput, queue depth, EC2 instance EBS bandwidth, latency target, and durability requirement. Thirty-percent storage utilization is not a reason to increase capacity merely to obtain performance.

Trap: “burst credits exhausted” identifies a `gp2` performance-model problem; choose `gp3` when its steady limits satisfy the workload, and `io2` Block Express when the scenario explicitly requires the higher sustained performance, latency, or durability envelope.

## Data lake pattern

```text
Raw data in S3
  -> Glue crawler / schema registration
  -> Glue ETL or EMR/Spark processing
  -> Curated Parquet partitions in S3
  -> Glue Data Catalog
  -> Athena / Redshift Spectrum / QuickSight
  -> Lake Formation permissions
```

### Raw vs curated

| Zone | Meaning |
|---|---|
| Raw/landing | Original immutable ingestion, minimal transformation |
| Cleansed | Validated, normalized, deduplicated |
| Curated | Analytics-optimized model, often Parquet/partitioned |
| Consumption | BI/ML/application-ready outputs |

## Glue

### Choose Glue when

- serverless ETL is required
- crawlers/catalog are needed
- schema discovery is needed
- Spark-based transformation is suitable
- data lake metadata is required

### Glue traps

- Glue is not a low-latency event processor by default.
- Glue Data Catalog is metadata, not the data itself.
- Glue crawler discovers schema; it does not guarantee perfect data model design.
- For streaming analytics, Kinesis/Flink is more directly relevant.

## Athena

Choose Athena when:

- SQL query on S3 is required
- serverless interactive analytics is enough
- data is in columnar format such as Parquet/ORC
- partition pruning can reduce scan cost

Trap: Athena cost is driven by data scanned. Poor partitioning and CSV/JSON can be expensive.

## Redshift

Choose Redshift when:

- managed data warehouse is required
- high-concurrency BI/reporting is needed
- complex analytics over large structured datasets
- workload benefits from Redshift performance model

Redshift Spectrum queries S3 data without loading all data into Redshift, but careful partitioning and file format still matter.

## EFS

Choose EFS when:

- multiple Linux instances/containers need shared POSIX file storage
- NFS semantics matter
- shared home directories/content repositories are needed
- serverless/shared file access with Lambda/ECS/EKS is needed

Traps:

- A Regional EFS file system stores data redundantly across multiple AZs in one
  Region and is mounted through mount targets in the clients' AZs. It is the
  normal answer for a shared, mutable Linux file system used by instances in
  different AZs.
- EBS Multi-Attach is limited to supported `io1`/`io2` volumes and instances in
  the same AZ. The application/file system must also coordinate concurrent
  writers; Multi-Attach does not turn ordinary block storage into a Regional
  shared file service.
- DataSync performs transfer and synchronization jobs. It is not a continuously
  shared file system or an application-level locking mechanism.
- One EFS file system is not mounted across Regions. Cross-Region requirements
  need replication/backup and a Regional access design.
- Performance mode and throughput mode matter.
- Not for Windows SMB shares; use FSx for Windows.
- Not object storage; use S3 for data lake/object workloads.

## FSx

| Service | Use |
|---|---|
| FSx for Windows File Server | SMB, Active Directory integration, Windows workloads |
| FSx for Lustre | HPC, ML, high-throughput scratch, S3-linked workloads |
| FSx for NetApp ONTAP | ONTAP features, multiprotocol, snapshots, replication |
| FSx for OpenZFS | ZFS features, snapshots/clones, high performance |

## Storage Gateway

| Gateway | Use |
|---|---|
| File Gateway | On-prem NFS/SMB access to S3 |
| Volume Gateway | iSCSI block storage backed by AWS |
| Tape Gateway | Virtual tape library replacement |

Choose Storage Gateway for hybrid applications that need local protocol access while data is backed by AWS storage.

S3 File Gateway exposes an on-premises NFS or SMB share with a one-to-one
mapping between files and S3 objects. Existing bucket objects appear as files,
and new files written through the share become objects. During a staged
six-month migration, this lets legacy applications keep one familiar file
interface while migrated applications access the same durable objects directly
from S3; daily copy jobs would deliberately maintain two datasets.

### S3 access points for prefix-based populations

Use separate S3 access points when one bucket contains many prefixes but
different user populations need stable, independently governed views. Each
access-point resource policy can restrict its object ARN to one or more
prefixes, while the bucket policy delegates the permitted access-point path.

```text
marketing access point -> mk/*
sales access point     -> sa/*
director access point  -> mk/* and sa/*
finance bucket access  -> all approved prefixes
```

Access-point aliases minimize client changes and additional prefixes can gain
new access points without creating buckets or attaching policies to individual
objects. S3 “folders” are only key prefixes and cannot carry resource policies.

## DataSync

Choose DataSync when:

- moving large file/object datasets online
- repeated scheduled sync is needed
- transferring between on-prem storage and AWS
- moving between AWS storage services
- preserving metadata and verifying transfers matters

Trap: DataSync moves files/objects. DMS migrates databases.

## Snow Family

Choose Snow when:

- network transfer is too slow/costly/unavailable
- large offline migration is required
- edge compute/disconnected environments are relevant

Trap: Do not choose Snow when continuous online replication is needed and bandwidth is adequate; choose DataSync/Direct Connect/replication.

## Analytics pipeline decision

| Requirement | Path |
|---|---|
| Batch ETL from S3 | Glue |
| Query S3 with SQL | Athena |
| Data warehouse | Redshift |
| Stream ingestion | Kinesis Data Streams/Firehose |
| Stream analytics | Managed Service for Apache Flink |
| Hadoop/Spark ecosystem control | EMR |
| Govern data lake access | Lake Formation |

### Managed SFTP-to-ETL event chain

For uploaded files that should be transformed without polling servers:

```text
Transfer Family SFTP -> S3
  -> S3 event through EventBridge
  -> Glue ETL job/workflow
  -> Glue Job State Change event
  -> EventBridge -> SQS completion message
```

This replaces the SFTP host, transformation cron host and messaging server with
managed services. Prefer event-driven S3 processing over a fixed five-minute
schedule when each upload is the actual trigger. EMR Serverless is justified
by Spark/Hive-scale processing requirements, not merely by the word
“transformation.”

### Organization-wide and cross-Region backup

AWS Backup can protect supported S3, EBS, EFS, RDS and Storage Gateway Volume
Gateway resources through one backup plan and copy recovery points to a vault
in another Region. This is lower overhead than creating a separate replication
mechanism for each storage service.

Organizations backup policies centrally generate AWS Backup plans across
accounts. Attach a complete, validated daily policy at the organization root
when every account has the same EBS requirement. Member accounts can view the
inherited plan but cannot modify the policy-created plan. Ensure the target
vaults and IAM roles exist, and verify that the first backups and copies
actually succeed.

## Analytics and data-intake discriminators

| Scenario signal | First service to evaluate | Boundary |
|---|---|---|
| Serverless Spark ETL, crawlers and a shared metadata catalog | AWS Glue | Glue is the data-integration/catalog service; it is not the default for a continuously running custom cluster |
| Hadoop/Spark ecosystem, cluster-level framework and instance control, or broad big-data tooling | Amazon EMR | More control and operational surface than Glue; EMR Serverless is an option when the framework is needed without cluster management |
| Fine-grained governed access to cataloged data lake tables, columns or rows across accounts | Lake Formation | It complements IAM, S3 and KMS; it does not replace storage or encryption policies |
| Serverless SQL directly over data in S3 | Athena | Query service, not ETL or storage |
| Managed data warehouse and repeated BI workloads | Redshift | Warehouse semantics, not an operational database |
| Dashboards and managed business intelligence | QuickSight | Visualization/BI layer, not the data store or ETL engine |
| Transfer supported SaaS records such as Salesforce data to or from S3/AWS without bespoke connector code | Amazon AppFlow | SaaS application integration, not file-system migration like DataSync |
| Find, subscribe to, license, or share third-party datasets and data products | AWS Data Exchange | Entitlement and delivery of external data, not transformation |

Long-tail exam rule: choose these services only when the stem names their
distinct workload. “Move data” alone is insufficient—identify SaaS records,
files, database rows, streams, or third-party data entitlements first.

## Exam traps

| Trap | Correction |
|---|---|
| “S3 is shared file storage” | Use EFS/FSx for file systems. |
| “Athena stores data” | Athena queries data in S3. |
| “Glue crawler cleans data” | Crawlers infer schema; ETL jobs transform data. |
| “Lake Formation replaces S3 bucket policies everywhere” | It adds data lake governance; S3/IAM/KMS still matter. |
| “Replication is backup” | Replication can replicate deletes/corruption depending config; use backups/versioning/object lock as needed. |
| “One Zone-IA for critical only copy” | One AZ loss risk; use Standard-IA or replicated copies. |
| “Use DataSync for Salesforce records” | Use AppFlow for supported SaaS application data; DataSync moves file/object storage. |
| “Data Exchange performs ETL” | It provides third-party data products and entitlements; use Glue/EMR for transformation. |
| “Enabling bucket default encryption rewrites old objects” | It applies to new writes; use an inventory/manifest and S3 Batch Operations for existing objects. |
| “Inspector finds sensitive fields inside S3 objects” | Use Macie for sensitive-data discovery in S3. |

## Additional references

- AppFlow SaaS-to-S3 flow: <https://docs.aws.amazon.com/appflow/latest/userguide/flow-tutorial-salesforce-s3.html>
- AWS Data Exchange overview: <https://docs.aws.amazon.com/data-exchange/latest/userguide/what-is.html>
- Macie sensitive-data discovery: <https://docs.aws.amazon.com/macie/latest/user/data-classification.html>
- S3 Batch Operations: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops.html>
