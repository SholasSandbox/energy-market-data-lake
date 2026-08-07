# 06 - Storage, Data Lake, and Analytics Services

**Last revised:** 2026-07-28

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

- EFS is regional and mounted through mount targets in AZs.
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

## Exam traps

| Trap | Correction |
|---|---|
| “S3 is shared file storage” | Use EFS/FSx for file systems. |
| “Athena stores data” | Athena queries data in S3. |
| “Glue crawler cleans data” | Crawlers infer schema; ETL jobs transform data. |
| “Lake Formation replaces S3 bucket policies everywhere” | It adds data lake governance; S3/IAM/KMS still matter. |
| “Replication is backup” | Replication can replicate deletes/corruption depending config; use backups/versioning/object lock as needed. |
| “One Zone-IA for critical only copy” | One AZ loss risk; use Standard-IA or replicated copies. |
