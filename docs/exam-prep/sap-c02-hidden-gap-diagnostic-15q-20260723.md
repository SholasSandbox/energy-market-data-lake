# SAP-C02 Closed-Book Hidden-Gap Diagnostic - 15 Questions - 2026-07-23

<!-- markdownlint-disable MD013 MD060 -->

**Last revised:** 2026-07-25

## Purpose and Evidence Boundary

**Document role:** fresh question-only diagnostic. Close all lessons, reviews,
logs, diagrams, documentation, search, and AI assistance before beginning.

This diagnostic tests whether recent decision rules transfer to unfamiliar,
constraint-heavy scenarios. It contains no answer key, explanations, domain
labels, topic headings, or scoring hints. It does not replace a full-length
mock examination or satisfy a booking criterion by itself.

## Attempt Rules

1. Use one uninterrupted **40-minute** timer.
2. Do not use notes, lessons, reviews, documentation, search, or AI assistance.
3. Choose exactly the number of responses requested. Multiple-response items
   receive credit only when the complete response set is correct.
4. Guess rather than leave an item blank.
5. Record uncertain question numbers only after freezing the answer set.
6. Submit the frozen answers, start time, end time, elapsed time, and uncertain
   question numbers. Scoring begins only after explicit submission.

Submit each question number immediately followed by the selected letter or
letters, together with the timing and uncertainty fields below.

## Questions

### 1 - Choose TWO

A company has 120 member accounts in AWS Organizations. Workload teams may
deploy regional resources only in `eu-west-1` and `eu-west-2`. Central security
accounts must be able to operate supported security services in additional
Regions, and developers have administrator permissions inside their workload
accounts.

Which TWO actions meet the requirements with the least ongoing administration?

- A. Deploy AWS Config rules that delete every resource found outside the two
  approved Regions.
- B. Attach an IAM permissions boundary to every current and future developer
  role in every workload account.
- C. Attach a Region-restriction SCP to the workload OUs by using
  `aws:RequestedRegion` and carefully excluding required global services.
- D. Disable every unapproved Region separately in every account, including
  the Organizations management account.
- E. Place the central security accounts in a separate OU that does not inherit
  the workload Region-restriction SCP.

### 2 - Choose THREE

An enterprise has two separate control requirements. Member-account
administrators must not be able to stop or delete the organization CloudTrail
trail. Delegated application administrators may create IAM roles, but every
application role must remain within an approved permissions ceiling.

Which THREE controls directly implement these requirements?

- A. Attach an SCP to the relevant OUs that denies the protected CloudTrail
  actions.
- B. Attach the approved permissions boundary only to the delegated
  administrator and assume it automatically propagates to every role created.
- C. Use an AWS Config rule as the sole preventive control for CloudTrail
  deletion.
- D. Attach the approved permissions boundary to each application role.
- E. Remove the default `FullAWSAccess` SCP without replacing its allowed
  permissions.
- F. Restrict the delegated administrator's role-creation permissions with an
  `iam:PermissionsBoundary` condition that requires the approved boundary.

### 3 - Choose TWO

A role in a member account has an identity policy that allows Amazon EC2
actions in every Region. Its permissions boundary also allows those actions.
An SCP inherited from the parent OU explicitly denies regional service actions
outside `eu-west-2`. The role attempts to launch an instance in `us-east-1`.

Which TWO statements are correct?

- A. The request succeeds because both the identity policy and permissions
  boundary allow it.
- B. The request is denied because the SCP limits the maximum permissions
  available in the member account.
- C. The SCP grants the role permission to launch instances in `eu-west-2`
  even if no identity policy allows that action.
- D. Changing the permissions boundary on the role automatically changes the
  SCP inherited from the OU.
- E. An administrator policy in the member account cannot override the
  inherited explicit deny.

### 4 - Choose TWO

A DynamoDB table stores audit events with `tenantId` as the partition key and
event time as the sort key. A few tenants generate most writes and experience
throttling even though the table uses on-demand capacity. The application must
still retrieve all events for one tenant across a requested time range.

Which TWO changes most directly address the access-pattern problem?

- A. Add DynamoDB Accelerator without changing the key design.
- B. Add a bounded calculated shard suffix to the partition key for
  high-volume tenants.
- C. Change to provisioned capacity and retain the same partition key.
- D. Query the tenant's shard keys in parallel for the time range, then merge
  and order the results in the application.
- E. Enable DynamoDB Streams and assume the stream redistributes table writes.

### 5 - Single choice

A DynamoDB application write-shards invoices across 20 partition-key suffixes.
Most reads retrieve one invoice by `tenantId` and `invoiceId`; scanning all 20
shards for every point read would be unnecessarily expensive.

Which sharding strategy best supports efficient point reads while retaining
write distribution?

- A. Select a new random suffix on every read.
- B. Use invoice status alone as the shard suffix.
- C. Derive a stable calculated suffix from `invoiceId` so the application can
  determine the target shard.
- D. Replace on-demand mode with provisioned capacity and remove the suffix.

### 6 - Choose TWO

A DynamoDB global secondary index uses the low-cardinality value `OPEN` as its
partition key. Write traffic is well distributed in the base table, but the
index receives concentrated writes and queries for open work items are
throttled.

Which TWO changes address the index access pattern?

- A. Write index partition-key values such as `OPEN#0` through `OPEN#N` by
  using a controlled sharding function.
- B. Enable DynamoDB Streams on the base table and leave the index key
  unchanged.
- C. Add DynamoDB Accelerator to redistribute writes across index partitions.
- D. Query each active `OPEN` shard and merge the returned work items in the
  application.
- E. Increase only the table's maximum read capacity without changing the
  index key.

### 7 - Choose THREE

A customer-facing application uses Aurora PostgreSQL, Amazon S3, and stateless
containers. The business approves a recovery point objective of 15 minutes and
a recovery time objective of 20 minutes for a regional outage. Reduced compute
capacity may run continuously in the recovery Region.

Which THREE design elements best support the stated objectives?

- A. Restore Aurora from the previous night's snapshot after a failure.
- B. Use Aurora Global Database with a secondary cluster in the recovery
  Region and a tested promotion procedure.
- C. Keep only infrastructure templates in the recovery Region and no running
  database or compute capacity.
- D. Configure S3 cross-Region replication with appropriate replication
  monitoring or Replication Time Control for the protected objects.
- E. Recreate secrets, certificates, and network dependencies manually after
  declaring a disaster.
- F. Run a scaled warm-standby application stack with replicated dependencies
  and health-based traffic failover.

### 8 - Choose TWO

A company routes production and development VPC traffic through a centralized
stateful inspection VPC attached to an AWS Transit Gateway. Production and
development must remain isolated, and return traffic must traverse the same
inspection appliances as the forward path.

Which TWO design choices are most important?

- A. Associate every attachment with one transit gateway route table and
  propagate every route automatically.
- B. Attach an internet gateway directly to the transit gateway.
- C. Use separate transit gateway route tables with deliberate attachment
  associations, propagation, and routes through the inspection attachment.
- D. Add direct VPC peering between production and development as a fallback
  path.
- E. Enable transit gateway appliance mode on the inspection VPC attachment
  and design symmetric forward and return paths.
- F. Advertise a direct default route from each workload VPC to the internet
  to bypass inspection during appliance maintenance.

### 9 - Single choice

An application role in a workload account must use a customer managed KMS key
owned by a central security account. The role's identity policy already allows
the required KMS API operations, but requests to the key are denied.

Which change is required?

- A. Add an SCP that grants the KMS operations to the workload account.
- B. Update the KMS key policy to allow the workload role or allow the workload
  account to delegate access, while retaining the required IAM permission or
  grant.
- C. Make the workload S3 bucket publicly writable.
- D. Replace the customer managed key with an S3 gateway endpoint.

### 10 - Choose TWO

A company is migrating an Oracle database to Aurora PostgreSQL. Stored
procedures and proprietary data types require conversion. The target must
remain synchronized with ongoing source changes until a short cutover window.

Which TWO services should form the core migration path?

- A. AWS Application Migration Service for schema conversion.
- B. AWS Schema Conversion Tool for assessment and conversion of supported
  schema and database code.
- C. AWS DataSync for transactional change capture.
- D. An encrypted Oracle snapshot restored directly as an Aurora PostgreSQL
  cluster.
- E. AWS Database Migration Service with full load and change data capture.

### 11 - Choose TWO

A market-data platform processes records for thousands of instruments. Records
for the same instrument must remain ordered, records must be replayable for
seven days, and three analytics applications must independently consume every
record without competing with one another.

Which TWO design choices best meet the requirements?

- A. Use Kinesis Data Streams with the instrument identifier as the partition
  key.
- B. Use one SQS FIFO queue shared by all three analytics applications.
- C. Publish only to an SNS standard topic with no durable subscriptions.
- D. Use one SQS standard queue and rely on message attributes for ordering.
- E. Configure sufficient Kinesis retention and independent consumers, using
  enhanced fan-out where dedicated per-consumer throughput is required.

### 12 - Single choice

A global application accepts long-lived TCP connections from fixed client
software. It runs behind Network Load Balancers in two Regions. Clients require
static anycast IP addresses and rapid routing to the nearest healthy regional
endpoint without relying on DNS cache expiry.

Which service is the best global entry point?

- A. Amazon CloudFront
- B. Route 53 geolocation routing with long TTL values
- C. An Application Load Balancer in one Region
- D. AWS Global Accelerator

### 13 - Choose TWO

A design document claims that ordinary asynchronous S3 cross-Region
replication guarantees an application recovery point objective of less than
one minute for every new object. No measurement or contractual service level
for that threshold is recorded.

Which TWO conclusions should a solutions architect record?

- A. Cross-Region replication synchronously acknowledges the original
  `PutObject` only after the replica is durable in the destination Region.
- B. S3 Replication Time Control provides a 15-minute replication commitment,
  not a guarantee that every object meets a sub-minute recovery point.
- C. Enabling S3 Versioning in the source Region alone proves regional
  recovery within one minute.
- D. Aurora Global Database replication automatically includes objects in S3
  buckets used by the application.
- E. The sub-minute claim remains unproven and must be changed, measured, or
  supported by a different approved design before it is treated as achieved.

### 14 - Choose TWO

A company has Direct Connect connections in two locations and VPCs attached to
transit gateways in two AWS Regions. The company wants the on-premises network
to reach the VPCs through a Direct Connect gateway without creating a private
virtual interface for every VPC.

Which TWO components are required for the intended transit-gateway path?

- A. A transit virtual interface from the Direct Connect connection to the
  Direct Connect gateway.
- B. A private virtual interface attached directly to every transit gateway.
- C. A public virtual interface that advertises the private VPC CIDR ranges.
- D. Associations between the Direct Connect gateway and the transit gateways,
  with appropriate allowed prefixes and route configuration.
- E. Full-mesh VPC peering between every attached VPC.

### 15 - Choose THREE

An enterprise has three independent transfer requirements:

1. External partners must upload files over managed SFTP directly into S3.
2. An on-premises NFS share must be copied repeatedly to Amazon EFS, including
   incremental synchronizations and integrity verification.
3. A Windows application requires managed SMB storage, Microsoft Active
   Directory integration, Windows ACLs, and Distributed File System support.

Which THREE service mappings meet these requirements with the least custom
infrastructure?

- A. Use AWS Storage Gateway Tape Gateway for partner SFTP uploads.
- B. Use AWS Transfer Family with an S3 backend for the partner endpoint.
- C. Use AWS DataSync for the repeated NFS-to-EFS transfers.
- D. Use AWS Migration Hub to copy the NFS file contents.
- E. Use Amazon EFS as a native SMB and Distributed File System target.
- F. Use Amazon FSx for Windows File Server for the Windows application.

## Frozen Submission Block

Do not complete this block until the answer set is final.

```text
Start: 11:05
End: 11:37
Elapsed:
Uncertain:1,5

1:BE
2:ADF
3:BE
4:BD
5:C
6:AC
7:BDF
8:CE
9:B
10:BE
11:AE
12:D
13:BE
14:AD
15:BCF
```

## Post-Submission Boundary

After submission, keep this question document unchanged. Record scoring,
reasoning, genuine misses, and any question-quality caveat in a separate
answer-bearing review artifact.
