# SAP-C02 Timed Mixed Diagnostic - 30 Questions - 2026-07-20

<!-- markdownlint-disable MD013 MD060 -->

## Purpose and Evidence Boundary

**Document role:** completed timed learner submission. Start at the
[Exam-Prep Revision Hub](README.md), but close all study and review material
before beginning this attempt.

This block approximates the current SAP-C02 domain weighting across 30 mixed,
unseen scenarios. It contains multiple-choice and multiple-response questions
but no answer key, explanations, domain labels, or scoring hints.

The official exam provides 180 minutes for 75 questions. The proportional time
limit for this diagnostic is **72 minutes**.

This is a timed diagnostic, not a full 75-question simulation. It does not by
itself satisfy the tracker booking criterion.

## Attempt Rules

1. Set one uninterrupted **72-minute** timer.
2. Do not open lessons, reviews, diagrams, documentation, search, AI assistance,
   notes, or an answer key during the attempt.
3. Choose exactly the number of responses requested. A multiple-response item
   receives credit only when the complete response set is correct.
4. Guess rather than leave a question blank; record uncertain questions only
   after freezing the answer set.
5. Stop when the timer expires, even if questions remain.
6. Submit the frozen answers, elapsed time, and any question numbers marked
   uncertain. Scoring begins only after explicit learner submission.

Submit answers in canonical form, for example:

```text
1AB 2C 3AE ... 30D
Elapsed: 68 minutes
Uncertain: 7, 14, 22
```

## Questions

### 1

A company uses AWS Organizations with separate production, development, and
security accounts. It must provide centrally managed workforce access without
creating IAM users in member accounts, and member-account administrators must
be prevented from leaving the organization.

Which TWO actions meet these requirements?

- A. Configure IAM Identity Center permission sets and account assignments.
- B. Attach an SCP that denies the Organizations action for leaving the
  organization to the relevant member-account boundary.
- C. Create identically named IAM users in every member account.
- D. Use an AWS Config aggregator to grant cross-account console access.
- E. Share an IAM role through AWS Resource Access Manager (AWS RAM).

### 2

A company runs an Amazon ECS service behind an Application Load Balancer. A new
application version must be introduced without interrupting current users. If
health checks or business metrics fail, traffic must automatically return to
the previous version.

Which deployment strategy best meets the requirements?

- A. Replace every task in place by using an all-at-once deployment.
- B. Use an AWS CodeDeploy blue/green deployment with test and production
  listeners plus automatic rollback alarms.
- C. Create a new ECS cluster manually and change DNS after deleting the old
  cluster.
- D. Use AWS Systems Manager Patch Manager to install the application version
  on the containers.

### 3

Security teams discover that developers repeatedly create security-group rules
that expose administrative ports to `0.0.0.0/0`. The company needs continuous
detection and automatic removal of noncompliant rules.

Which solution provides the most direct response?

- A. Evaluate the rules with an AWS Config managed or custom rule and associate
  automatic remediation that invokes an AWS Systems Manager Automation runbook.
- B. Enable VPC Flow Logs and wait for rejected connections.
- C. Store AWS CloudTrail logs in S3 Glacier Flexible Retrieval.
- D. Use AWS Trusted Advisor only during quarterly reviews.

### 4

A company must exit a data centre within four months. It has 400 supported
VMware virtual machines and wants to move them to AWS with minimal application
changes while continuously replicating server disks until cutover.

Which service is the best fit?

- A. AWS Database Migration Service (AWS DMS)
- B. AWS DataSync
- C. AWS Application Migration Service
- D. AWS Elastic Disaster Recovery used only after an outage

### 5

A multi-account company needs centralized API-activity logs that workload-
account administrators cannot delete. The security team must be able to verify
log integrity and retain the objects under a defined immutable-retention
period.

Which TWO actions best meet these requirements?

- A. Create an organization trail that delivers to a dedicated Log Archive
  account.
- B. Use an S3 bucket with versioning, appropriately governed Object Lock
  retention, and restricted deletion permissions in the Log Archive account.
- C. Deliver separate account trails to buckets owned by each workload team.
- D. Use an AWS Config aggregator instead of AWS CloudTrail.
- E. Store the only copy in each account's default CloudWatch log group.

### 6

A company maintains a complete application stack in a second AWS Region. The
stack runs continuously at reduced capacity and can process requests before it
is scaled up to handle the full production load.

Which disaster-recovery pattern is described?

- A. Backup and restore
- B. Pilot light
- C. Warm standby
- D. Multi-site active/active

### 7

Amazon ECS tasks connect to an Amazon RDS database. Database credentials are
currently stored in the container image, and the security team requires
automatic rotation without embedding long-lived credentials in deployments.

Which solution is most appropriate?

- A. Store the credentials as plaintext ECS environment variables.
- B. Store and rotate the credentials in AWS Secrets Manager, and grant the ECS
  task role permission to retrieve the secret.
- C. Put the credentials in an encrypted object in a public S3 bucket.
- D. Add the credentials to the EC2 user data of the container instances.

### 8

A company must migrate 2 PB from an on-premises NFS system. The network cannot
transfer the initial dataset inside the required 10-day window. After the bulk
copy, the company needs to transfer changed files before final cutover.

Which TWO services should the company use?

- A. AWS Snowball Edge for the initial offline transfer
- B. Amazon S3 Transfer Acceleration for an offline appliance workflow
- C. AWS DataSync for the incremental network transfer
- D. AWS Storage Gateway Volume Gateway as the migration tracker
- E. AWS Database Migration Service for NFS metadata

### 9

Applications in VPCs must resolve on-premises private DNS names, and
on-premises systems must resolve records in Route 53 private hosted zones. The
networks are already connected through AWS Direct Connect and a Transit
Gateway.

Which design meets the DNS requirement?

- A. Use an AWS Config aggregator and Transit Gateway route propagation.
- B. Associate the private hosted zones directly with the on-premises network.
- C. Add an Internet Gateway to each VPC and use public Route 53 records.
- D. Deploy Route 53 Resolver outbound and inbound endpoints with the required
  forwarding rules.

### 10

An ordering platform receives millions of events per hour. Events for the same
customer must remain ordered, consumers must replay seven days of history, and
multiple analytics applications must consume independently.

Which TWO design choices best meet the requirements?

- A. Use Amazon Kinesis Data Streams with the customer identifier as the
  partition key.
- B. Use one Amazon SQS FIFO queue and allow only one analytics consumer.
- C. Configure sufficient Kinesis retention and independent consumers or
  enhanced fan-out.
- D. Publish events only to an Amazon SNS topic without durable subscriptions.
- E. Store the current event only in AWS Systems Manager Parameter Store.

### 11

A fault-tolerant batch platform has a predictable compute baseline and large,
interruptible daily bursts. Analysis also shows that several instance types are
oversized.

Which strategy should the solutions architect recommend first?

- A. Purchase Reserved Instances for the current oversized fleet and keep all
  burst capacity On-Demand.
- B. Move all workloads to Dedicated Hosts.
- C. Rightsize from measured utilization, cover the stable baseline with an
  appropriate Savings Plan, and use Spot capacity for interruption-tolerant
  bursts.
- D. Disable Auto Scaling so the monthly bill is predictable.

### 12

A company is migrating an Oracle database to Amazon Aurora PostgreSQL. The
schema and database code require conversion, and ongoing changes must replicate
until a low-downtime cutover.

Which TWO services should be used?

- A. AWS Schema Conversion Tool (AWS SCT) for schema and code conversion
- B. AWS Database Migration Service with full load and change data capture
- C. AWS Application Migration Service for database schema conversion
- D. AWS DataSync for transactional log replication
- E. Amazon RDS Multi-AZ deployment for heterogeneous schema conversion

### 13

A global enterprise has hundreds of VPCs across multiple accounts. It needs
centralized, transitive routing, separate route domains for production and
non-production, and private connectivity to on-premises networks through
Direct Connect.

Which architecture best meets the requirements?

- A. Create a full mesh of VPC peering connections and one virtual private
  gateway per VPC.
- B. Publish every application through public Application Load Balancers.
- C. Use AWS PrivateLink as the transitive routing hub for all CIDR ranges.
- D. Use Transit Gateway route tables for segmentation and associate the
  Transit Gateway with a Direct Connect gateway.

### 14

A public web application is delivered through CloudFront. The company requires
centralized filtering of SQL injection and cross-site scripting requests, plus
enhanced managed protection and response support for large DDoS attacks.

Which TWO controls meet the requirements?

- A. Associate an AWS WAF web ACL with the CloudFront distribution.
- B. Use Amazon GuardDuty to block requests at the CloudFront edge.
- C. Enable AWS Shield Advanced protection for the relevant resources.
- D. Replace CloudFront with a network ACL that allows only port 443.
- E. Use Amazon Inspector to inspect every incoming HTTP request.

### 15

An Aurora database supports a read-heavy application. Monitoring shows high CPU
utilization on the writer while write throughput remains low. The application
can direct read-only queries to a separate endpoint.

Which change most directly improves database read scalability?

- A. Increase the backup-retention period.
- B. Add Aurora Replicas and direct read-only traffic to the reader endpoint.
- C. Convert the cluster to a single-instance deployment.
- D. Route read traffic through an S3 gateway endpoint.

### 16

A business has selected a SaaS product to replace a legacy commercial
application. The legacy application will be decommissioned after its data is
transferred to the SaaS product.

Which migration strategy is this?

- A. Rehost
- B. Refactor
- C. Repurchase
- D. Retain

### 17

A company needs to allocate multi-account AWS costs to business units even when
accounts contain several workloads. Finance also needs logical groupings that
span accounts and alerts for unusual spending patterns.

Which THREE actions best meet the requirements?

- A. Activate consistent user-defined cost allocation tags.
- B. Use only account names as the allocation model.
- C. Define AWS Cost Categories for the required business groupings.
- D. Use CloudWatch infrastructure alarms as the only cost alert.
- E. Configure AWS Cost Anomaly Detection with appropriate monitors and
  subscriptions.

### 18

A global game exposes a latency-sensitive API from two active AWS Regions. It
needs a static anycast entry point that sends users to healthy regional
endpoints, and its key-value data must support multi-Region active-active
access.

Which TWO services best meet these requirements?

- A. Amazon Route 53 simple routing and a single-Region Amazon RDS instance
- B. AWS Global Accelerator for the API endpoints
- C. AWS Storage Gateway for the key-value data
- D. DynamoDB global tables
- E. An S3 gateway endpoint shared across Regions

### 19

Workers consume messages from an Amazon SQS standard queue. Some jobs run
longer than the visibility timeout, causing duplicate deliveries and duplicate
charges to an external payment system.

Which improvement best addresses the problem?

- A. Shorten the visibility timeout and delete messages before processing.
- B. Replace the queue with an Amazon SNS topic without subscriptions.
- C. Make payment processing idempotent, extend or heartbeat the visibility
  timeout for active jobs, and route repeatedly failing messages to a DLQ.
- D. Increase message retention and assume each message will be delivered once.

### 20

A company needs to migrate 50 TB from an on-premises NFS server to Amazon EFS
over an existing 10-Gbps Direct Connect connection. The service must preserve
file metadata, perform incremental transfers, and validate copied data.

Which service is the best fit?

- A. AWS Application Migration Service
- B. AWS DataSync
- C. AWS Database Migration Service
- D. Amazon Kinesis Data Firehose

### 21

A consumer role in Account B already has `s3:GetObject` permission through the
bucket policy of Account A. The objects use an Account A customer-managed KMS
key, but reads fail with an authorization error during decryption.

Which TWO additional permissions are required?

- A. The KMS key policy in Account A must allow the Account B consumer role or
  delegate the required use to Account B.
- B. An SCP must explicitly grant `kms:Decrypt` to Account B.
- C. The S3 gateway endpoint policy alone must grant KMS administration.
- D. The consumer role's IAM policy in Account B must allow the required KMS
  decrypt action on the key.
- E. The object must be made public so KMS can decrypt it.

### 22

Private ECS tasks need same-Region access to Amazon S3 and DynamoDB at the
lowest additional endpoint cost. They also call a third-party public IPv4 API
that does not support PrivateLink.

Which architecture best meets the requirements?

- A. Send all traffic through one NAT instance with source/destination checking
  enabled.
- B. Create interface endpoints for S3 and DynamoDB and remove all egress for
  the third-party API.
- C. Use gateway endpoints for S3 and DynamoDB, and retain a reviewed NAT path
  for the unsupported public API.
- D. Use Route 53 Resolver inbound endpoints as the egress path.

### 23

Production CloudFormation stacks frequently drift because administrators make
manual changes. Deployments also proceed without showing reviewers the exact
resource changes.

Which TWO improvements best address these issues?

- A. Run CloudFormation drift detection and reconcile detected drift.
- B. Permit manual changes but rename resources after every deployment.
- C. Generate CloudFormation change sets in the deployment pipeline and require
  review before execution.
- D. Store templates only on an administrator's laptop.
- E. Disable stack rollback to preserve failed resources.

### 24

A company wants to modernize a synchronous monolith that experiences highly
variable request traffic. New background tasks can run asynchronously, must
buffer traffic spikes, and should minimize server-management overhead.

Which architecture is most appropriate?

- A. A single larger EC2 instance with a local queue file
- B. An Auto Scaling group that writes every task directly to one database
- C. A Direct Connect gateway connected to an on-premises batch server
- D. Amazon API Gateway and AWS Lambda for request handling, with Amazon SQS
  buffering asynchronous work for Lambda or managed workers

### 25

A ransomware-recovery design must protect backups from a compromised workload
account, retain recovery points against early deletion, and demonstrate that
the application can be restored.

Which THREE controls best meet the requirements?

- A. Copy backups to a separately administered backup account.
- B. Apply governed immutable-retention controls such as AWS Backup Vault Lock
  or an appropriate logically air-gapped vault design.
- C. Keep the only backup vault in the workload account with the same
  administrators.
- D. Treat successful backup jobs as proof that the application meets RTO.
- E. Schedule restore tests and validate the restored application and
  dependencies.

### 26

An event-driven application is expected to grow tenfold during a launch. Load
testing shows that compute scales correctly, but requests are throttled by a
regional service quota.

Which action should the solutions architect take?

- A. Add more Availability Zones and assume every quota increases
  automatically.
- B. Monitor quota utilization, request the required increase before launch,
  and include quota verification in deployment readiness checks.
- C. Disable throttling metrics so alarms do not fire.
- D. Move the application to one larger subnet.

### 27

A security team must detect unauthorized IAM policy changes across every
account, preserve the originating identity and API details, and invoke a
controlled remediation workflow.

Which design best meets the requirements?

- A. Use VPC Flow Logs as the source of IAM API identity data.
- B. Poll each IAM console weekly and send results by email.
- C. Use an organization CloudTrail trail, match relevant events with
  EventBridge, and invoke a controlled Lambda or Step Functions remediation
  workflow with audit output.
- D. Use S3 server access logs without CloudTrail management events.

### 28

On-premises applications require low-latency NFS access to frequently used
files. The authoritative objects should reside in Amazon S3, while the local
environment keeps a managed cache and continues using file protocols.

Which service best meets the requirement?

- A. Amazon S3 File Gateway
- B. AWS Snowball Edge used as the permanent file server
- C. Amazon S3 Transfer Acceleration
- D. AWS Database Migration Service

### 29

A central networking account owns VPCs and subnets. Application accounts in the
same AWS Organization must deploy resources into shared subnets without
creating duplicate VPCs or VPC peering connections.

Which TWO actions are required?

- A. Enable resource sharing with AWS Organizations and share the subnets by
  using AWS RAM.
- B. Share the VPC route table by using an S3 bucket policy.
- C. Create one Internet Gateway in every participant account for the shared
  VPC.
- D. Keep VPC ownership and network controls in the networking account while
  participant accounts create supported resources in the shared subnets.
- E. Use AWS PrivateLink to transfer ownership of the VPC CIDR to participant
  accounts.

### 30

A company serves static content and a dynamic TCP application from multiple
Regions. It wants edge caching for static objects and a fixed anycast entry
point that routes dynamic connections to healthy, low-latency regional
endpoints.

Which solution best meets the requirements?

- A. Use only Route 53 simple routing for both workloads.
- B. Use CloudFront for static content and AWS Global Accelerator for the
  dynamic TCP application.
- C. Use AWS WAF as the global TCP routing service and Amazon S3 for dynamic
  connections.
- D. Use one NAT Gateway as the global ingress endpoint.

## Answer Sheet

| Question | Choice(s) | Question | Choice(s) | Question | Choice(s) |
|---:|:---:|---:|:---:|---:|:---:|
| 1 | AB | 11 | C | 21 | AD |
| 2 | D | 12 | AB | 22 | C |
| 3 | A | 13 | D | 23 | AC |
| 4 | C | 14 | AC | 24 | D |
| 5 | AB | 15 | B | 25 | ABE |
| 6 | C | 16 | C | 26 | B |
| 7 | B | 17 | ACE | 27 | C |
| 8 | AC | 18 | BD | 28 | A |
| 9 | D | 19 | C | 29 | AD |
| 10 | AC | 20 | B | 30 | B |

## Attempt Status

- Status: explicitly submitted and assessed on 2026-07-21.
- Time limit: 72 minutes.
- Start: 00:00 Europe/London.
- End: 01:07 Europe/London.
- Elapsed: 67 minutes; five minutes remained.
- Submitted choices: `1AB 2D 3A 4C 5AB 6C 7B 8AC 9D 10AC 11C 12AB 13D 14AC 15B 16C 17ACE 18BD 19C 20B 21AD 22C 23AC 24D 25ABE 26B 27C 28A 29AD 30B`.
- Uncertain questions: not supplied.
- Score: **29/30 (96.7%)**.
- Single-choice: **17/18**.
- Multiple-response: **12/12**.
- Miss: Question 2, ECS blue/green deployment and rollback.
- Review: [answer-bearing assessment](sap-c02-mixed-diagnostic-30q-review-20260721.md).
- Tracker role: immediate timed mixed diagnostic; narrower than the two full
  75-question simulations required before the September 7 readiness review.
