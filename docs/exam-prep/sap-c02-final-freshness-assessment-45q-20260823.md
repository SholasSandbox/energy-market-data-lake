# SAP-C02 Final Freshness Assessment - 45 Questions - 2026-08-23

<!-- markdownlint-disable MD013 MD060 -->

## Purpose and Evidence Boundary

**Document role:** answer-free, closed-book final freshness assessment.

This assessment provides one substantial confidence check before the final
exam-week taper. It contains 45 mixed SAP-C02 scenarios with deliberate extra
coverage of the two narrow Full Mock 009 boundaries: conventional PrivateLink
endpoint-service frontends versus Gateway Load Balancer endpoints, and Amazon
S3 Inventory/Batch Operations/S3 Select roles in large-scale re-encryption.

This is not a tenth full mock, a new booking gate, or authority to reopen the
GO decision because of one isolated result. It contains no answer key,
explanations, domain labels, or scoring hints. Create the answer-bearing review
only after the learner explicitly freezes and submits the response set.

## Attempt Rules

1. Start on Sunday, 2026-08-23 and set one uninterrupted **90-minute** timer.
2. Close all lessons, reviews, diagrams, documentation, search, AI assistance,
   notes, and answer-bearing artifacts before starting.
3. Choose exactly the number of responses requested. A multiple-response item
   receives credit only when the complete response set is correct.
4. Guess rather than leave a question blank.
5. Stop when the timer expires, even if questions remain.
6. After stopping, freeze the answer set before recording uncertain question
   numbers or opening any review material.
7. Submit the frozen answers, elapsed time, and uncertainty list. Scoring begins
   only after explicit learner submission.

Submit answers in canonical form, for example:

```text
1B 2AD 3C ... 45A
Elapsed: 84 minutes
Uncertain: 6, 18, 31
```

## Questions

### 1

A company uses AWS Organizations with separate production, development, and
security accounts. It must give employees centrally managed access to multiple
AWS accounts without creating IAM users in those accounts.

Which service BEST meets the requirement?

A. Amazon Cognito user pools.

B. AWS IAM Identity Center.

C. AWS Directory Service Simple AD deployed separately in every account.

D. AWS Resource Access Manager.

### 2 - Choose TWO

A company must centrally record management activity from every account in its
AWS organization. Workload-account administrators must not be able to delete
the retained evidence.

Which TWO actions best meet the requirements?

A. Create a multi-Region organization trail from an authorized account.

B. Create independent trails that deliver only to buckets owned by each
workload account.

C. Replace CloudTrail with an AWS Config aggregator.

D. Deliver the organization trail to a tightly controlled bucket in a
dedicated log-archive account.

E. Store the only copy in the default CloudWatch Logs group of each workload
account.

### 3

An administrator attaches an SCP that allows Amazon S3 read actions to an OU.
No IAM policy or resource policy grants those actions to a role in a member
account.

What is the result?

A. The role receives S3 read access because the SCP grants it.

B. The role does not receive access because SCPs limit permissions but do not
grant them.

C. The role receives access only to buckets in the management account.

D. The SCP is converted automatically into an IAM permissions boundary.

### 4

A provider must expose a private TCP application to hundreds of consumer VPCs
that have overlapping CIDR ranges. Consumers need private application access
without general routing into the provider VPC.

Which component should front the conventional AWS PrivateLink endpoint
service?

A. Application Load Balancer.

B. Network Load Balancer.

C. Gateway Load Balancer endpoint.

D. Transit Gateway peering attachment.

### 5

Applications in AWS must resolve private on-premises DNS names, and on-premises
systems must resolve records in Route 53 private hosted zones. Private network
connectivity already exists.

Which solution meets the DNS requirements?

A. An AWS Config aggregator and VPC Flow Logs.

B. Public hosted zones and an Internet Gateway.

C. Route 53 health checks associated with every private record.

D. Route 53 Resolver outbound and inbound endpoints with forwarding rules.

### 6 - Choose TWO

A company wants to steer traffic from many VPCs through a scalable fleet of
third-party firewalls in a centralized inspection VPC. The appliances must be
inserted transparently, and routing domains must prevent traffic from bypassing
inspection.

Which TWO components best meet the requirements?

A. A Gateway Load Balancer for the virtual-appliance fleet.

B. An Application Load Balancer in every application VPC.

C. Transit Gateway route tables that direct the required flows through the
inspection path.

D. VPC peering between every pair of application VPCs.

E. An S3 gateway endpoint in the inspection VPC.

### 7

Developers repeatedly create security-group rules that expose administrative
ports to `0.0.0.0/0`. The company needs continuous detection and controlled
automatic remediation.

Which solution is MOST appropriate?

A. Use VPC Flow Logs and manually inspect them each quarter.

B. Use an AWS Config rule with remediation through an AWS Systems Manager
Automation runbook.

C. Store CloudTrail logs in S3 Glacier Deep Archive without evaluation.

D. Use an AWS Cost and Usage Report to identify the rules.

### 8

A company must leave a data centre rapidly. It has hundreds of supported
physical and virtual servers and wants continuous block-level replication to
AWS followed by test launches and a controlled cutover with minimal application
changes.

Which service BEST fits?

A. AWS Database Migration Service.

B. AWS Application Migration Service.

C. AWS DataSync.

D. AWS Transfer Family.

### 9 - Choose TWO

A company runs a warm-standby application in a second Region. Data replication
is already configured. During a declared regional outage, the company must
increase the standby application's capacity and direct users to it.

Which TWO actions complete the response?

A. Scale the standby compute and application tier to production capacity.

B. Restore the only copy of all data from monthly offline tapes.

C. Delete the failed Region before allowing client traffic.

D. Use the configured DNS or global traffic-routing control to shift traffic
to the healthy Region.

E. Convert the design to backup and restore during the incident.

### 10

AWS Compute Optimizer lacks memory-utilization evidence for a fleet of EC2
instances. The company wants better rightsizing recommendations for memory-bound
workloads.

What should the company do?

A. Enable S3 server access logging.

B. Publish supported memory metrics through the CloudWatch agent for Compute
Optimizer to use.

C. Replace the instances with Spot Instances before collecting metrics.

D. Enable Route 53 query logging.

### 11 - Choose THREE

A role in Account A must read objects from an S3 bucket in Account B. The
objects are encrypted with a customer-managed KMS key in Account B. No SCP or
permissions boundary blocks the request.

Which THREE permission layers are required?

A. The role's identity permissions must allow the required S3 and KMS actions.

B. The management account must attach `AdministratorAccess` to the role.

C. The bucket policy must allow the cross-account S3 access.

D. An AWS Config aggregator must authorize the role session.

E. The KMS key policy must permit the cross-account use of the key.

F. A network ACL must name the role ARN.

### 12

Hundreds of services across multiple accounts need application-layer
connectivity, service discovery, authentication policies, and support for
overlapping VPC CIDR ranges. The company does not want to manage a separate
endpoint service for every service.

Which service BEST fits?

A. VPC peering.

B. Amazon VPC Lattice.

C. Internet Gateway.

D. AWS Direct Connect gateway.

### 13 - Choose TWO

A company must migrate 3 PB from an on-premises NFS system. Its network cannot
transfer the initial dataset inside the migration window. After the bulk move,
the company must copy changed files before cutover.

Which TWO services should it use?

A. AWS Snowball Edge for the initial offline transfer.

B. AWS Database Migration Service for the NFS filesystem.

C. Amazon S3 Transfer Acceleration as an offline appliance.

D. AWS DataSync for the subsequent incremental transfer.

E. AWS Migration Hub as the file-copy engine.

### 14

A company changes the default encryption configuration on an S3 bucket. The
bucket already contains several billion objects encrypted with an older key.

What happens to the existing objects?

A. Amazon S3 automatically rewrites every existing object immediately.

B. Existing objects retain their current encryption until an operation rewrites
them.

C. Existing objects become unreadable until S3 Select updates them.

D. Existing objects are moved automatically to S3 Glacier Flexible Retrieval.

### 15

An event platform needs seven-day replay, several independent consumers, and
ordering for events that share the same customer identifier.

Which service is the BEST fit?

A. Amazon SQS standard queue with one consumer.

B. Amazon Kinesis Data Streams using the customer identifier as the partition
key.

C. Amazon SNS without durable subscriptions.

D. Amazon MQ used only as a scheduled batch store.

### 16 - Choose TWO

A central networking account owns VPCs and subnets. Application accounts in
the same organization must deploy supported resources into those subnets while
network ownership and controls remain centralized.

Which TWO actions are required?

A. Enable AWS RAM sharing with AWS Organizations and share the required
subnets.

B. Create duplicate VPCs in every application account.

C. Transfer ownership of each route table to an application account.

D. Keep the VPC resources under the networking account while participant
accounts create supported resources in the shared subnets.

E. Connect every participant to the networking account by public Internet
Gateway.

### 17

A security vendor exposes a scalable fleet of virtual firewalls through an
AWS PrivateLink-powered service. Consumer VPC route tables must steer packets
through the appliances transparently.

Which consumer-side component is required?

A. Interface VPC endpoint connected to an NLB application service.

B. S3 gateway endpoint.

C. Gateway Load Balancer endpoint.

D. Transit Gateway peering attachment.

### 18

A company is migrating an Oracle database to Amazon Aurora PostgreSQL. After
schema conversion, it needs full-load migration followed by ongoing replication
until cutover.

Which service performs the data movement and continuous change replication?

A. AWS Application Migration Service.

B. AWS Database Migration Service.

C. AWS DataSync.

D. AWS Transfer Family.

### 19 - Choose THREE

A CloudFront distribution uses a private S3 origin. It must prevent direct
public bucket access, select between two origins using request attributes, and
cache responses at edge locations.

Which THREE components meet the requirements?

A. Origin access control for the S3 origin.

B. A public-read bucket ACL.

C. A CloudFront distribution and cache behaviour.

D. An Internet Gateway attached to the S3 bucket.

E. A Lambda@Edge origin-request function for dynamic origin selection.

F. An S3 gateway endpoint as the public edge cache.

### 20 - Choose TWO

On-premises servers must access Amazon S3 privately across Direct Connect.
The company wants the servers to use private IP connectivity and familiar S3
DNS names without sending the traffic through the public internet.

Which TWO components are appropriate?

A. An S3 gateway endpoint advertised through Direct Connect.

B. An S3 interface VPC endpoint.

C. An Internet Gateway with a default route from on premises.

D. Route 53 Resolver inbound endpoint support for the required private DNS
resolution path.

E. VPC peering between the on-premises network and S3.

### 21

A multi-account company wants one member account to manage GuardDuty findings
and organization-wide detector configuration. It wants to keep the management
account out of routine security operations.

What should it configure?

A. A GuardDuty delegated administrator in the security-tooling account.

B. A GuardDuty detector independently managed by every developer.

C. An S3 bucket policy that grants GuardDuty administrative actions.

D. A Route 53 Resolver rule shared from the management account.

### 22

EC2 instances in three Availability Zones need concurrent access to one
managed, elastic, POSIX-compatible NFS filesystem.

Which storage service BEST meets the requirement?

A. One EBS volume attached to all instances across the Availability Zones.

B. Amazon EFS with mount targets in the required Availability Zones.

C. EC2 instance store replicated by startup scripts.

D. Amazon S3 mounted as a block device.

### 23 - Choose TWO

A company must discover process-level and TCP-connection dependencies on
physical servers and then track the overall migration portfolio from one
AWS location.

Which TWO services or features should it use?

A. AWS Application Discovery Agent on the physical servers.

B. AWS Database Migration Service schema conversion only.

C. AWS Migration Hub for centralized discovery and migration tracking.

D. Amazon Inspector network reachability rules as the migration tracker.

E. AWS Snowcone as the process-dependency collector.

### 24

An Amazon RDS for PostgreSQL database is CPU constrained by reporting queries.
The transactional workload must remain on the writer, and the application can
tolerate slightly stale report data.

Which change is MOST appropriate?

A. Add a read replica and direct reporting queries to it.

B. Enable Multi-AZ and send read traffic to the standby instance in every
deployment mode.

C. Replace the database with an SQS queue.

D. Store database credentials in EC2 user data.

### 25 - Choose TWO

A read-heavy DynamoDB application needs microsecond response times for
eventually consistent reads. The application currently calls DynamoDB directly.

Which TWO changes are required to use DynamoDB Accelerator effectively?

A. Create a highly available DAX cluster.

B. Modify the application to use the DAX client and cluster endpoint.

C. Add an S3 gateway endpoint to every DAX node.

D. Request strongly consistent DAX reads for every operation.

E. Replace the DynamoDB table with an EBS volume.

### 26

An Organizations tag policy defines the allowed spelling and values for a
`CostCenter` tag. An engineer assumes this policy will deny every untagged
resource-creation request.

Which statement is correct?

A. Tag policies standardize and report tag compliance but do not universally
act as an API-deny control for all untagged creation requests.

B. Tag policies grant IAM permissions for tagged resources.

C. Tag policies encrypt the tag values with KMS.

D. Tag policies replace all SCPs attached to the organization.

### 27 - Choose THREE

A company declares a regional disaster for a warm-standby application. The
secondary Region already receives replicated data and runs a reduced-capacity
application stack.

Which THREE actions form a complete high-level recovery chain?

A. Confirm or promote the secondary data tier as required by the database
design.

B. Delete all resources in the failed Region before validating recovery.

C. Scale the secondary application and compute tiers to production capacity.

D. Disable health checks so traffic remains pinned to the failed Region.

E. Shift traffic through the configured DNS or global-routing control.

F. Replace the standby architecture with tape restore during the incident.

### 28 - Choose TWO

A company has billions of existing S3 objects encrypted with an old KMS key.
It needs an authoritative managed object list and a fleet-scale operation that
rewrites the objects with a new encryption configuration.

Which TWO services or features should it use?

A. Amazon S3 Inventory to generate the object manifest.

B. CloudFront invalidations to modify object encryption.

C. S3 Select to update encryption metadata in place.

D. S3 Batch Operations Copy with the required encryption settings.

E. EBS snapshots as the S3 object manifest.

### 29

A company is modernizing a large mainframe estate and wants an AWS service that
uses automated analysis and transformation capabilities for mainframe
modernization.

Which service should it evaluate?

A. AWS Storage Gateway.

B. AWS Transform.

C. AWS Transfer Family.

D. AWS Elastic Disaster Recovery.

### 30

A central observability account must view metrics, logs, and traces shared by
many source accounts without moving all workloads into the monitoring account.

Which service provides the cross-account observability links and sink model?

A. Amazon CloudWatch Observability Access Manager.

B. AWS CloudTrail Lake only.

C. AWS Migration Hub.

D. AWS Resource Explorer used as a metrics database.

### 31 - Choose TWO

A stateless API runs actively in two Regions. The data tier must support
multi-Region writes, and clients must be routed to healthy regional endpoints.

Which TWO components best meet the requirements?

A. DynamoDB global tables for the multi-Region data tier.

B. One single-AZ RDS instance with no replica.

C. Route 53 health-based routing or AWS Global Accelerator for regional traffic
selection.

D. A NAT Gateway as the global client entry point.

E. An EBS snapshot copied manually after each outage.

### 32

A company needs a break-glass administrative path for its management account.
The path must be separate from routine administrator group membership and used
only during emergencies.

Which design is MOST appropriate?

A. Give every developer permanent management-account administrator access.

B. Create a dedicated emergency principal with strong MFA and a narrowly
assigned emergency permission set, and monitor and reduce access after use.

C. Share the root password through a team chat channel.

D. Disable logging for emergency sessions.

### 33

A company moves an existing VMware-based workload to VMware Cloud on AWS while
retaining the virtualization platform and making minimal changes to the virtual
machines.

Which migration strategy is this?

A. Retire.

B. Repurchase.

C. Relocate.

D. Refactor.

### 34

An engineer proposes using S3 Select to rotate the KMS key on an existing S3
object.

Why is the proposal incorrect?

A. S3 Select retrieves selected content from a supported object; it does not
rewrite the object or change its encryption configuration.

B. S3 Select can change encryption only for EBS snapshots.

C. S3 Select is an IAM identity service.

D. S3 Select changes bucket policies but not objects.

### 35

A company wants AWS Config to evaluate one managed rule consistently across
member accounts while routine configuration remains outside the Organizations
management account.

Which design BEST supports this model?

A. Register an appropriate member account as the AWS Config delegated
administrator and create the organization rule from an authorized account.

B. Use a CloudWatch dashboard as the compliance evaluator.

C. Create unrelated account-local rules and prevent central visibility.

D. Replace configuration recorders with VPC Flow Logs.

### 36

An API Gateway REST API must be callable only from approved VPCs over private
connectivity. It must not expose a publicly reachable endpoint.

Which design BEST meets the requirement?

A. Use a private API and an execute-api interface VPC endpoint with an
appropriate resource policy.

B. Use an edge-optimized public API without authorization.

C. Put the API behind a NAT Gateway and allow every source.

D. Use an S3 gateway endpoint to invoke API Gateway.

### 37

An AWS Batch workload needs a preinstalled proprietary library and should use
discounted spare EC2 capacity for interruptible jobs.

Which configuration BEST meets the requirements?

A. A managed EC2 compute environment using an approved custom AMI and Spot
capacity.

B. A Fargate-only environment that mounts an EC2 AMI.

C. An unmanaged Lambda environment with Spot pricing.

D. An S3 Lifecycle rule that launches compute instances.

### 38 - Choose TWO

A trading partner requires managed Applicability Statement 2 (AS2) message
exchange. Payloads must be stored durably in Amazon S3.

Which TWO components meet the requirements?

A. AWS Transfer Family AS2 resources and workflows.

B. Amazon MQ used as the AS2 protocol endpoint.

C. AWS DataSync used as the AS2 identity provider.

D. AWS DMS used to generate AS2 message dispositions.

E. An Amazon S3 bucket for the exchanged payloads.

### 39

The security team must prevent member accounts in a workload OU from using the
root user for routine API actions. The control must apply even if an IAM policy
otherwise allows an action.

Which control is MOST appropriate?

A. An OU-targeted SCP with the required explicit denies and tested exception
boundary.

B. An IAM permissions boundary attached only to one developer role.

C. A cost-allocation tag.

D. An AWS Config aggregator with no rules.

### 40 - Choose THREE

A company needs ransomware-resilient backups with stronger administrative
isolation, enforced retention, and evidence that recovery procedures work.

Which THREE measures best address these requirements?

A. Use an appropriately isolated or logically air-gapped backup vault.

B. Keep the only backup in the same workload account with unrestricted delete
permissions.

C. Apply the required vault retention controls, such as AWS Backup Vault Lock
where appropriate.

D. Disable backup monitoring to reduce operational overhead.

E. Treat successful backup-job status as proof that every restore will work.

F. Schedule and review restore testing.

### 41

Several independent applications must consume and replay an ordered event
history. Each event carries a customer ID, and order is required only within a
customer's events.

Which design BEST fits?

A. Kinesis Data Streams with the customer ID as the partition key and separate
consumer applications.

B. One SQS standard queue with destructive reads and no retention.

C. EventBridge Scheduler with one schedule per event.

D. Amazon SES distribution lists.

### 42 - Choose TWO

A compliance programme must re-encrypt a very large existing S3 population.
The team wants a managed report of the objects and a managed bulk operation
that performs same-bucket copies using the new encryption settings.

Which TWO choices form the required workflow?

A. Generate an S3 Inventory report suitable for the job manifest.

B. Change bucket default encryption and assume every old object is rewritten.

C. Run an S3 Batch Operations Copy job with the required encryption and object
property settings.

D. Use S3 Select as the operation that changes encryption metadata.

E. Run CloudFront invalidations for every object key.

### 43

A database migration has completed its initial load. The source must remain
online while inserts and updates are replicated to the target until a short
final cutover window.

Which capability should the migration team use?

A. AWS DMS ongoing replication using change data capture.

B. AWS Snowball Edge import with no subsequent synchronization.

C. Amazon EBS Fast Snapshot Restore.

D. AWS Backup cold storage transition.

### 44

A company has tagged resources with `Project`, `Environment`, and `CostCenter`.
It wants these user-defined tags to appear in cost-allocation reports.

What must an authorized billing administrator do?

A. Activate the selected user-defined cost-allocation tags in the payer or
management-account billing settings.

B. Attach the tags as IAM policies.

C. Create Route 53 records named after each tag.

D. Add the tags only to CloudTrail event selectors.

### 45

An architect proposes using an S3 gateway endpoint so on-premises clients can
reach S3 privately through a Direct Connect connection and a Transit Gateway.

Which statement is correct?

A. Gateway endpoint connectivity does not extend through Direct Connect,
VPN, Transit Gateway, or VPC peering; use an appropriate S3 interface-endpoint
pattern for on-premises private access.

B. Gateway endpoints are globally advertised through BGP by default.

C. Gateway endpoints create public IP addresses in the on-premises network.

D. Gateway endpoints require a Gateway Load Balancer appliance.

## Answer Sheet

| Question | Choice(s) | Question | Choice(s) | Question | Choice(s) |
|---:|:---:|---:|:---:|---:|:---:|
| 1 | B | 16 | AD | 31 | AC |
| 2 | AD | 17 | C | 32 | B |
| 3 | B | 18 | B | 33 | C |
| 4 | D | 19 | ACE | 34 | A |
| 5 | D | 20 | BD | 35 | A |
| 6 | AC | 21 | A | 36 | A |
| 7 | B | 22 | B | 37 | A |
| 8 | B | 23 | AC | 38 | AE |
| 9 | AD | 24 | A | 39 | A |
| 10 | B | 25 | AB | 40 | ACF |
| 11 | ACE | 26 | A | 41 | A |
| 12 | B | 27 | ACE | 42 | AC |
| 13 | AD | 28 | AD | 43 | A |
| 14 | B | 29 | B | 44 | A |
| 15 | B | 30 | A | 45 | A |

## Attempt Status

- Status: **FROZEN learner submission; independently assessed**.
- Scheduled date: 2026-08-23.
- Time limit: 90 minutes.
- Start: 22:15 Europe/London.
- End: 23:10 Europe/London.
- Elapsed: 55 minutes; 35 minutes remained.
- Submitted choices: `1B 2AD 3B 4D 5D 6AC 7B 8B 9AD 10B 11ACE 12B 13AD 14B 15B 16AD 17C 18B 19ACE 20BD 21A 22B 23AC 24A 25AB 26A 27ACE 28AD 29B 30A 31AC 32B 33C 34A 35A 36A 37A 38AE 39A 40ACF 41A 42AC 43A 44A 45A`.
- Uncertain questions: 20, 27, and 45.
- Scoring: **44/45 (97.8%)**; 28/29 single-response and 16/16 exact-match
  multiple-response.
- Review: [answer-bearing independent assessment](sap-c02-final-freshness-assessment-45q-review-20260823.md).
- Tracker role: one bounded final-freshness assessment followed by tapering;
  not a full mock or booking gate.
