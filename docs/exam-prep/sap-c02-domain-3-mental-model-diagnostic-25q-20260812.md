# SAP-C02 Domain 3 Mental-Model Diagnostic - 25 Questions - 2026-08-12

<!-- markdownlint-disable MD013 MD060 -->

## Purpose and evidence boundary

**Document role:** completed timed question-only focused diagnostic.

This assessment contains 20 questions centered on SAP-C02 Domain 3,
**Continuous Improvement for Existing Solutions**, plus five questions from
other SAP-C02 domains. The cross-domain questions are deliberately mixed in
without labels so that identifying the architectural objective is part of the
exercise.

The Domain 3 coverage spans:

- operational excellence;
- security improvement;
- performance improvement;
- reliability improvement; and
- cost optimization.

There is no answer key, rationale, task label, topic heading, or scoring hint in
this artifact. Scoring begins only after the learner freezes and explicitly
submits the complete answer set. This is focused diagnostic evidence, not a
full 75-question simulation and not booking-gate evidence.

The official exam allows 180 minutes for 75 questions. The proportional time
limit for this 25-question diagnostic is **60 minutes**.

## Attempt rules

1. Set one uninterrupted **60-minute** timer.
2. Do not open revision notes, previous reviews, diagrams, AWS documentation,
   search, AI assistance, or an answer key during the attempt.
3. Select exactly the number of responses requested. A multiple-response item
   receives credit only when the entire selected set is correct.
4. Guess rather than leave a question blank.
5. Record uncertainty only after freezing the answer set.
6. Stop when the timer expires, even if questions remain.
7. Submit the frozen answers, elapsed time, and uncertain question numbers.

Use canonical answer form:

```text
1ABC 2D 3A ... 25BE
Elapsed: 57 minutes
Uncertain: 4, 11, 19
```

## Questions

### 1

A company deploys an Amazon ECS application by replacing all running tasks at
once. A recent release passed container health checks but produced incorrect
business results for 20 minutes before engineers manually restored the prior
version. The company wants production traffic shifted gradually, validation
against the replacement environment, and automatic rollback when either
technical or business metrics breach thresholds.

Which THREE changes best meet these requirements?

- A. Use an AWS CodeDeploy blue/green deployment for the ECS service.
- B. Continue using all-at-once replacement but increase the ECS deployment
  timeout.
- C. Configure separate test and production listener paths for the replacement
  task set.
- D. Use Systems Manager Patch Manager to install the container image.
- E. Associate CloudWatch alarms with the deployment and enable automatic
  rollback.
- F. Delete the original task definition as soon as the new tasks become
  healthy.

### 2

Several applications retrieve the same hardcoded Amazon RDS credential from a
configuration file. A password change caused an outage because the applications
continued using the old value. The company wants automatic rotation with the
least development and operational effort. The requirement does not specify
uninterrupted database access during the brief rotation window.

Which solution is most appropriate?

- A. Store the password in Parameter Store as a standard plaintext parameter
  and update it manually.
- B. Store the credential in Secrets Manager, update the applications to
  retrieve it at runtime, and use single-user rotation.
- C. Store two database credentials in Secrets Manager and always use an
  alternating-users rotation strategy.
- D. Place the password in an encrypted AMI and replace every instance when the
  password changes.

### 3

An Amazon ECS service scales from 20 to 500 tasks during reporting periods.
Each task opens several connections to an Aurora PostgreSQL cluster. The writer
reaches its connection limit, and failovers are prolonged because thousands of
stale connections reconnect simultaneously. The company can change the
database endpoint in application configuration but wants no major rewrite.

Which solution provides the most direct improvement?

- A. Add an Aurora read replica and send every SQL statement to it.
- B. Increase the ECS service's minimum task count to reduce database load.
- C. Place RDS Proxy between the ECS service and the Aurora cluster.
- D. Export the reporting tables to Amazon S3 after every request.

### 4

A company has 80 AWS accounts in AWS Organizations. It wants centrally managed
workforce access, preventive restrictions that member-account administrators
cannot override, and immutable centralized evidence of API activity.

Which THREE actions meet these requirements?

- A. Create matching IAM users independently in all member accounts.
- B. Use IAM Identity Center permission sets and account assignments.
- C. Use security groups to deny unauthorized AWS API calls.
- D. Apply SCPs at the appropriate roots or OUs.
- E. Store the only CloudTrail copy in each workload account.
- F. Create an organization trail that delivers to a separately administered
  Log Archive account.

### 5

An application runs in private subnets in two Availability Zones. Both private
subnets send internet-bound traffic through one NAT gateway in the first
Availability Zone. The company has observed cross-AZ processing charges and
loses outbound connectivity from both zones when that NAT gateway's zone is
impaired.

Which solution best improves the existing design?

- A. Deploy a second internet gateway in the other Availability Zone and retain
  the existing routes.
- B. Route both private subnets through the original NAT gateway to preserve
  connection state.
- C. Replace both private subnets with public subnets.
- D. Deploy one NAT gateway in each Availability Zone and route each private
  subnet to its local NAT gateway.

### 6

A company stores two datasets in Amazon S3. Dataset 1 contains 400 TB of user
documents with access patterns that vary by object and cannot be predicted.
Dataset 2 contains compliance records that are almost never read, must be kept
for 10 years, and may take up to 48 hours to retrieve. The company wants to
minimize storage cost without violating retrieval requirements.

Which configuration is most appropriate?

- A. Dataset 1 in S3 Standard-IA and Dataset 2 in S3 Glacier Instant Retrieval.
- B. Dataset 1 in S3 Intelligent-Tiering and Dataset 2 in S3 Glacier Deep
  Archive.
- C. Both datasets in S3 Standard because their future access cannot be
  guaranteed.
- D. Dataset 1 in S3 One Zone-IA and Dataset 2 in S3 Glacier Flexible Retrieval.

### 7

A company must replatform a self-managed IBM Db2 database to a managed
relational database on AWS with minimal downtime. A separate Windows
application server cannot be modernized and must retain its operating system
and application binaries.

Which solution best meets these requirements?

- A. Use AWS Application Migration Service for the Windows server, and use AWS
  DMS with AWS SCT for the heterogeneous database migration.
- B. Use AWS DataSync for the Windows server and AWS Application Migration
  Service for the database schema conversion.
- C. Use AWS DMS to rehost both servers without schema conversion.
- D. Use Storage Gateway for continuous block replication and convert the
  database after cutover.

### 8

A microservices application runs across eight AWS accounts. Teams have CPU and
memory metrics but cannot identify which downstream call causes intermittent
request latency. Each account's telemetry is viewed separately. The company
wants distributed request tracing and one monitoring account with the least
operational overhead.

Which TWO actions best meet these requirements?

- A. Use CloudTrail Lake as the application request tracer.
- B. Use VPC Flow Logs to reconstruct every application method call.
- C. Instrument the services with AWS Distro for OpenTelemetry and publish
  traces to X-Ray.
- D. Deploy and maintain a separate self-managed tracing cluster in every
  workload account.
- E. Configure CloudWatch cross-account observability links to a central
  monitoring account.

### 9

Developers repeatedly create security-group rules that expose administrative
ports to `0.0.0.0/0`. The security team wants continuous detection and automatic
removal of noncompliant rules while retaining evidence of each remediation.

Which solution provides the most direct improvement?

- A. Use CloudTrail Insights to block every `AuthorizeSecurityGroupIngress`
  call.
- B. Use VPC Flow Logs to remove a rule after a successful connection.
- C. Use an AWS Config rule and associate automatic remediation through a
  Systems Manager Automation runbook.
- D. Use Cost Anomaly Detection to notify the security team.

### 10

A tightly coupled computational-fluid-dynamics workload runs on EC2. Adding
more ordinary instances has not reduced job duration because node-to-node
latency and collective communication dominate processing time.

Which TWO changes are most likely to improve performance?

- A. Distribute the instances across as many Regions as possible.
- B. Place the instances in a cluster placement group.
- C. Replace the instances with burstable general-purpose instances.
- D. Use EFA-capable instances and configure the workload to use EFA.
- E. Place each node behind an Application Load Balancer.

### 11

An Auto Scaling group is designed to add hundreds of EC2 instances during
quarter-end processing. The previous event failed because the account reached
its regional On-Demand vCPU quota before the desired capacity was reached. The
company expects the workload to continue growing.

Which solution provides the best long-term reliability improvement?

- A. Add more scaling policies without reviewing service quotas.
- B. Retry every failed launch indefinitely without a terminal alarm.
- C. Replace CloudWatch alarms with S3 event notifications.
- D. Monitor relevant quota utilization, alarm before available capacity is
  exhausted, and request justified quota increases before growth events.

### 12

A customer-facing web tier has stable baseline usage but may move from EC2 to
Fargate during the next year. A separate image-processing workload is
checkpointed, fault tolerant, and can tolerate interruption. Finance wants the
largest savings without constraining the planned compute migration.

Which purchasing strategy is most appropriate?

- A. Buy EC2 Instance Savings Plans for both workloads.
- B. Buy Compute Savings Plans for the web-tier baseline and use Spot capacity
  for image processing.
- C. Run both workloads entirely On-Demand.
- D. Use Spot capacity for the customer-facing baseline and Standard Reserved
  Instances for image processing.

### 13

A company is designing a new payment platform. The business requires an RPO of
less than one minute and an RTO of 15 minutes after a complete regional outage.
The secondary Region may run at reduced capacity before failover, but the
application must already be deployable and able to serve traffic there.

Which disaster-recovery strategy best meets these requirements?

- A. Store monthly backups in the primary Region only.
- B. Use a pilot light with no application compute running in the secondary
  Region.
- C. Use warm standby with cross-Region data replication, tested failover
  automation, and traffic-routing controls.
- D. Use Multi-AZ resources only in the primary Region.

### 14

An application team believes its Auto Scaling and multi-AZ design will survive
an Availability Zone impairment, but the recovery procedure has never been
tested. The company wants controlled production experiments, reusable failure
scenarios, and automatic termination of an experiment if customer-impact
metrics breach a threshold.

Which TWO actions best meet these requirements?

- A. Create AWS Fault Injection Service experiment templates for the approved
  failure scenarios.
- B. Wait for an actual Availability Zone outage and document the result.
- C. Configure CloudWatch alarms as stop conditions for the experiments.
- D. Use AWS Config to simulate network and instance failures.
- E. Disable monitoring during each experiment to avoid false alarms.

### 15

Operations staff patch 600 EC2 instances manually by connecting through a
bastion host over SSH. Security wants the bastion and inbound SSH removed,
interactive sessions logged, and patching automated during approved maintenance
windows.

Which TWO actions meet these requirements?

- A. Replace the bastion with a larger bastion and rotate its SSH key monthly.
- B. Use Systems Manager Session Manager for interactive access and configure
  session logging.
- C. Schedule an unmonitored operating-system update command in each instance's
  local crontab.
- D. Use AWS Config as the interactive shell service.
- E. Use Systems Manager Patch Manager with patch baselines and maintenance
  windows.

### 16

An application repeatedly reads a small set of hot items from DynamoDB and
requires microsecond response times. The team created a multi-node DAX cluster,
but application traffic continues to go directly to DynamoDB and performance
has not changed.

Which solution completes the performance improvement?

- A. Add a local secondary index with the same partition and sort key.
- B. Request strongly consistent reads through DAX for every operation.
- C. Replace DynamoDB with S3 Standard-IA.
- D. Keep the DAX cluster distributed across Availability Zones and update the
  application to use the DAX client for supported DynamoDB operations.

### 17

Workers consume messages from an SQS Standard queue. A malformed message is
retried indefinitely, and occasional duplicate deliveries cause the workers to
repeat external side effects.

Which TWO improvements most directly address these problems?

- A. Configure a dead-letter queue with an appropriate maximum receive count.
- B. Set the queue visibility timeout to zero.
- C. Delete each message before processing starts.
- D. Make processing idempotent by recording a stable operation identifier with
  a conditional write before applying the side effect.
- E. Replace the queue with an SNS topic that has no retry policy.

### 18

A company has 40 VPCs across four AWS Regions and two data centres. It wants a
transitive hub for VPC connectivity, private connectivity from the data centres,
and bidirectional resolution between on-premises domains and Route 53 private
hosted zones.

Which architecture best meets the requirements?

- A. Use regional Transit Gateways with the required inter-Region peering,
  connect Direct Connect through appropriate gateways and virtual interfaces,
  and deploy Route 53 Resolver inbound and outbound endpoints with forwarding
  rules.
- B. Create a full mesh of VPC peering connections and publish every private
  name in a public hosted zone.
- C. Use one internet gateway as the transitive router for all VPCs and data
  centres.
- D. Use S3 gateway endpoints for transitive routing and DNS forwarding.

### 19

Finance needs monthly analysis of cost by account, application tag, AWS
service, and usage type. Analysts must retain detailed historical line items and
query them with SQL. Existing summary dashboards do not provide sufficient
granularity.

Which solution best meets these requirements?

- A. Use only the current month's AWS Budgets forecast.
- B. Use CloudTrail Lake as the billing line-item store.
- C. Activate the required cost-allocation tags, deliver an AWS Cost and Usage
  Report to S3, and query it with Athena.
- D. Export Trusted Advisor results once per year.

### 20

An organization has 2,000 EC2 instances across many accounts and Regions.
Configuration settings regularly drift from the approved operating-system
baseline, and teams repair each instance manually. The company wants an
organization-scale, repeatable configuration-management mechanism with minimal
custom infrastructure.

Which solution is most appropriate?

- A. Use CloudTrail to rewrite operating-system configuration after every API
  call.
- B. Build separate SSH bastions and shell scripts in every account.
- C. Use Route 53 health checks to apply the operating-system baseline.
- D. Use Systems Manager State Manager associations, distributed through an
  organization-aware Systems Manager configuration where appropriate.

### 21

A company wants stronger ransomware-recovery evidence. Backups must resist
privileged deletion, survive compromise of the workload account, and be proven
restorable on a recurring schedule.

Which THREE actions best meet these requirements?

- A. Keep the only recovery point in the workload account's default backup
  vault.
- B. Use AWS Backup Vault Lock in compliance mode for protected recovery
  points.
- C. Treat successful backup-job status as proof that the application meets its
  recovery objectives.
- D. Maintain supported cross-account copies or logically air-gapped vault
  copies under separate administrative control.
- E. Grant workload administrators permission to delete every recovery point
  during an incident.
- F. Configure AWS Backup restore testing plans and validate the restored
  resources.

### 22

A global multiplayer application uses TCP and UDP endpoints in two AWS Regions.
Users need static anycast IP addresses, traffic acceleration across the AWS
global network, and rapid health-based routing away from an impaired regional
endpoint without depending on DNS-cache expiry.

Which service is the best fit?

- A. CloudFront
- B. AWS Global Accelerator
- C. Route 53 private hosted zones
- D. API Gateway

### 23

A company is designing a new event-ingestion platform. Events for the same
customer must remain ordered, multiple consumers must process the stream
independently, and consumers must be able to replay seven days of history.

Which solution best meets these requirements?

- A. Use Kinesis Data Streams with the customer identifier as the partition key
  and configure the required retention period.
- B. Use an SQS Standard queue and delete messages immediately after the first
  consumer reads them.
- C. Use EventBridge Scheduler to store and replay the events.
- D. Use an SNS topic with email subscriptions as the system of record.

### 24

A production MySQL database runs on a single RDS instance in one Availability
Zone. Nightly snapshots exist, but an instance or Availability Zone failure
causes an outage while engineers restore the latest snapshot. The application
requires automatic failover with minimal architectural change.

Which improvement is most appropriate?

- A. Convert the database to an RDS Multi-AZ deployment.
- B. Create a read replica in the same Availability Zone and require engineers
  to promote it manually.
- C. Take snapshots every hour and retain the single-AZ deployment.
- D. Place the database behind a NAT gateway.

### 25

Applications in private subnets transfer large volumes to S3 and DynamoDB and
retrieve secrets from Secrets Manager. All traffic currently passes through NAT
gateways. The endpoint usage is high enough that the expected endpoint charges
are lower than the existing NAT data-processing charges, and the company wants
to retain private service access.

Which TWO changes best meet these requirements?

- A. Move the applications to public subnets and assign public IP addresses.
- B. Enable S3 Transfer Acceleration for same-Region private traffic.
- C. Add gateway VPC endpoints for S3 and DynamoDB and update the relevant
  route tables and endpoint policies.
- D. Replace the NAT gateways with internet gateways attached to each private
  subnet.
- E. Add a Secrets Manager interface VPC endpoint and configure its security
  groups and private DNS as required.

## Frozen submission

The following learner submission was frozen before marking:

```text
Attempt date: Aug-12-2026
Attempt number: 1
Start: 14:37
End: 15:24
Elapsed: 47 minutes
Uncertain: None

1: ACE
2: B
3: C
4: BDF
5: D
6: B
7: A
8: CE
9: C
10: BD
11: D
12: B
13: C
14: AC
15: BE
16: D
17: AD
18: A
19: C
20: D
21: BDF
22: B
23: A
24: A
25: CE
```
