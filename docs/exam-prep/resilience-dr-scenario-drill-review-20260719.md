# SAP-C02 Resilience and DR Scenario Drill Review - 2026-07-19

<!-- markdownlint-disable MD013 MD060 -->

## Scope and Evidence Boundary

**Document role:** answer-bearing, source-backed review. Start at the
[Exam-Prep Revision Hub](README.md). Do not use this document as a blind
attempt.

This review applies the decision rules from the
[Resilience and DR lesson](aws-resilience-dr-sap-c02-key-lessons-20260718.md),
the
[RTO/RPO decision table](../planning/domain-2-rto-rpo-decision-table-20260718.md),
and the
[Lakehouse recovery mapping](../planning/domain-2-lakehouse-recovery-mapping-20260719.md).
It tests pattern selection, backup-versus-replication boundaries, Regional
failure, cyber recovery, restore testing, and service-specific cues.

Completing this source review alone records documentation coverage only. A
later explicit learner submission is recorded separately in the
[2026-07-20 submission record](resilience-dr-scenario-drill-submission-20260720.md).
That record preserves the score and the learner's no-answer-key attestation
without reclassifying this answer-bearing document as a structurally isolated
blind attempt. Neither artifact is a restore test or proof of a live recovery
path.

## Scenario Questions

### 1. Lowest-cost Regional recovery

A non-critical internal workload can tolerate hours of data loss and up to a
day of outage. The company wants the lowest standing cost and already maintains
versioned infrastructure and application artifacts. Which DR pattern is the
first candidate?

- A. Backup and restore
- B. Pilot light
- C. Warm standby
- D. Multi-site active/active

### 2. Core data services already running

A recovery Region continuously holds replicated databases and the minimum
services needed to protect the data. Application compute and most surrounding
infrastructure will be deployed only after disaster declaration. Which pattern
is described?

- A. Backup and restore
- B. Pilot light
- C. Warm standby
- D. Multi-site active/active

### 3. Complete reduced-capacity environment

A complete application stack is always running in the recovery Region at
reduced capacity. It can process traffic immediately, but it must scale before
carrying the full production load. Which pattern is described?

- A. Backup and restore
- B. Pilot light
- C. Warm standby
- D. Multi-site active/active

### 4. Multi-AZ versus Regional failure

An RDS database uses a Multi-AZ standby and has passed an Availability Zone
failover test. The business now requires the workload to continue after loss of
the entire AWS Region. Which conclusion is correct?

- A. The Multi-AZ test already proves Regional DR
- B. Add another subnet in the same Region
- C. Design a multi-Region recovery strategy and test its data, dependencies,
  traffic shift, and failback
- D. Replace the standby with an S3 gateway endpoint

### 5. S3 replication prerequisites

A design proposes S3 Cross-Region Replication for new raw Lakehouse objects.
Which prerequisite is mandatory for live replication?

- A. Versioning enabled on both source and destination buckets
- B. A NAT Gateway in every Availability Zone
- C. AWS Elastic Disaster Recovery agents on S3
- D. A public bucket policy on the destination

### 6. Replication and corrupted data

An application writes corrupted objects and the writes replicate to the DR
bucket. The design team claims the current replica is sufficient recovery
protection. What is the most important correction?

- A. Replication guarantees a known-good recovery point
- B. Add versioned or independent recovery points and test selection of a
  known-good point
- C. Disable integrity validation to shorten RTO
- D. Replace S3 replication with Multi-AZ RDS

### 7. Cyber-recovery isolation

A compromised workload-account administrator must not be able to delete every
usable recovery point. The accounts are in AWS Organizations, and the recovery
copy must be administered through a separate security boundary. Which control
best addresses that requirement?

- A. Cross-account AWS Backup copies into a separately administered destination
  vault, with retention/immutability controls assessed against the requirement
- B. More frequent backups into the same workload-account vault only
- C. S3 Cross-Region Replication to another bucket in the same account only
- D. An additional Multi-AZ standby in the workload account

### 8. Successful backup jobs but unknown RTO

All scheduled backup jobs report success, but no resource has been restored and
validated. Auditors ask whether the workload can meet its recovery-time
objective. What should happen next?

- A. Treat backup-job success as proof of RTO
- B. Run periodic restore testing, validate the restored resource/application,
  and compare measured time with the approved objective
- C. Lower the documented RTO until it matches the backup schedule
- D. Enable cross-Region replication and skip restore testing

### 9. Whole-server recovery

A company must recover a fleet of supported physical and virtual servers into
AWS using continuous block-level replication and orchestrated launch of
recovery instances. Which service is the closest fit?

- A. AWS Backup only
- B. AWS Elastic Disaster Recovery
- C. S3 Cross-Region Replication
- D. DynamoDB global tables

### 10. DynamoDB logical corruption

A bad deployment writes incorrect values to a DynamoDB table for twenty
minutes. The team needs a table containing data from immediately before the bad
deployment and must validate it before cutover. Which mechanism is the first
fit?

- A. DynamoDB point-in-time recovery to a new table
- B. Add a second Availability Zone to the existing table
- C. Route all traffic to the same current global-table replica
- D. Use AWS DRS to launch an EC2 recovery instance

### 11. RDS availability versus DR

An RDS workload needs automatic failover for an Availability Zone failure and
a separately designed recovery option for Regional loss. Which distinction is
correct?

- A. Multi-AZ is the local high-availability control; a cross-Region replica or
  backup-based recovery path addresses the separate Regional requirement
- B. A same-Region read replica automatically replaces Multi-AZ failover
- C. Multi-AZ synchronously replicates to every AWS Region
- D. A cross-Region read replica is always synchronous and needs no promotion

### 12. Current Lakehouse recovery claim

The Energy Data Lakehouse has live S3 Versioning and lifecycle controls,
versioned code and Terraform, a remote state backend, and previously successful
Glue/Athena validation. It has no approved RTO/RPO, independent Lakehouse
backup, cross-Region data copy, or recovery drill. Which statement is accurate?

- A. The Lakehouse has a verified Regional DR implementation
- B. The Lakehouse has recoverability foundations, but tested workload recovery
  and a selected DR pattern are not proved
- C. Terraform definitions prove the recovery-time objective
- D. Previous successful queries prove that failback will work

## Answer Key

| Question | Answer | Durable decision rule | Main trap |
|---:|:---:|---|---|
| 1 | A | Choose the least costly and least complex pattern with a credible path to the approved objectives; long tolerances and low standing-cost priority point first to backup and restore. | Selecting an always-on pattern without a requirement that justifies its cost. |
| 2 | B | Pilot light keeps the critical core and replicated data running while other application capacity is created after declaration. | Calling any reduced DR footprint warm standby even when it cannot yet process requests. |
| 3 | C | Warm standby is a complete, functional, reduced-capacity environment that primarily needs scale-up and traffic redirection. | Confusing a complete running stack with a data-only pilot light. |
| 4 | C | Multi-AZ availability and multi-Region DR address different failure scopes; Regional recovery needs its own data, dependency, traffic, and failback design. | Generalizing an AZ failover result to loss of the Region. |
| 5 | A | S3 live replication requires versioning on both source and destination plus appropriate replication permissions. | Adding unrelated networking or making the destination public. |
| 6 | B | Replication improves copy freshness and geographic availability but can reproduce a bad write; retain selectable historical/isolated recovery points and test them. | Treating the newest copy as necessarily good. |
| 7 | A | Cross-account copies move recovery ownership outside the compromised workload-account boundary; vault access, encryption, retention, Organizations, and restore authority still require deliberate design. | Treating another Region in the same authority boundary as cyber isolation. |
| 8 | B | Backup completion proves that a recovery point was created, not that it restores correctly or within workload RTO; restore and application validation must be measured. | Reporting green backup jobs as end-to-end recovery proof. |
| 9 | B | AWS DRS is the server-recovery fit for continuous block replication and recovery-instance launch orchestration. | Using a centralized backup-policy service as the default whole-server replication answer. |
| 10 | A | DynamoDB PITR restores a selected historical point to a new independent table, allowing validation before traffic changes. | Using a current replica to recover from a bad write that may already have replicated. |
| 11 | A | RDS Multi-AZ provides local high availability; cross-Region replicas or backups address a separately stated Regional recovery requirement and still need promotion/traffic/failback planning. | Treating standby and read-replica behavior as interchangeable. |
| 12 | B | Versioning, code, state, and prior primary-path evidence are recovery foundations; only an exercised, business-valid path supplies tested recovery evidence. | Converting documentation or previous healthy operation into a DR claim. |

## Detailed Review

### Pattern selection: Questions 1-3

AWS orders the common multi-Region patterns by increasing cost/complexity and
decreasing recovery time: backup and restore, pilot light, warm standby, then
multi-site active/active. The decisive cue is what already runs in the recovery
Region:

- **backup and restore:** deploy infrastructure and restore data after the
  event;
- **pilot light:** critical data/core services run, but the workload cannot
  process requests without deploying or starting more components;
- **warm standby:** the complete workload runs at reduced capacity and can
  process traffic before scale-up; and
- **multi-site active/active:** multiple Regions actively serve production
  traffic and must handle cross-Region data consistency and conflict concerns.

Do not memorize generic time labels as guarantees. The business objective,
dependencies, quotas, automation, workload size, and observed tests determine
whether a pattern is credible.

### Failure scope and replication: Questions 4-6

An Availability Zone result does not answer a Region-loss requirement. A
multi-Region design must identify the recovery data, deployable capacity,
identity, keys, secrets, DNS/traffic action, networking, quotas, operators, and
failback path.

S3 replication requires versioning on both buckets and permission for S3 to
replicate on the owner's behalf. Replication configuration also needs explicit
decisions for existing objects, encryption, ownership, delete markers,
monitoring, time objectives, and cost. A version-specific deletion in the
source is not propagated as deletion of that version in the destination, while
delete-marker behavior depends on the replication-rule configuration. Those
details help isolation, but they do not make the newest replica a known-good
recovery point after corruption.

### Cyber recovery and testing: Questions 7-8

Cross-account AWS Backup copies can place recovery points in a separately
administered account in the same Organization. The design still needs supported
resource/Region checks, destination-vault access, encryption-key compatibility,
retention or immutability, recovery-role access, monitoring, and a tested path
back to a usable workload.

AWS Backup restore testing can schedule restores, record restore-job duration,
and optionally trigger validation. A successful restore job alone still does
not prove application integrity, dependency readiness, business validation, or
failback. Measure end-to-end recovery against the approved objective and retain
cleanup/cost evidence for the isolated test scope.

### Service-specific decisions: Questions 9-11

AWS DRS continuously replicates source-server block changes into a staging
area and orchestrates recovery instances. It is a server recovery tool, not the
general answer for S3, DynamoDB, managed database backup policy, or every
serverless dependency. Network throughput, staging resources, boot time,
application consistency, traffic redirection, and failback still affect the
actual outcome.

DynamoDB PITR supplies historical recovery points and restores to a new table;
the new table must be validated and deliberately placed into service. Global
tables serve multi-Region availability, but a current replica does not replace
historical recovery from a replicated bad write.

RDS Multi-AZ uses a standby for high availability within a Region. Read replicas
use asynchronous replication and can support read scaling or a separately
designed recovery path, including cross-Region use and promotion. The correct
answer depends on whether the requirement is local availability, read scaling,
or Regional recovery.

### Lakehouse boundary: Question 12

The current Lakehouse evidence supports only this statement:

> Recoverability foundations are recorded; tested workload recovery is not
> proved.

The repository maps a possible same-bucket logical recovery and reconstruction
path, but no accountable owner has approved an RTO/RPO or failure scope. It
does not prove independent or immutable recovery points, cross-account or
cross-Region data protection, clean infrastructure reconstruction, restored
Glue/Athena operation, business validation, traffic/publication, or failback.

## Cross-Scenario Decision Map

| Requirement cue | First mechanism or question to assess | Evidence boundary |
|---|---|---|
| Long tolerance and lowest standing cost | Backup and restore | Restore/rebuild time and recovery-point age must be tested against approved objectives |
| Critical data/core only in recovery Region | Pilot light | Remaining provisioning, configuration, quota, and validation steps must fit RTO |
| Complete reduced-capacity stack already running | Warm standby | Prove immediate function, scale-up, dependencies, and traffic shift |
| Multiple Regions actively serving | Multi-site active/active | Prove conflict handling, full-load survival, data recovery, and Region-isolation behavior |
| AZ failure | Multi-AZ/high-availability control | Does not prove Regional recovery |
| Regional S3 copy | S3 Cross-Region Replication or backup copy according to objective | Versioning, permissions, existing objects, deletes, encryption, monitoring, and recovery use remain explicit |
| Workload-account compromise | Separately administered cross-account/air-gapped recovery boundary | Geographic separation alone is insufficient |
| Green backup job | Restore testing plus workload validation | Backup success is not RTO or business recovery proof |
| Whole-server continuous replication | AWS Elastic Disaster Recovery | Does not replace recovery design for managed/serverless dependencies |
| DynamoDB bad write | PITR to a new table | Validate and redirect deliberately; a current global replica may contain the same bad write |
| RDS AZ availability | Multi-AZ | Cross-Region recovery is a separate requirement |
| Current Lakehouse posture | Preserve versioning and reconstruction foundations; define objectives before selecting stronger controls | No live recovery pattern or tested result exists |

## Review Outcome and Next Gate

The twelve scenarios cover every source-backed review category required by the
Resilience/DR study plan:

- DR-pattern selection;
- backup-versus-replication boundaries;
- Regional versus Availability Zone failure;
- cyber-recovery isolation;
- restore testing;
- AWS Backup versus AWS DRS;
- S3, DynamoDB, and RDS recovery cues; and
- the current Lakehouse evidence boundary.

The source-backed scenario gate is complete in the repository. On 2026-07-20, the learner
explicitly submitted all 12 choices and scored 12/12, stating that the answers
were completed on the first attempt without looking at the answer key. The
[submission record](resilience-dr-scenario-drill-submission-20260720.md)
preserves that result, its untimed status, and the caveat that the source file
was answer-bearing rather than structurally isolated. A fresh question-only
spaced retest is the next Resilience/DR evidence gate.

## Official AWS References

- [REL13-BP02: Use defined recovery strategies to meet recovery objectives](https://docs.aws.amazon.com/wellarchitected/latest/framework/rel_planning_for_recovery_disaster_recovery.html)
- [Disaster recovery options in the cloud](https://docs.aws.amazon.com/whitepapers/latest/disaster-recovery-workloads-on-aws/disaster-recovery-options-in-the-cloud.html)
- [AWS Backup restore testing](https://docs.aws.amazon.com/aws-backup/latest/devguide/restore-testing.html)
- [AWS Backup cross-account copies](https://docs.aws.amazon.com/aws-backup/latest/devguide/create-cross-account-backup.html)
- [AWS Backup logically air-gapped vaults](https://docs.aws.amazon.com/aws-backup/latest/devguide/logicallyairgappedvault.html)
- [AWS Elastic Disaster Recovery concepts](https://docs.aws.amazon.com/drs/latest/userguide/CloudEndure-Concepts.html)
- [AWS DRS recovery and failback](https://docs.aws.amazon.com/drs/latest/userguide/failback.html)
- [S3 replication requirements](https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication-requirements.html)
- [What Amazon S3 replication does and does not replicate](https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication-what-is-isnot-replicated.html)
- [DynamoDB point-in-time recovery](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Point-in-time-recovery.html)
- [Amazon RDS read replicas](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_ReadRepl.html)
