# SAP-C02 Resilience/DR Fresh Spaced Retest — 12 Questions

<!-- markdownlint-disable MD013 MD060 -->

**Created:** 2026-07-26<br>
**Earliest attempt:** 2026-07-27<br>
**Document role:** question-only, closed-book spaced retest

## Attempt Rules

- Do not start before 2026-07-27.
- Close all lessons, reviews, answer keys, documentation, search, and AI tools.
- Use one uninterrupted **35-minute** attempt where practical.
- Questions explicitly marked **Choose TWO** or **Choose THREE** use exact-match
  scoring: no partial credit.
- Record uncertain question numbers without changing the required answer count.
- Freeze and explicitly submit the complete answer set before any scoring or
  explanation.
- No answer key exists in this document. It will be produced only after a frozen
  submission.

## Questions

### 1

A payment-processing workload has an approved recovery time objective of 20
minutes and a recovery point objective of 2 minutes. A complete copy of the
application runs continuously in a second Region at reduced capacity and passes
live synthetic transactions, but production traffic is not normally routed
there. During a Regional failure, the team must scale that environment and
shift production traffic.

Which disaster-recovery pattern is described?

- A. Backup and restore
- B. Pilot light
- C. Warm standby
- D. Multi-site active/active

### 2 — Choose TWO

A regulated workload uses resources supported by AWS Backup. Its recovery
design must survive both loss of the primary Region and compromise of an
administrator in the workload account. Auditors also require evidence that the
recovery points can produce a usable application.

Which TWO actions best satisfy these requirements?

- A. Copy recovery points to a separately administered backup account and a
  destination vault in another Region, with the destination encryption and
  access policies deliberately controlled.
- B. Periodically restore from the destination recovery boundary, validate the
  application and dependencies, and measure the end-to-end recovery time.
- C. Keep all recovery points in the workload account but increase backup
  frequency.
- D. Add another Multi-AZ standby in the primary Region and treat its failover
  test as Regional recovery evidence.
- E. Copy recovery points to another Region while retaining unrestricted delete
  authority for the compromised workload-account administrator.

### 3 — Choose TWO

Two versioned S3 buckets in different Regions sit behind an S3 Multi-Region
Access Point. The application normally writes only to the active Region. After
a failover, it may write to the formerly passive bucket. Objects created in
either Region must subsequently be available through either bucket.

Which TWO controls are required for this design?

- A. Configure two-way S3 Cross-Region Replication between the buckets.
- B. Use the Multi-Region Access Point failover controls to change which Region
  receives requests during the disruption.
- C. Rely on the Multi-Region Access Point to copy objects automatically without
  replication rules.
- D. Put a NAT Gateway in front of each bucket to synchronize objects.
- E. Replace bucket versioning with Route 53 multivalue-answer routing.

### 4

During a scheduled AWS Elastic Disaster Recovery drill, recovery EC2 instances
launch successfully in AWS, but production users continue to reach the original
on-premises servers. An engineer reports that DRS failover has failed.

Which assessment is most accurate?

- A. A recovery drill is deliberately non-disruptive; redirecting production
  traffic is a separate failover action outside DRS.
- B. DRS drills must automatically update every Route 53 hosted zone before the
  recovery instances can start.
- C. Continuous block replication stops whenever drill instances are launched.
- D. DRS is only a centralized backup-policy service and cannot launch EC2
  recovery instances.

### 5 — Choose TWO

An Aurora Global Database supports a multi-Region application. The operations
team must distinguish a planned Regional rotation from the loss of the current
primary Region.

Which TWO statements are correct?

- A. Use a managed switchover for a controlled move when the participating
  clusters and dependencies are healthy and compatible.
- B. Use failover for an unplanned primary-Region outage and account for possible
  data loss from asynchronous replication lag.
- C. A same-Region Multi-AZ standby alone satisfies the Regional-rotation and
  Regional-outage requirements.
- D. A Route 53 health check automatically promotes an Aurora secondary cluster
  to writer without a database failover operation.
- E. A managed switchover requires no engine-version compatibility checks.

### 6

A warm-standby application is healthy in the recovery Region, but a disaster
exercise fails because the application cannot decrypt a required secret. The
KMS key and its usable policy were never established in the recovery path.

What is the most important conclusion?

- A. Lowering the Route 53 record TTL will repair the missing cryptographic
  dependency.
- B. The recovery design is incomplete because regional dependencies such as
  keys, secrets, IAM, and permissions must be recoverable and tested.
- C. Rename the design pilot light; no technical remediation is needed.
- D. S3 Multi-Region Access Points automatically reproduce KMS keys and key
  policies.

### 7 — Choose TWO

A workload has an approved RTO of 30 minutes and RPO of 5 minutes. The company
runs an isolated recovery exercise.

Which TWO measurements directly test those objectives?

- A. Elapsed time from the simulated interruption until the workload is restored
  and business-valid service is available.
- B. Age of the selected consistent recovery point relative to the simulated
  interruption.
- C. Duration of the most recent backup job, without performing a restore.
- D. Number of pages in the recovery runbook.
- E. CloudFormation deployment time alone, excluding data, dependencies,
  validation, and traffic readiness.

### 8

Two Regions actively serve 60% and 40% of normal traffic. Each Region has only
enough capacity for its current share. The architecture review states that the
active/active label guarantees uninterrupted service after either Region is
lost.

Which correction is most accurate?

- A. Active/active requires capacity and load testing against the intended
  failure scenario; the surviving Region is not automatically able to absorb
  all traffic.
- B. Every active/active design must keep exactly 100% of global peak capacity
  idle in each Region.
- C. Route 53 replication automatically adds compute and database capacity to
  the surviving Region.
- D. Because both Regions served traffic before failure, dependency and data
  consistency testing is unnecessary.

### 9 — Choose TWO

A malformed deployment overwrites valid customer records. The changes have
already propagated to a geographically separate replica. The team needs a
known-good state from before the deployment.

Which TWO controls most directly address this recovery requirement?

- A. Retain selectable historical or immutable recovery points through an
  appropriate versioning, point-in-time recovery, or backup mechanism.
- B. Rehearse restoring the selected point, validating application integrity,
  and deliberately returning it to service.
- C. Add traffic routing to the current replica and assume geographic distance
  preserved the old values.
- D. Reduce DNS TTL while continuing to use only the corrupted current state.
- E. Add more read replicas without retaining historical recovery points.

### 10

A global TCP application must present fixed IP addresses that partners can
allowlist. During a Regional endpoint failure, traffic should move to healthy
endpoints without waiting for clients to discard a cached application DNS
answer.

Which AWS service best fits the traffic-entry requirement?

- A. Amazon Route 53 Resolver outbound endpoint
- B. AWS Global Accelerator
- C. Amazon S3 Multi-Region Access Point
- D. AWS Transit Gateway

### 11 — Choose TWO

AWS Backup restore testing successfully creates a restored database resource.
The team wants to claim that the workload recovery objective has been proved.

Which TWO additional pieces of evidence are still required?

- A. Application-level validation that the restored data and required
  dependencies produce correct business behavior.
- B. Measurement of the complete recovery path through readiness for service,
  with any required traffic, reconciliation, and cleanup steps understood.
- C. A screenshot showing only that the original backup job completed.
- D. A statement that every successful resource restore automatically proves
  workload failback.
- E. A shorter written RTO chosen after observing the restore job.

### 12 — Choose THREE

The Energy Data Lakehouse has S3 Versioning, lifecycle controls, versioned code,
Terraform definitions, and evidence that the primary Glue/Athena path worked.
It still has no approved RTO/RPO, independent recovery copy, or tested workload
restore.

Which THREE conclusions are justified?

- A. These are useful recoverability foundations, not proof of a tested DR
  implementation.
- B. An accountable business owner must define objectives and failure scope
  before a paid DR pattern is selected.
- C. A recovery exercise must validate data, infrastructure, identity, keys,
  dependencies, Glue/Athena behavior, and measured recovery outcomes before a
  tested-recovery claim is made.
- D. Multi-site active/active is already the accepted design because it has the
  lowest theoretical RTO.
- E. Re-ingestible source data eliminates the need to assess source retention,
  metadata, permissions, or irreplaceable artifacts.

## Frozen Submission Template

Copy and complete this block without adding explanations:

```text
Start:  12:31
End: 12:47
Uncertain:2,4

1:C
2:AB
3:AB
4:A
5:AB
6:B
7:AB
8:A
9:AB
10:B
11:AB
12:ABC

Submission status: FROZEN
```

## Evidence Boundary

Generating or opening this file does not create a score. After an explicit
frozen submission, the answers will be independently assessed by exact match
and recorded separately. This focused result will remain distinct from a full
75-question simulation and from any live recovery implementation evidence.
