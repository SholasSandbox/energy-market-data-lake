<!-- markdownlint-disable MD013 MD060 -->

# SAP-C02 Full Mock 004 — Closed-Book Spaced Retest

**Created:** 2026-08-07

**Questions:** 8

**Suggested limit:** 24 minutes

**Mode:** Fresh, closed-book, close-distractor remediation
**Submission status:** FROZEN

## Purpose and Evidence Boundary

This question-only retest covers the four genuine themes exposed by Full Mock
004:

1. logically air-gapped backup isolation and demonstrated recovery;
2. CloudFront origin-failover HTTP-method boundaries;
3. complete ECS blue/green configuration with CodeDeploy; and
4. DynamoDB MREC/MRSC selection and the MRSC TTL restriction.

The questions use fresh scenarios and distractors. They do not reuse Full Mock
004 wording. This is focused remediation evidence, not a full-domain mock and
not booking evidence.

No answer key, rationale, lesson, review, documentation, search result, or AI
assistance should be opened before the submission is frozen.

## Attempt Rules

1. Answer all eight questions in one uninterrupted sitting.
2. Select one answer unless the question says **Choose TWO** or **Choose
   THREE**.
3. Multiple-response questions receive credit only for an exact match: every
   required option and no additional option.
4. Record uncertain question numbers only after choosing all answers.
5. Enter start and end times, then change `Submission status` to `FROZEN` in
   both the header and submission template.
6. Do not revise a frozen response before marking.

---

## Questions

### 1. Backup isolation boundary

A company protects workloads with AWS Backup. Its incident-response team wants
recovery points whose backup data is stored in an AWS Backup service-owned
account, is always protected by Vault Lock in compliance mode, and can be
shared with a named recovery account for restoration.

Which solution is designed for these requirements?

A. Store the recovery points in the workload account's default backup vault
and enable governance-mode Vault Lock.<br>
B. Create an EBS Recycle Bin retention rule in the recovery account.<br>
C. Store the recovery points in an AWS Backup logically air-gapped vault and
share the vault with the recovery account through AWS RAM.<br>
D. Place the recovery points in an S3 bucket with S3 Object Lock and share the
bucket through an S3 access point.

### 2. Ransomware recovery controls — Choose THREE

An enterprise is designing recovery for a ransomware event that might include
compromise of privileged identities in a workload account. Backups must resist
early deletion, remain recoverable outside the compromised administrative
boundary, and be regularly demonstrated as restorable.

Which THREE controls best satisfy the requirements?

A. Apply AWS Backup Vault Lock in compliance mode with carefully selected
retention settings.<br>
B. Keep the only recovery point in the workload account's default vault so the
application team can restore it quickly.<br>
C. Maintain a supported cross-account copy or logically air-gapped recovery
point with separately controlled recovery access.<br>
D. Configure AWS Backup restore testing and perform validation appropriate to
the restored workload.<br>
E. Use governance-mode Vault Lock as proof that no sufficiently privileged
identity can remove the lock.<br>
F. Treat a successful backup-job status as proof that the application can be
restored within its RTO.

### 3. CloudFront method boundary

A CloudFront distribution uses an origin group with an HTTPS API as the
primary origin and a standby API as the secondary origin. When the primary
returns a configured failover status, `GET` requests reach the standby, but
`POST` requests do not.

What is the correct explanation?

A. CloudFront built-in origin failover applies only to eligible `GET`, `HEAD`,
and `OPTIONS` viewer requests, not write methods such as `POST`.<br>
B. CloudFront requires the secondary API to use the same Availability Zone as
the primary before it can fail over a `POST` request.<br>
C. CloudFront origin failover supports `POST` only when Origin Shield is
enabled in the secondary Region.<br>
D. Lambda@Edge must sign every `POST` request before CloudFront can apply its
built-in origin-group failover.

### 4. Read and write continuity — Choose TWO

A global application uses CloudFront for cacheable reads and an API for
state-changing writes. The architect must provide origin failure handling for
reads and a safe continuity design for writes.

Which TWO design decisions are correct?

A. Configure a CloudFront origin group and applicable failover status codes
for eligible `GET`, `HEAD`, and cached `OPTIONS` requests.<br>
B. Rely on the same CloudFront origin group to retry `POST`, `PUT`, and
`DELETE` requests automatically against the secondary origin.<br>
C. Design write continuity separately with deliberate routing, idempotency,
retry, and data-consistency controls.<br>
D. Add Lambda@Edge solely to expand CloudFront's built-in origin failover to
every HTTP method.<br>
E. Put both origins in one Availability Zone because CloudFront cannot fail
over between Regions.

### 5. ECS blue/green validation path

An ECS service behind an Application Load Balancer needs blue/green releases.
The team must send test traffic to the replacement task set before gradually
shifting production traffic, and it must be able to stop a deployment when a
CloudWatch alarm enters the alarm state.

Which solution is the best fit?

A. Use an ECS CodeDeploy blue/green deployment with two target groups, a
production listener, an optional test listener on the same load balancer, and
the alarm configured in the deployment group.<br>
B. Use the ECS rolling deployment controller with one target group and a Route
53 weighted record for each individual task.<br>
C. Use a CloudFront origin group as the ECS deployment controller and direct
the secondary origin to the replacement task definition.<br>
D. Create two ECS clusters connected through Transit Gateway and use BGP local
preference to shift application requests.

### 6. Complete CodeDeploy configuration — Choose THREE

A company is preparing an Amazon ECS service for CodeDeploy blue/green
deployments. Production traffic and pre-traffic validation must reach separate
task sets during deployment.

Which THREE elements belong in the design?

A. Define two load-balancer target groups so CodeDeploy can manage the original
and replacement task sets.<br>
B. Configure a production listener and, for the stated pre-traffic path, a test
listener on the same Application or Network Load Balancer.<br>
C. Supply an AppSpec revision that identifies the replacement task definition
and its container name and port.<br>
D. Use one target group only because CodeDeploy changes the target group's VPC
between the blue and green task sets.<br>
E. Replace the CodeDeploy deployment controller with an ECS rolling update;
otherwise test traffic cannot reach the replacement task set.<br>
F. Configure CloudFront origin failover because it creates the green ECS task
set and terminates the blue task set.

### 7. MRSC session expiry

A globally distributed application needs strongly consistent DynamoDB reads
in multiple Regions with zero RPO. Its proposed table design also depends on
DynamoDB TTL to remove expired sessions automatically. The architect proposes
two replica Regions and one witness Region.

What should the architect conclude?

A. The topology is valid and the witness enables TTL for the two replicas.<br>
B. The topology must use three full replicas because TTL is unavailable only
when a witness is present.<br>
C. The expiry design must change because DynamoDB TTL is not supported for
MRSC global tables, with or without a witness.<br>
D. TTL becomes synchronous automatically when an MRSC table uses strongly
consistent reads.

### 8. Separate consistency requirements — Choose TWO

An enterprise has two DynamoDB workloads:

- a settlement table requires strongly consistent reads across Regions and
  zero RPO but does not use TTL; and
- a session table can tolerate eventual cross-Region consistency and requires
  DynamoDB TTL.

Which TWO choices correctly map the global-table modes to these requirements?

A. Use MRSC for the settlement table in exactly three Regions, implemented as
three replicas or two replicas and one witness.<br>
B. Use MREC for the session table because MREC supports TTL.<br>
C. Use MRSC with a witness for the session table because the witness adds TTL
processing without storing a full replica.<br>
D. Use MREC for the settlement table because MREC provides strongly consistent
cross-Region reads and zero RPO.<br>
E. Use MRSC for both tables because TTL is supported by every global-table
consistency mode.

---

## Frozen Submission Template

```text
Start:  16:30
End: 16:52
Uncertain: 2,6

1:C
2:ACD
3 A
4:AD
5:A
6:ABC
7:C
8:AB

Submission status: FROZEN
```

The frozen submission was independently assessed after completion. Keep the
question-only attempt above separate from the
[answer-bearing review](sap-c02-full-mock-004-spaced-retest-review-20260807.md).
