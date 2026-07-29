# SAP-C02 Resilience/DR Fresh Spaced Retest Review — 2026-07-27

<!-- markdownlint-disable MD013 MD060 -->

**Last revised:** 2026-07-27<br>
**Document role:** answer-bearing assessment; open only after submission

## Evidence Boundary

This review independently assesses the frozen answer set preserved in the
[question-only retest](sap-c02-resilience-dr-spaced-retest-12q-20260726.md).
The learner completed the test from 12:31 to 12:47 on 2026-07-27, recorded
Questions 2 and 4 as uncertain, and explicitly froze the submission before
scoring.

This is focused, structurally isolated Resilience/DR retention evidence. It is
not a full Domain 2 examination, a 75-question simulation, or proof of a live
backup, restore, replication, failover, or failback implementation.

## Assessment

Multiple-response questions use exact-match scoring with no partial credit.

| Question | Type | Frozen answer | Key | Result |
|---:|---|:---:|:---:|:---:|
| 1 | Single | C | C | Correct |
| 2 | Choose TWO | AB | AB | Correct |
| 3 | Choose TWO | AB | AB | Correct |
| 4 | Single | A | A | Correct |
| 5 | Choose TWO | AB | AB | Correct |
| 6 | Single | B | B | Correct |
| 7 | Choose TWO | AB | AB | Correct |
| 8 | Single | A | A | Correct |
| 9 | Choose TWO | AB | AB | Correct |
| 10 | Single | B | B | Correct |
| 11 | Choose TWO | AB | AB | Correct |
| 12 | Choose THREE | ABC | ABC | Correct |

**Score: 12/12 (100%).**

- Single-response: **5/5**.
- Multiple-response: **7/7** by exact match.
- Duration: **16 minutes** of the recommended 35 minutes.
- Uncertain responses: **2/2 correct**.

## Decision Review

| Question | Durable decision rule |
|---:|---|
| 1 | A complete reduced-capacity recovery environment that is already running but must scale and receive redirected production traffic is warm standby. |
| 2 | Cross-account plus cross-Region recovery separates authority and geography; periodic restore and application validation prove whether the recovery point is usable within the objective. |
| 3 | S3 Multi-Region Access Point failover controls route requests; two-way Cross-Region Replication keeps buckets synchronized when writes can occur after failover. |
| 4 | A DRS recovery drill launches isolated drill instances without moving production traffic; traffic redirection is a separate failover operation outside DRS. |
| 5 | Aurora Global Database switchover is for a controlled healthy move; unplanned failover can have a non-zero RPO because of asynchronous replication lag. |
| 6 | A DR environment is not recoverable if dependencies such as KMS keys, secrets, IAM permissions, certificates, or quotas are missing or untested. |
| 7 | RTO measures time to business-valid service; RPO measures the age of the selected consistent recovery point relative to the disruption. |
| 8 | Active/active describes traffic service, not automatic spare capacity; the surviving footprint must be sized and tested against the intended failure. |
| 9 | Geographic replication can reproduce corruption; historical or immutable recovery points plus rehearsed validation are required for known-good recovery. |
| 10 | Global Accelerator supplies fixed global entry IP addresses and routes new connections to healthy endpoints without relying on clients to refresh an application DNS answer. |
| 11 | A successful resource restore is not end-to-end workload proof; application validation and measured service readiness remain required. |
| 12 | Versioning, lifecycle, code, and Terraform are recovery foundations; business objectives and an exercised dependency-complete recovery path are still required. |

## Source Verification

- [AWS disaster-recovery strategies](https://docs.aws.amazon.com/whitepapers/latest/disaster-recovery-workloads-on-aws/disaster-recovery-options-in-the-cloud.html)
- [AWS Backup cross-account copies](https://docs.aws.amazon.com/aws-backup/latest/devguide/create-cross-account-backup.html)
- [AWS Backup restore-test validation](https://docs.aws.amazon.com/aws-backup/latest/devguide/restore-testing-validation.html)
- [S3 Multi-Region Access Point failover controls](https://docs.aws.amazon.com/AmazonS3/latest/userguide/MrapFailover.html)
- [AWS Elastic Disaster Recovery recovery and failback](https://docs.aws.amazon.com/drs/latest/userguide/failback.html)
- [Aurora Global Database switchover and failover](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-global-database-disaster-recovery.html)
- [AWS Global Accelerator endpoints](https://docs.aws.amazon.com/global-accelerator/latest/dg/about-endpoints.html)
- [AWS Well-Architected disaster-recovery planning](https://docs.aws.amazon.com/wellarchitected/latest/framework/rel-13.html)

## Outcome and Next Gate

No wrong-answer entry or immediate Resilience/DR remediation is required. The
fresh spaced-recall gate is complete. Continue monitoring the same decisions in
independent full mocks; a recurrence creates a new remediation item.

The next tracker-ordered study gate is the six-question non-relational database
spaced retest no earlier than 2026-07-28, followed by full mock 002 under the
two-mock weekly cadence.
