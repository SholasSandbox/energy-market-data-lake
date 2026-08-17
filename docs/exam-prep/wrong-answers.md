# SAP-C02 Wrong-Answer Log

<!-- markdownlint-disable MD013 MD060 -->

**Last revised:** 2026-08-15

This log is the durable exam-prep companion to the tracker wrong-answer table.
It is seeded from the Lakehouse repository tracker and is not Lakehouse
implementation evidence from any external tutorial workspace.

**Document role:** cumulative remediation and status log. Use the
[Exam-Prep Revision Hub](README.md) to choose the correct workflow. Do not open
this log while completing a blind review cycle.

## How to Use This Log

1. During **test**, keep this document closed.
2. After explicit submission, compare the answer with the applicable decision
   rule and explain the losing choice.
3. Add or update an entry only for a genuine miss or material precision gap.
4. Schedule a spaced blind retest; do not count rereading as a review cycle.
5. Keep untimed recall separate from timed-practice and booking evidence.

For the active folder sequence and document status, start at the
[Exam-Prep Revision Hub](README.md). Review Cycle 2 is preserved in the
[four-question retention test](wrong-answer-review-cycle-2-blind-attempt-20260715.md),
including its initial drafts, corrected final submission, and assessment. The
clean 25-question mixed Block 2 remains separate practice evidence. Full mocks
001-007 and their narrow traps are recorded below. Full Mock 003 produced no
new miss; Full Mock 004 exposed four themes, and its 2026-08-07 spaced retest
scored 7/8. Full Mock 005 then scored 73/75: it closed the remaining
Lambda@Edge transfer gap, produced a single-response ARC over-selection, and
exposed one genuine new AS2-versus-Amazon-MQ service-selection gap. Full Mock
006 scored 71/75 on the complexity-calibrated paper and passed ARC and AS2
transfer, but exposed four confident solution-composition misses. Full Mock
007 then scored 75/75 and transferred all four Mock 006 remediation targets.

The separate [AWS Skill Builder assessment review](aws-skill-builder-sap-c02-assessment-review-20260809.md)
records official-practice attempt 2, all 30 keyed misses, eleven confident
misses, and the two dated-key exceptions. It is linked rather than duplicated
here because it is broad paused-assessment calibration, not a focused blind
retest or timed mock.

## Quick Remediation and Reference Index

Use the entry for the mistake and remediation status, the local note for the
exam-oriented mental model, and the AWS source when exact service behaviour or
scope needs to be rechecked.

| Weak area | Durable entry | Local revision | Official AWS documentation |
|---|---|---|---|
| Hybrid private DNS | [Hybrid DNS Private Resolution](#2026-06-19-hybrid-dns-private-resolution) | [Route 53 lesson](revision-notes/targeted-lessons/route-53-sap-c02-key-lessons-20260715.md) | [Resolver hybrid forwarding](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/resolver-overview-DSN-queries-to-vpc.html) |
| Urgent data-centre exit | [Urgent Data-Centre Exit](#2026-06-19-urgent-data-centre-exit) | [Migration lesson](revision-notes/targeted-lessons/aws-migration-discovery-transfer-tracking-sap-c02-key-lessons-20260728.md) | [Application Migration Service rehost pattern](https://docs.aws.amazon.com/prescriptive-guidance/latest/migration-database-rehost-tools/mgn.html) |
| Private multi-VPC hybrid architecture | [Private Hybrid Network Architecture](#2026-07-01-private-hybrid-network-architecture) | [Networking lesson](revision-notes/targeted-lessons/aws-networking-sap-c02-key-lessons-20260717.md) | [Direct Connect gateways](https://docs.aws.amazon.com/directconnect/latest/UserGuide/direct-connect-gateways.html) · [Resolver hybrid forwarding](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/resolver-overview-DSN-queries-to-vpc.html) |
| Replayable ordered event ingestion | [Replayable Ordered Event Ingestion](#2026-07-01-replayable-ordered-event-ingestion) | [Review Cycle 2 explanation](wrong-answer-review-cycle-2-blind-attempt-20260715.md#final-assessment) | [Kinesis concepts](https://docs.aws.amazon.com/streams/latest/dev/key-concepts.html) · [Enhanced fan-out consumers](https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html) |
| ECS blue/green deployment and rollback | [ECS Blue/Green Deployment Versus Patch Manager](#2026-07-21-ecs-bluegreen-deployment-versus-patch-manager) | [Timed diagnostic review](sap-c02-mixed-diagnostic-30q-review-20260721.md) · [Mock 004 retest review](sap-c02-full-mock-004-spaced-retest-review-20260807.md) | [ECS blue/green deployments](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/deployment-type-bluegreen.html) · [Patch Manager](https://docs.aws.amazon.com/systems-manager/latest/userguide/patch-manager.html) |
| SCP scope versus permissions boundary | [SCP Versus IAM Permissions Boundary](#2026-07-23-scp-versus-iam-permissions-boundary) | [Hidden-gap model](sap-c02-hidden-gap-model-review-20260725.md#model-1---scp-versus-iam-permissions-boundary) · [Retest review](sap-c02-scp-permissions-boundary-closed-book-retest-review-20260724.md) | [Organizations SCPs](https://docs.aws.amazon.com/organizations/latest/userguide/orgs_manage_policies_scps.html) · [IAM permissions boundaries](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_boundaries.html) |
| DynamoDB write sharding and read fan-out | [DynamoDB Write Sharding and Read Fan-Out](#2026-07-23-dynamodb-write-sharding-and-read-fan-out) | [Hidden-gap model](sap-c02-hidden-gap-model-review-20260725.md#model-2---dynamodb-write-sharding-and-fan-out-reads) | [DynamoDB write sharding](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-sharding.html) |
| DynamoDB lifecycle, change processing, and recovery | [DynamoDB Lifecycle and Recovery Feature Mapping](#2026-07-25-dynamodb-lifecycle-and-recovery-feature-mapping) | [Database lesson](revision-notes/targeted-lessons/aws-non-relational-databases-sap-c02-key-lessons-20260724.md) · [Diagnostic review](sap-c02-non-relational-databases-diagnostic-review-20260725.md) | [TTL](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html) · [Streams](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html) · [PITR restore](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/pointintimerecovery_restores.html) · [GSIs](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html) |
| Time-series history versus latest state | [Time-Series History Versus Latest-State Lookup](#2026-07-25-time-series-history-versus-latest-state-lookup) | [Database lesson](revision-notes/targeted-lessons/aws-non-relational-databases-sap-c02-key-lessons-20260724.md) · [Diagnostic review](sap-c02-non-relational-databases-diagnostic-review-20260725.md#17---time-series-selection) | [Timestream data modelling](https://docs.aws.amazon.com/timestream/latest/developerguide/data-modeling.html) · [DynamoDB core components](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.CoreComponents.html) |
| Physical-server dependency discovery | [Application Discovery Agent Versus Agentless Collection](#2026-07-28-application-discovery-agent-versus-agentless-collection) | [Migration lesson](revision-notes/targeted-lessons/aws-migration-discovery-transfer-tracking-sap-c02-key-lessons-20260728.md) | [Discovery Agent](https://docs.aws.amazon.com/application-discovery/latest/userguide/discovery-agent.html) · [Agentless Collector](https://docs.aws.amazon.com/application-discovery/latest/userguide/agentless-collector.html) |
| Workforce AWS-account access | [IAM Identity Center Versus Cognito](#2026-07-29-iam-identity-center-versus-cognito) | [Full mock 002 review](sap-c02-full-mock-002-review-20260729.md) · [8/8 spaced-retest assessment](sap-c02-full-mock-002-spaced-retest-review-20260801.md) | [Identity Center account assignments](https://docs.aws.amazon.com/singlesignon/latest/userguide/assignusers.html) · [Cognito identity pools](https://docs.aws.amazon.com/cognito/latest/developerguide/cognito-identity.html) |
| Migration Hub home Region | [Migration Hub Home Region Versus Data Transfer](#2026-07-29-migration-hub-home-region-versus-data-transfer) | [Migration lesson](revision-notes/targeted-lessons/aws-migration-discovery-transfer-tracking-sap-c02-key-lessons-20260728.md) · [Full mock 002 review](sap-c02-full-mock-002-review-20260729.md) · [8/8 spaced-retest assessment](sap-c02-full-mock-002-spaced-retest-review-20260801.md) | [Migration Hub home Region](https://docs.aws.amazon.com/migrationhub/latest/ug/home-region.html) · [What DataSync transfers](https://docs.aws.amazon.com/datasync/latest/userguide/what-is-datasync.html) |
| DAX application integration | [DAX Cluster and Client](#2026-07-29-dax-cluster-and-client) | [Database lesson](revision-notes/targeted-lessons/aws-non-relational-databases-sap-c02-key-lessons-20260724.md) · [Full mock 002 review](sap-c02-full-mock-002-review-20260729.md) · [8/8 spaced-retest assessment](sap-c02-full-mock-002-spaced-retest-review-20260801.md) | [DAX request path](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DAX.concepts.html) · [DAX cluster components](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DAX.concepts.cluster.html) |
| Private on-premises S3 access | [S3 Interface Versus Gateway Endpoint](#2026-07-29-s3-interface-versus-gateway-endpoint) | [Networking lesson](revision-notes/targeted-lessons/aws-networking-sap-c02-key-lessons-20260717.md) · [Full mock 002 review](sap-c02-full-mock-002-review-20260729.md) · [8/8 spaced-retest assessment](sap-c02-full-mock-002-spaced-retest-review-20260801.md) | [S3 gateway endpoint limits and hybrid interface path](https://docs.aws.amazon.com/vpc/latest/privatelink/vpc-endpoints-s3.html) |
| Backup isolation and restore evidence | [Logically Air-Gapped Backup and Restore Evidence](#2026-08-05-logically-air-gapped-backup-and-restore-evidence) | [Resilience/DR lesson](revision-notes/targeted-lessons/aws-resilience-dr-sap-c02-key-lessons-20260718.md#aws-backup-isolation-and-restore-evidence) · [Mock 004 retest review](sap-c02-full-mock-004-spaced-retest-review-20260807.md) | [Logically air-gapped vault](https://docs.aws.amazon.com/aws-backup/latest/devguide/logicallyairgappedvault.html) · [Vault Lock](https://docs.aws.amazon.com/aws-backup/latest/devguide/vault-lock.html) · [Restore testing](https://docs.aws.amazon.com/aws-backup/latest/devguide/restore-testing.html) |
| CloudFront origin-failover methods and write continuity | [CloudFront Origin-Failover Method Boundary](#2026-08-05-cloudfront-origin-failover-method-boundary) | [Resilience/DR lesson](revision-notes/targeted-lessons/aws-resilience-dr-sap-c02-key-lessons-20260718.md#cloudfront-origin-failover-boundary) · [Mock 004 retest review](sap-c02-full-mock-004-spaced-retest-review-20260807.md) | [CloudFront origin failover](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/high_availability_origin_failover.html) · [Lambda@Edge events](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/lambda-event-structure.html) |
| DynamoDB MRSC TTL restriction | [DynamoDB MRSC Versus TTL](#2026-08-05-dynamodb-mrsc-versus-ttl) | [Non-relational database lesson](revision-notes/targeted-lessons/aws-non-relational-databases-sap-c02-key-lessons-20260724.md#global-tables-mrec-versus-mrsc) · [Mock 004 retest review](sap-c02-full-mock-004-spaced-retest-review-20260807.md) | [DynamoDB global-table security](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/globaltables-security.html) |
| ARC routing-control single-response discipline | [ARC Routing-Control Over-Selection](#2026-08-07-arc-routing-control-over-selection) | [Core edge and DNS note](revision-notes/core/03-load-balancing-dns-edge.md#application-recovery-controller-routing-controls) · [Mock 005 review](sap-c02-full-mock-005-review-20260807.md) | [ARC routing control](https://docs.aws.amazon.com/r53recovery/latest/dg/routing-control.html) |
| AS2 versus managed message brokers | [Transfer Family AS2 Versus Amazon MQ](#2026-08-07-transfer-family-as2-versus-amazon-mq) | [Core migration note](revision-notes/core/10-migration-modernization.md#transfer-family-and-as2) · [Mock 005 review](sap-c02-full-mock-005-review-20260807.md) | [Transfer Family AS2](https://docs.aws.amazon.com/transfer/latest/userguide/send-as2-messages.html) · [Amazon MQ architecture](https://docs.aws.amazon.com/amazon-mq/latest/developer-guide/amazon-mq-broker-architecture.html) |
| Private CloudFront origin plus dynamic origin selection | [OAC Plus Dynamic Origin Selection](#2026-08-12-oac-plus-dynamic-origin-selection) | [Mock 006 review](sap-c02-full-mock-006-review-20260812.md#miss-1---question-9-private-origin-plus-dynamic-origin-selection) | [CloudFront OAC](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/private-content-restricting-access-to-s3.html) · [Lambda@Edge request events](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/lambda-edge-event-request-response.html) |
| Multi-AZ shared NFS semantics | [Regional EFS Versus Periodic EBS Copies](#2026-08-12-regional-efs-versus-periodic-ebs-copies) | [Mock 006 review](sap-c02-full-mock-006-review-20260812.md#miss-2---question-17-shared-filesystem-versus-periodic-copies) | [EFS features](https://docs.aws.amazon.com/efs/latest/ug/features.html) · [EFS mount targets](https://docs.aws.amazon.com/efs/latest/ug/accessing-fs.html) |
| Batch custom AMI and Spot composition | [Batch EC2 Custom AMI Plus Spot](#2026-08-12-batch-ec2-custom-ami-plus-spot) | [Mock 006 review](sap-c02-full-mock-006-review-20260812.md#miss-3---question-29-batch-custom-ami-plus-spot-capacity) | [Batch managed EC2 compute environments](https://docs.aws.amazon.com/batch/latest/userguide/create-compute-environment-managed-ec2.html) · [Batch custom AMIs](https://docs.aws.amazon.com/batch/latest/userguide/create-batch-ami.html) |
| Regional failover orchestration completeness | [Warm-Standby Routing Completeness](#2026-08-12-warm-standby-routing-completeness) | [Mock 006 review](sap-c02-full-mock-006-review-20260812.md#miss-4---question-45-warm-standby-failover-completeness) | [Cross-Region failover guidance](https://docs.aws.amazon.com/solutions/cross-region-failover-and-graceful-failback-on-aws/) |

## Review Status

| Item | Status |
|---|---|
| Current through practice block | Full Mock 007 completed 2026-08-15 at 75/75 in 142 minutes; 48/48 single-response, 27/27 exact-match multiple-response, and 7/7 uncertain. All four Mock 006 remediation targets transferred; no new wrong-answer entry or focused retest is required. Full Mock 008 is next |
| Source-backed carry-forward review | Completed 2026-07-09; Review Cycle 1 completed 2026-07-15 and reviewed-and-corrected Review Cycle 2 completed 2026-07-18 |
| First review cycle evidenced | Completed 2026-07-15: 4/4 blind recall |
| Second review cycle evidenced | Completed 2026-07-18: corrected final submission scored 4/4; initial saved drafts contained material gaps, so this is not recorded as an unchanged clean blind pass |
| Controlling tracker reference | `docs/planning/sap-c02-readiness-tracker.md` |

## Practice Score Summary

| Block | Score | Percentage | Notes |
|---|---:|---:|---|
| Block 002 | 18 / 20 | 90% | Missed hybrid DNS and rehost vs refactor. |
| Block 003 | 24 / 25 | 96% | One multi-select option-selection miss on hybrid networking. |
| Block 004 | 25 / 25 | 100% | Clean pass. |
| Block 005 | 25 / 25 | 100% | Clean pass. Strong multi-select discipline. |
| Block 006 | 24 / 25 | 96% | Missed Kinesis vs SQS FIFO streaming distinction. |
| Block 007 | 25 / 25 | 100% | Clean pass. Stronger coverage of Domain 2 and Domain 3 topics. |
| Mixed practice Block 2 | 25 / 25 | 100% | Explicitly submitted 2026-07-18; all 10 multi-select questions correct; timing and question text not supplied; separate from Review Cycle 2. |
| Timed mixed diagnostic - 30 questions | 29 / 30 | 96.7% | Explicitly submitted 2026-07-21 in 67/72 minutes; all 12 multiple-response questions correct; missed ECS blue/green deployment versus Patch Manager. |
| Full mock 001 - 75 questions | 73 / 75 | 97.3% | Completed 2026-07-23 in 133/180 minutes; 48/48 single choice and 25/27 multiple response; missed SCP scope versus permissions boundaries and DynamoDB write-sharding read fan-out. |
| SCP versus permissions boundary retest | 6 / 6 | 100% | Completed 2026-07-24 in 18 minutes; all three uncertain responses were correct and the original trap did not recur; attempted before the preferred spacing date, so treat as immediate remediation rather than spaced-recall proof. |
| Non-relational database diagnostic | 15 / 18 | 83.3% | Completed 2026-07-25 in 40 minutes; 9/9 single-choice and 6/9 multiple-response; misses on questions 4, 8, and 17; six-question spaced retest due no earlier than 2026-07-28. |
| Resilience/DR spaced retest | 12 / 12 | 100% | Completed 2026-07-27 in 16 minutes; all seven exact-match multiple-response questions correct. |
| Non-relational database spaced retest | 6 / 6 | 100% | Completed 2026-07-28 in 15 minutes; focused remediation closed, independent-mock transfer remains. |
| Full mock 002 - 75 questions | 71 / 75 | 94.7% | Completed 2026-07-29 in 139/180 minutes; 45/48 single-response and 26/27 exact-match multiple-response; four narrow misses logged below. |
| Full mock 002 spaced retest | 8 / 8 | 100% | Completed 2026-08-01 in 17 minutes; 4/4 single-response and 4/4 exact-match multiple-response; Questions 4 and 7 uncertain and correct; focused remediation complete. |
| Full mock 003 - 75 questions | 75 / 75 | 100% | Completed 2026-08-03 in 106/180 minutes; 48/48 single-response, 27/27 exact-match multiple-response, and 12/12 uncertain answers correct; no new wrong-answer entry. |
| Full mock 004 - 75 questions | 70 / 75 | 93.3% | Completed 2026-08-05 in 113/180 minutes; 45/48 single-response, 25/27 exact-match multiple-response, and 13/15 uncertain answers correct; Questions 13, 14, 31, 47, and 75 missed. |
| Full mock 004 spaced retest | 7 / 8 | 87.5% | Completed 2026-08-07 in 22 minutes; 4/4 single-response, 3/4 exact-match multiple-response, and both uncertain answers correct; Question 4 exposed a genuine Lambda@Edge versus separate write-continuity misconception. |
| Full mock 005 - 75 questions | 73 / 75 | 97.3% | Completed 2026-08-07 in 108/180 minutes; 46/48 single-response, 27/27 exact-match multiple-response, and 14/16 uncertain answers correct; Questions 47 and 56 missed. |
| Full mock 006 - 75 questions | 71 / 75 | 94.7% | Completed 2026-08-12 in 190 wall-clock minutes with approximately 10 learner-reported interruption minutes near the end; 47/48 single-response, 24/27 exact-match multiple-response, 11/11 uncertain, and every domain above 80%; Questions 9, 17, 29, and 45 missed. |
| Full mock 007 - 75 questions | 75 / 75 | 100% | Completed 2026-08-15 in 142/180 minutes; 48/48 single-response, 27/27 exact-match multiple-response, 7/7 uncertain, and every domain at 100%; all four Mock 006 remediation targets transferred. |

## Artifact Evidence

| Artifact | Scope | Repository Handling |
|---|---|---|
| `sap-c02-completed-exercises-003-to-006.zip` | Completed review logs for Blocks 003 through 006 | Binary archive retained outside the public repo; sanitized manifest recorded at `docs/exam-prep/artifacts/sap-c02-completed-exercises-003-to-006-manifest.md`. |
| `sap-c02-mixed-diagnostic-30q-20260720.md` and `sap-c02-mixed-diagnostic-30q-review-20260721.md` | Timed 30-question submission and answer-bearing assessment | Retained separately so the frozen learner answers precede the review; 29/30 in 67 minutes. |
| `sap-c02-non-relational-databases-diagnostic-18q-20260724.md` and `sap-c02-non-relational-databases-diagnostic-review-20260725.md` | Closed-book database submission and answer-bearing assessment | Frozen answers retained separately from the review; 15/18 in 40 minutes; exact-match misses limited to questions 4, 8, and 17. |
| `sap-c02-full-mock-002-75q-20260728.md` and `sap-c02-full-mock-002-review-20260729.md` | Full timed mock 002 submission and answer-bearing assessment | Frozen answers retained before independent exact-match marking; 71/75 in 139 minutes; four narrow misses logged below. |
| `sap-c02-full-mock-002-spaced-retest-8q-20260801.md` and `sap-c02-full-mock-002-spaced-retest-review-20260801.md` | Fresh focused retest and answer-bearing assessment | Frozen submission scored 8/8 in 17 minutes; all four Mock 002 gaps recalled correctly and subsequently transferred in Full Mock 003. |
| `sap-c02-full-mock-003-75q-20260801.md` and `sap-c02-full-mock-003-review-20260803.md` | Full timed Mock 003 submission and answer-bearing assessment | Frozen submission scored 75/75 in 106 minutes; all four domains, all 27 exact-match multiple-response items, and all 12 uncertain answers were correct; no new miss. |
| `sap-c02-full-mock-004-75q-20260804.md` and `sap-c02-full-mock-004-review-20260805.md` | Full timed Mock 004 submission and answer-bearing assessment | Frozen submission scored 70/75 in 113 minutes; five misses reduced to four themes, including an ECS blue/green recurrence, with source-note remediation and a later short retest required. |
| `sap-c02-full-mock-004-spaced-retest-8q-20260807.md` and `sap-c02-full-mock-004-spaced-retest-review-20260807.md` | Fresh focused retest and answer-bearing assessment | Frozen submission scored 7/8 in 22 minutes; three themes passed, while the CloudFront/Lambda@Edge write-continuity distinction remains open for independent-mock transfer. |
| `sap-c02-full-mock-005-75q-20260807.md` and `sap-c02-full-mock-005-review-20260807.md` | Full timed Mock 005 submission and answer-bearing assessment | Frozen submission scored 73/75 in 108 minutes; all 27 multiple-response questions and both Domains 1 and 2 were correct; two narrow single-response misses were recorded. |
| `sap-c02-full-mock-006-75q-20260812.md` and `sap-c02-full-mock-006-review-20260812.md` | Complexity-calibrated full Mock 006 submission and answer-bearing assessment | Frozen submission scored 71/75; 190 wall-clock minutes included approximately 10 learner-reported interruption minutes, so pacing is qualified; all four misses were confident Domain 2 solution-composition errors. |
| `sap-c02-full-mock-007-75q-20260815.md` and `sap-c02-full-mock-007-review-20260815.md` | Complexity-calibrated full Mock 007 submission and answer-bearing assessment | Submitted response set scored 75/75 in 142 minutes; all single-response, exact-match multiple-response, uncertain, domain, and Mock 006 transfer checks passed. |

## Review Cycle 1 Checklist

Opened: 2026-07-07

Completion rule: mark this cycle complete only after each drill can be answered
from memory without looking at the correction text first.

| Drill | Review Question | Status |
|---|---|---|
| Hybrid DNS | Which AWS service pair handles private DNS forwarding between VPCs and on-premises DNS? | Correct from memory 2026-07-15 |
| Urgent migration | When the scenario says urgent data-centre exit and minimal change, which migration pattern wins first? | Correct from memory 2026-07-15 |
| Private hybrid network | Which services map to private multi-VPC hybrid routing and DNS, and why is Internet Gateway excluded? | Correct from memory 2026-07-15 |
| Replayable event ingestion | Which requirements push the answer from SQS FIFO to Kinesis Data Streams? | Correct from memory 2026-07-15 |

## Review Cycle 1 Evidence - 2026-07-15

Mode: four blind, free-response drills completed without the correction text in
the prompt. Result: **4/4 correct**.

| Drill | Learner recall evidence | Assessment |
|---|---|---|
| Hybrid DNS | Identified inbound and outbound Route 53 Resolver endpoints, Resolver rules, and Transit Gateway as transport rather than DNS forwarding. | Correct. |
| Urgent migration | Selected rehost/lift-and-shift with AWS Application Migration Service and described block-level replication. | Correct. |
| Private hybrid network | Selected Transit Gateway with Direct Connect or VPN for private routing and Resolver endpoints for DNS; excluded Internet Gateway because no public path was required. | Correct. |
| Replayable event ingestion | Selected Kinesis Data Streams for replay, high throughput, and multiple independent consumers while preserving ordered stream processing. | Correct; retain the more precise exam wording that ordering is per partition key. |

This completes recall-based Review Cycle 1 only. It is untimed, does not count
as a practice exam, and did not by itself complete the required second review
cycle.
The broader Route 53 consolidation lesson is recorded in
`docs/exam-prep/revision-notes/targeted-lessons/route-53-sap-c02-key-lessons-20260715.md`.

## Mixed Practice Block 2 Evidence - 2026-07-18

The learner explicitly submitted a separate 25-question mixed practice block.
The supplied marking result was **25/25 (100%)**, including 15/15 single-choice
and 10/10 multi-select questions. The exact submitted choices, reported
coverage, and evidence limitations are preserved in
`docs/exam-prep/sap-c02-mixed-practice-block-2-submission-20260718.md`.

The result is not timed evidence because timing was not supplied, and it cannot
be independently re-marked because the question text and answer key are not in
the repository. It creates no new wrong-answer entries. It did not complete or
supersede the targeted four-question Review Cycle 2 test.

## Review Cycle 2 Evidence - 2026-07-18

The learner explicitly submitted revised final answers to the four-question
retention test. The final answers scored **4/4 (100%)** and correctly state the
durable decision rules for hybrid DNS, rehost/MGN, private multi-VPC hybrid
routing, and replayable ordered ingestion.

The file also preserves earlier saved draft answers. Those drafts contained
material gaps: an incomplete DNS-versus-transport explanation, an MGN service
name conflation, PrivateLink selected for hybrid transport, and imprecise
partition/shard plus SQS-throughput reasoning. Because the explicit final
answers materially correct those drafts, this is recorded as a completed
review-and-correction cycle, not an unchanged clean blind pass.

No new wrong-answer themes are added; the corrections reinforce the existing
four entries. The result is untimed and does not count as full-exam or booking
evidence.

## Carry-Forward Review - 2026-07-09

Scope: this is a repository-backed review of the logged corrections and their
supporting comparison notes. It does not claim that the learner completed the
recall-based Review Cycle 1 checklist; those four drills remain pending.

| Weak area | Verified decision rule | Supporting repository evidence | Carry-forward action |
|---|---|---|---|
| Hybrid DNS | Use Route 53 Resolver inbound/outbound endpoints and forwarding rules; AWS Config does not resolve DNS. | `docs/planning/domain-2-network-access-patterns-20260621.md` | Complete the pending recall drill, then retain this pattern for the September networking matrix. |
| Urgent migration | Rehost first with AWS Application Migration Service when speed and minimal change are explicit. | This log's 2026-06-19 entry and the tracker migration weak-area register. | Revisit in the September migration decision table; do not treat one corrected question as full migration readiness. |
| Private hybrid network | Map each service to a stated need: Direct Connect Gateway and Transit Gateway for private hybrid routing, Route 53 Resolver for DNS, and no Internet Gateway unless public access is required. | `docs/planning/domain-2-network-access-patterns-20260621.md` | Complete the pending recall drill, then carry the decision pattern into the September networking artifacts. |
| Replayable event ingestion | Prefer Kinesis Data Streams for per-key ordering, replay, and independent consumers; use SQS FIFO for ordered queueing and deduplication. | This log's 2026-07-01 entry and tracker wrong-answer table. | Complete the pending recall drill and test the distinction in a future timed practice block. |

The carry-forward review confirms that the identified weak areas have durable
decision rules and a later milestone. It does not change the booking criterion:
the second review cycle and timed-practice evidence were still required at that
point. The mixed Block 2 clean pass and corrected Review Cycle 2 were later
recorded on 2026-07-18; timed-practice evidence remains required.

## Decision Rules To Drill

| Pattern | Preferred Decision Rule |
|---|---|
| Hybrid DNS between AWS and on-premises | Use Route 53 Resolver inbound/outbound endpoints and forwarding rules. |
| Urgent data-centre exit with minimal change | Rehost first with AWS Application Migration Service, then optimize. |
| Private multi-VPC hybrid connectivity | Use Direct Connect Gateway, Transit Gateway, and Route 53 Resolver when private routing and hybrid DNS are stated. |
| Replayable high-throughput event ingestion with multiple consumers | Use Kinesis Data Streams with partition keys and independent consumers; use SQS FIFO for ordered queueing, not stream replay. |
| ECS application revision with ALB traffic shifting and rollback | Use an ECS blue/green deployment strategy with health/metric validation and rollback; Patch Manager patches managed nodes rather than deploying container application revisions. |
| Organization-wide preventive restriction with account-class exceptions | Apply an SCP at the appropriate OU; place differently governed security accounts in a separate OU. Use a permissions boundary only to cap permissions for a specific IAM identity. |
| Concentrated DynamoDB writes on a few entity keys | Add calculated shard suffixes to distribute writes; query all relevant shards in parallel and merge/order the results on read. Capacity mode does not repair a poor key distribution. |
| DynamoDB expiry, change processing, and recovery | TTL ages items out; Streams emits item-change records; PITR restores earlier table state; a GSI only supplies another query access path. |
| Time-series history plus latest keyed state | Use Timestream for timestamp-centred history and time-window analysis; use DynamoDB for known-key, millisecond latest-state lookup. |
| Detailed physical-server process and network dependencies | Install Application Discovery Agent on the target hosts; evaluate current Agentless Collector modules separately for supported VMware inventory and agentless network collection. |

## Entries

### 2026-06-19: Hybrid DNS Private Resolution

#### Reconstructed question — Exercise Block 002, Question 10

**Not exact question — reconstructed from summary.** The original full question
and distractor set were not retained. This reconstruction preserves the same
service-selection trap and the known submitted and correct choices.

> A company connects several on-premises data centres to AWS through Direct
> Connect. Workloads in multiple VPCs must resolve names in the on-premises
> private DNS namespace. On-premises systems must also resolve records in AWS
> private hosted zones. A central security account already collects resource
> configuration data from all AWS accounts.
>
> Which solution provides the required bidirectional private DNS resolution?
>
> A. Deploy Route 53 Resolver inbound and outbound endpoints and configure the
> required forwarding rules and on-premises conditional forwarders.<br>
> B. Associate every VPC with the same private hosted zone and rely on Direct
> Connect to forward DNS queries automatically.<br>
> C. Configure an organization AWS Config aggregator in the security account.<br>
> D. Attach all VPCs to a transit gateway and enable DNS support on the VPC
> attachments.
>
> **Submitted:** C<br>
> **Correct:** A

```text
Question theme: AWS and on-premises private name resolution
SAP-C02 domain: Domain 1 / networking
My answer pattern: Treated AWS Config aggregation as if it solved DNS forwarding.
Correct answer pattern: Route 53 Resolver inbound/outbound endpoints with forwarding rules.
Why correct: Resolver endpoints bridge DNS queries between VPCs and on-premises DNS servers while keeping private names resolvable.
Why my answer was wrong: AWS Config aggregates resource configuration and compliance state; it does not forward or resolve DNS queries.
Exam trap: A governance or inventory service appears plausible because the scenario says multi-account or hybrid, but the functional requirement is DNS resolution.
Service comparison: Route 53 Resolver vs AWS Config.
Action: Drill hybrid DNS scenarios until the resolver-endpoint wording is automatic.
Review status: Review Cycle 1 correct from memory on 2026-07-15; Review Cycle 2 corrected final answer accepted on 2026-07-18, with the initial draft gap preserved.
```

References: [local Route 53 lesson](revision-notes/targeted-lessons/route-53-sap-c02-key-lessons-20260715.md) ·
[AWS Resolver hybrid forwarding](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/resolver-overview-DSN-queries-to-vpc.html)

### 2026-06-19: Urgent Data-Centre Exit

#### Reconstructed question — Exercise Block 002, Question 14

**Not exact question — reconstructed from summary.** The original full question
and distractor set were not retained. This reconstruction preserves the same
constraint-versus-modernization trap and the known submitted and correct
choices.

> A company's data-centre contract expires in six months. A legacy application
> runs on supported Linux and Windows virtual machines. The company must move
> the application to AWS before the deadline with minimal code and operating
> system changes. The business accepts modernization after the migration is
> complete.
>
> Which migration strategy and AWS service best meet the immediate requirement?
>
> A. Refactor the application into microservices running on Amazon EKS before
> migrating production traffic.<br>
> B. Replatform the application onto AWS Lambda and Amazon DynamoDB during the
> migration.<br>
> C. Rehost the servers by using AWS Application Migration Service, then
> optimize the application after cutover.<br>
> D. Retain the application on premises and expose it to AWS through Direct
> Connect until a full redesign is complete.
>
> **Submitted:** A<br>
> **Correct:** C

```text
Question theme: Migration strategy with urgent exit and minimal application change
SAP-C02 domain: Domain 4
My answer pattern: Chose the attractive long-term refactor answer.
Correct answer pattern: Rehost first with AWS Application Migration Service, then optimize after migration.
Why correct: The scenario prioritizes speed, low change, and data-centre exit risk reduction.
Why my answer was wrong: Refactoring may be the better target architecture later, but it violates the immediate time and change constraints.
Exam trap: Prefer the most modern architecture instead of the answer that satisfies the stated constraint first.
Service comparison: AWS Application Migration Service rehost vs refactor/replatform options.
Action: For migration questions, identify time pressure, change tolerance, and cutover risk before choosing the pattern.
Review status: Review Cycle 1 correct from memory on 2026-07-15; Review Cycle 2 corrected final answer accepted on 2026-07-18, with the initial service-name and sequencing gaps preserved.
```

References: [local migration lesson](revision-notes/targeted-lessons/aws-migration-discovery-transfer-tracking-sap-c02-key-lessons-20260728.md) ·
[AWS Application Migration Service rehost pattern](https://docs.aws.amazon.com/prescriptive-guidance/latest/migration-database-rehost-tools/mgn.html)

### 2026-07-01: Private Hybrid Network Architecture

#### Reconstructed question — Exercise Block 003, Question 13

**Not exact question — reconstructed from summary.** The original full question
was not retained. This reconstruction preserves the same unnecessary-component
trap and the known submitted and correct answer sets.

> A company has two on-premises data centres connected to AWS by Direct
> Connect and dozens of VPCs spread across several AWS accounts. The company
> requires private, centrally managed routing between the data centres and the
> VPCs. AWS workloads and on-premises applications must also resolve each
> other's private DNS names. Public internet connectivity is not required.
>
> Which THREE components should the company use?
>
> A. An AWS Direct Connect gateway<br>
> B. An AWS Transit Gateway shared with the application accounts<br>
> C. An internet gateway in each application VPC<br>
> D. Route 53 Resolver inbound and outbound endpoints with forwarding rules<br>
> E. AWS PrivateLink endpoints for full IP routing between every VPC and the
> data centres
>
> **Submitted:** A, B, C<br>
> **Correct:** A, B, D

```text
Question theme: Private connectivity, many VPCs, centralized routing, and hybrid DNS
SAP-C02 domain: Domain 1 / networking
My answer pattern: Added an Internet Gateway despite private connectivity being required.
Correct answer pattern: Direct Connect Gateway plus Transit Gateway plus Route 53 Resolver inbound/outbound endpoints.
Why correct: DX Gateway and Transit Gateway support scalable private hybrid connectivity, while Resolver endpoints handle hybrid DNS.
Why my answer was wrong: Internet Gateway supports public internet routing and was not required by the private-routing scenario.
Exam trap: Adding a familiar network component that is not mapped to a stated requirement.
Service comparison: Internet Gateway vs Direct Connect Gateway / Transit Gateway / Route 53 Resolver.
Action: For multi-select networking questions, require every selected service to map to an explicit requirement.
Review status: Review Cycle 1 correct from memory on 2026-07-15; Review Cycle 2 corrected final answer accepted on 2026-07-18, with the initial PrivateLink transport error preserved.
```

References: [local Networking lesson](revision-notes/targeted-lessons/aws-networking-sap-c02-key-lessons-20260717.md) ·
[AWS Direct Connect gateways](https://docs.aws.amazon.com/directconnect/latest/UserGuide/direct-connect-gateways.html) ·
[AWS Resolver hybrid forwarding](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/resolver-overview-DSN-queries-to-vpc.html)

### 2026-07-01: Replayable Ordered Event Ingestion

#### Reconstructed question — Exercise Block 006, Question 5

**Not exact question — reconstructed from summary.** The original full question
was not retained. This reconstruction preserves the same queue-versus-stream
and absolute-throughput trap and the known submitted and correct answer sets.

> A digital-commerce platform produces millions of clickstream events per
> hour. Events for the same customer must be processed in order, while events
> for different customers can be processed in parallel. Fraud detection,
> personalization, and analytics applications must each consume every event
> independently. Consumers must be able to replay retained events after an
> outage.
>
> Which THREE design choices meet these requirements?
>
> A. Ingest the events into Amazon Kinesis Data Streams.<br>
> B. Use the customer ID as the Kinesis partition key.<br>
> C. Put all events on one Amazon SQS FIFO queue because it provides unlimited
> throughput and allows every consumer to receive every message independently.<br>
> D. Register independent Kinesis consumers and use enhanced fan-out where
> dedicated per-consumer throughput is required.<br>
> E. Send the events directly to one Lambda function that invokes every
> downstream application synchronously.
>
> **Submitted:** A, B, C<br>
> **Correct:** A, B, D

```text
Question theme: High-throughput replayable ingestion with per-customer ordering and multiple consumers
SAP-C02 domain: Domain 2 / Domain 3
My answer pattern: Chose SQS FIFO because of ordering language.
Correct answer pattern: Kinesis Data Streams with customer-ID partition keys and independent consumers or enhanced fan-out.
Why correct: Kinesis supports ordered records per partition key, replay from stream retention, and multiple independent consumers.
Why my answer was wrong: SQS FIFO is an ordered queueing and deduplication pattern, not the best fit for replayable stream processing with multiple consumers.
Exam trap: Latching onto "ordering" and ignoring replay plus multiple-consumer requirements.
Service comparison: Kinesis Data Streams vs SQS FIFO.
Action: Split event questions into ordering, replay, throughput, and consumer model before selecting the service.
Review status: Review Cycle 1 correct from memory on 2026-07-15; Review Cycle 2 corrected final answer accepted on 2026-07-18, with the initial partition/shard and SQS reasoning gaps preserved.
```

References: [local Review Cycle 2 explanation](wrong-answer-review-cycle-2-blind-attempt-20260715.md#final-assessment) ·
[AWS Kinesis concepts](https://docs.aws.amazon.com/streams/latest/dev/key-concepts.html) ·
[AWS enhanced fan-out consumers](https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html)

### 2026-07-21: ECS Blue/Green Deployment Versus Patch Manager

#### Original question — Timed Mixed Diagnostic, Question 2

> A company runs an Amazon ECS service behind an Application Load Balancer. A
> new application version must be introduced without interrupting current
> users. If health checks or business metrics fail, traffic must automatically
> return to the previous version.
>
> Which deployment strategy best meets the requirements?
>
> A. Replace every task in place by using an all-at-once deployment.<br>
> B. Use an AWS CodeDeploy blue/green deployment with test and production
> listeners plus automatic rollback alarms.<br>
> C. Create a new ECS cluster manually and change DNS after deleting the old
> cluster.<br>
> D. Use AWS Systems Manager Patch Manager to install the application version
> on the containers.

#### Original question — Full Mock 004, Question 31

> An ECS service uses an Application Load Balancer. The company wants
> controlled blue/green releases, a test endpoint for validation, and automatic
> rollback if the new task set causes an alarm.<br><br>
> Which TWO components should be used?<br><br>
> A. AWS CodeDeploy with ECS blue/green deployment<br>
> B. Systems Manager Patch Manager as the application deployment controller<br>
> C. An S3 lifecycle rule to shift traffic<br>
> D. Route 53 multivalue answers without a deployment controller<br>
> E. Production and test listeners or listener rules associated with the
> deployment

```text
Question theme: ECS application deployment with side-by-side validation, ALB traffic shifting, and automatic rollback
SAP-C02 domain: Domain 2 / deployment strategy
My answer pattern: Initially selected AWS Systems Manager Patch Manager. In Full Mock 004, correctly selected CodeDeploy but omitted the production/test listener component in a Choose TWO response.
Correct answer pattern: Use an ECS blue/green deployment strategy with test and production listeners plus health or metric alarms that can trigger rollback.
Why correct: Blue/green deployment keeps the original task set available while the replacement is validated, shifts load-balancer traffic deliberately, and can restore traffic to the original revision after failure or an alarm.
Why my answer was wrong: Patch Manager does not own ECS task-set replacement, ALB traffic shifting, application rollout validation, or revision rollback. CodeDeploy alone was also an incomplete exact-match answer when the question separately required the listener configuration.
Exam trap: Selecting the deployment controller but omitting a second required architecture component in a multi-response question.
Service comparison: ECS blue/green deployment / CodeDeploy versus AWS Systems Manager Patch Manager.
Action: Complete a fresh exact-match retest no earlier than 2026-08-07; state both the deployment controller and the ALB listener/rule configuration.
Review status: Focused spaced retest Questions 5 and 6 were correct on 2026-08-07, including the complete three-part configuration response. Focused remediation passed; continue ordinary independent-mock monitoring.
```

References: [local diagnostic review](sap-c02-mixed-diagnostic-30q-review-20260721.md) ·
[local Full Mock 004 review](sap-c02-full-mock-004-review-20260805.md) ·
[local Mock 004 spaced-retest review](sap-c02-full-mock-004-spaced-retest-review-20260807.md) ·
[AWS ECS blue/green deployments](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/deployment-type-bluegreen.html) ·
[AWS Systems Manager Patch Manager](https://docs.aws.amazon.com/systems-manager/latest/userguide/patch-manager.html)

### 2026-07-23: SCP Versus IAM Permissions Boundary

#### Original question — Full Mock 001, Question 1

> A global manufacturing company has 140 AWS accounts in AWS Organizations.
> Product teams can deploy workloads only in `eu-west-1` and `eu-central-1`.
> The security team must retain the ability to use global AWS services and to
> operate centralized security tooling in any required Region. Developers
> currently have administrator IAM permissions in their workload accounts.
>
> Which TWO actions will meet these requirements with the LEAST ongoing
> administrative effort?
>
> A. Create AWS Config rules in every account to detect resources outside the
> approved Regions and automatically delete them.<br>
> B. Attach an IAM permissions boundary to every developer role that denies all
> actions outside the two approved Regions.<br>
> C. Attach an SCP to the workload OUs that denies regional service actions
> outside the approved Regions, using `aws:RequestedRegion`, while excluding
> required global services.<br>
> D. Place security tooling accounts in a separate OU that is not subject to
> the workload Region-restriction SCP.<br>
> E. Disable all unapproved Regions individually in every member account and in
> the management account.

```text
Question theme: Organization-wide Region restriction for workload accounts with different requirements for security accounts
SAP-C02 domain: Domain 1 - Design Solutions for Organizational Complexity
Question number: 1
My answer: B, C
Correct answer: C, D
My answer pattern: Used an IAM identity-level control where the scenario required an organization-wide preventive restriction.
Correct answer pattern: Apply a Region-restriction SCP to the workload OU and keep security accounts in a separate OU that does not inherit that restriction.
Why correct: An SCP defines the maximum permissions available in affected member accounts and can enforce the restriction across an OU. Separate OU placement preserves a different policy-inheritance boundary for security accounts.
Why my answer was wrong: An IAM permissions boundary limits one IAM user or role; it does not impose the required account-wide or OU-wide preventive control.
Exam trap: Selecting an identity-scoped maximum-permissions control for an organization-scoped requirement.
Service comparison: AWS Organizations SCP versus IAM permissions boundary.
Error category: Scope or policy-evaluation error; multi-select exact-match error.
Action: Continue recurrence monitoring in independent full mocks; retain the role-ARN versus role-session-ARN resource-policy boundary distinction.
Review status: Focused remediation passed 6/6 on 2026-07-24 in 18 minutes, with all three learner-marked uncertain items correct. Full Mock 003 Question 42 correctly retained the SCP guardrail and explicit-deny boundary; continue ordinary recurrence monitoring.
```

References: [local SCP/boundary model](sap-c02-hidden-gap-model-review-20260725.md#model-1---scp-versus-iam-permissions-boundary) ·
[local retest review](sap-c02-scp-permissions-boundary-closed-book-retest-review-20260724.md) ·
[AWS Organizations SCPs](https://docs.aws.amazon.com/organizations/latest/userguide/orgs_manage_policies_scps.html) ·
[AWS IAM permissions boundaries](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_boundaries.html)

### 2026-07-23: DynamoDB Write Sharding and Read Fan-Out

#### Original question — Full Mock 001, Question 13

> A company operates a DynamoDB table that stores telemetry by `deviceId` as
> the partition key. A small number of industrial gateways write 80% of all
> records, causing throttling even though the table uses on-demand capacity.
> The application must continue to support efficient retrieval of all telemetry
> for one gateway and one time range.
>
> Which TWO changes would most directly address the issue?
>
> A. Query the calculated shards in parallel and merge the time-ordered results
> in the application.<br>
> B. Replace on-demand mode with provisioned capacity without changing the key
> design.<br>
> C. Enable DynamoDB Streams and archive old records to Amazon S3.<br>
> D. Add a calculated write-shard suffix to the partition key for high-volume
> gateways.<br>
> E. Increase the table's maximum read capacity only.

#### Recurrence question — Non-Relational Database Diagnostic, Question 4

> A team changes a hot key from `market#GB` to `market#GB#0` through
> `market#GB#15` using a random suffix. The application must return all events
> for `market#GB` in timestamp order.
>
> Which two consequences must the read design handle? Choose TWO.
>
> A. Query the relevant shard keys and merge the result sets.<br>
> B. Use a strongly consistent read against a GSI to reconstruct global order.<br>
> C. Sort or otherwise reconcile results across shards.<br>
> D. Replace all queries with one table Scan using a filter expression.<br>
> E. Expect DAX to identify the correct shard automatically.

```text
Question theme: Concentrated writes on a small number of very high-volume DynamoDB partition keys
SAP-C02 domain: Domain 3 - Continuous Improvement for Existing Solutions
Question number: 13
My answer: B, D
Correct answer: A, D
My answer pattern: Treated capacity mode as the repair for poor key distribution and omitted the read consequence of write sharding.
Correct answer pattern: Add a calculated shard suffix to distribute writes across partition keys, then query every relevant shard in parallel and merge and order the results.
Why correct: Shard suffixes spread concentrated writes across partitions, but the original entity's records then span several partition keys and require fan-out reads.
Why my answer was wrong: Changing capacity mode can alter capacity management, but it does not repair a hot-key access pattern or remove the need to distribute writes.
Exam trap: Focusing on provisioned versus on-demand capacity while ignoring partition-key distribution and the resulting read-path trade-off.
Service comparison: DynamoDB key-design write sharding versus capacity-mode selection.
Error category: Missed architectural consequence; multi-select exact-match error.
Action: Reconstruct the complete read path—query every relevant shard, merge the results, then establish cross-shard order—and retain that GSI reads are eventually consistent only.
Review status: Focused spaced retest passed 6/6 on 2026-07-28 in 15 minutes. Questions 1 and 2 correctly recalled fan-out, merge, cross-shard ordering, and GSI consistency despite learner-marked uncertainty. Full Mock 003 Questions 41 and 63 correctly diagnosed and repaired a hot GSI key; continue ordinary recurrence monitoring.
```

References: [local write-sharding model](sap-c02-hidden-gap-model-review-20260725.md#model-2---dynamodb-write-sharding-and-fan-out-reads) ·
[AWS DynamoDB write sharding](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-sharding.html)

### 2026-07-25: DynamoDB Lifecycle and Recovery Feature Mapping

#### Original question — Non-Relational Database Diagnostic, Question 8

> A compliance team requires old session items to expire automatically, an
> asynchronous processor to react to item changes, and the ability to restore
> the table to an earlier point after accidental writes.
>
> Which three capabilities address the requirements? Choose THREE.
>
> A. Time to Live (TTL)<br>
> B. DynamoDB Streams<br>
> C. Point-in-time recovery (PITR)<br>
> D. A global secondary index<br>
> E. DynamoDB Accelerator<br>
> F. A filter expression

```text
Question theme: Automatic expiry, asynchronous item-change processing, and restoration after accidental writes
SAP-C02 domain: Domain 2 / Domain 3
Question number: 8
My answer: A, C, D
Correct answer: A, B, C
My answer pattern: Correctly chose TTL and PITR but selected a GSI rather than DynamoDB Streams for reacting to item changes.
Correct answer pattern: Use TTL for asynchronous expiry, DynamoDB Streams for item-change events, and PITR for restoration to an earlier table state.
Why correct: Each capability maps independently to one requirement: data ageing, change capture, and recovery.
Why my answer was wrong: A GSI creates another query access path; it does not publish insert, update, or delete events to a consumer.
Exam trap: Treating every DynamoDB auxiliary feature as an indexing option instead of mapping the requested function precisely.
Service comparison: DynamoDB TTL versus Streams versus PITR versus GSI.
Error category: Feature-mapping knowledge gap; multiple-response requirement-decomposition error.
Action: Reproduce the four-part mapping from memory and monitor recurrence in independent mocks.
Review status: Focused spaced retest questions 3, 4, and 6 were correct on 2026-07-28. The TTL, Streams, PITR, GSI, and primary-key roles were separated correctly; focused remediation is complete.
```

References: [local database lesson](revision-notes/targeted-lessons/aws-non-relational-databases-sap-c02-key-lessons-20260724.md) ·
[local diagnostic review](sap-c02-non-relational-databases-diagnostic-review-20260725.md) ·
[AWS DynamoDB TTL](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html) ·
[AWS DynamoDB Streams](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html) ·
[AWS PITR restore](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/pointintimerecovery_restores.html) ·
[AWS global secondary indexes](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html)

### 2026-07-25: Time-Series History Versus Latest-State Lookup

#### Original question — Non-Relational Database Diagnostic, Question 17

> An industrial platform ingests timestamped sensor measurements. It needs
> high-rate ingestion, queries over time windows, and different handling for
> recent versus historical measurements. A separate operational service needs
> millisecond lookup of the latest state by device ID.
>
> Which two-service design best separates the access patterns? Choose TWO.
>
> A. Use Amazon Timestream for time-window measurement analysis.<br>
> B. Use DynamoDB for latest-state lookup by device ID.<br>
> C. Use Amazon Neptune for all measurements because time is a relationship.<br>
> D. Use ElastiCache as the only durable history store.<br>
> E. Use DocumentDB only because the readings can be represented as JSON.

```text
Question theme: Historical time-window telemetry analysis plus millisecond latest-state lookup by known device ID
SAP-C02 domain: Domain 2 / Domain 3
Question number: 17
My answer: A
Correct answer: A, B
My answer pattern: Correctly selected Timestream for historical analysis but omitted DynamoDB for the separately stated latest-state access pattern.
Correct answer pattern: Use Timestream for timestamp-centred history and time-window queries, and DynamoDB for known-key latest-state retrieval.
Why correct: The two stores serve distinct access patterns rather than forcing operational and analytical requests into one model.
Why my answer was wrong: The answer satisfied only the historical-analysis clause; a Choose TWO response also required a service for the operational latest-state clause.
Exam trap: Stopping after finding one valid service in a multi-requirement, multiple-response question.
Service comparison: Amazon Timestream versus DynamoDB in a purpose-built multi-store design.
Error category: Scenario-decomposition and answer-completeness error; exact-match multiple-response error.
Action: Count required selections and map one selected component to every explicit requirement before submission; monitor recurrence in independent mocks.
Review status: Focused spaced retest questions 5 and 6 were correct on 2026-07-28. The learner mapped both history and latest-state clauses and selected the required number of options; focused remediation is complete.
```

References: [local database lesson](revision-notes/targeted-lessons/aws-non-relational-databases-sap-c02-key-lessons-20260724.md) ·
[local diagnostic review](sap-c02-non-relational-databases-diagnostic-review-20260725.md#17---time-series-selection) ·
[AWS Timestream data modelling](https://docs.aws.amazon.com/timestream/latest/developerguide/data-modeling.html) ·
[AWS DynamoDB core components](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.CoreComponents.html)

### 2026-07-28: Application Discovery Agent Versus Agentless Collection

#### Available question excerpt from the supplied external assessment

Only the following question text and two evaluated options were supplied; no
complete distractor set was retained:

> An enterprise plans to migrate a complex environment containing hundreds of
> interconnected on-premises physical servers to AWS. The project management
> team needs to establish a phased migration wave plan based on actual network
> dependency mapping between applications. Which AWS discovery mechanism
> provides this visual mapping capability?
>
> A. Deploy the AWS Application Discovery Service Agentless Discovery Connector
> tool.<br>
> B. Install the agent-based AWS Application Discovery Service on all target
> servers.

```text
Question theme: Migration-wave planning for hundreds of interconnected on-premises physical servers using network-dependency evidence
SAP-C02 domain: Domain 4 - Accelerate Workload Migration and Modernization
My answer: Agentless Discovery Connector
Correct answer pattern: Install AWS Application Discovery Agent on the physical target servers when detailed process and TCP connection evidence is required.
Why correct: Discovery Agent supports physical servers and VMs and captures system configuration, performance, running processes, and TCP network connections for dependency analysis.
Why my answer was wrong: The selected legacy agentless connector wording did not fit detailed host-level evidence across physical servers.
Exam trap: Choosing lower-administration agentless collection without checking the source environment, collection module, and telemetry depth.
Service comparison: AWS Application Discovery Agent versus current Application Discovery Service Agentless Collector, with Migration Hub as the organization/tracking surface.
Precision caveat: Current Agentless Collector includes a Network Data Collection module for supported VMware-discovered servers. The vCenter module supplies inventory; the network module uses WinRM for Windows or SNMP for Linux and therefore still needs credentials and network access. Do not generalize the older answer explanation into “agentless can never map dependencies,” or claim that connection mapping comes from hypervisor APIs alone.
Action: During the Domain 4 migration matrix, compare physical versus VMware scope, process/TCP detail, deployment overhead, and Migration Hub's role. Validate transfer in the normal full-mock cadence; create a focused retest only if the distinction recurs.
Review status: New external-assessment miss supplied 2026-07-28. No complete question set, timing, or score was provided; this entry records only the demonstrated decision gap.
```

References: [local migration lesson](revision-notes/targeted-lessons/aws-migration-discovery-transfer-tracking-sap-c02-key-lessons-20260728.md) ·
[AWS Discovery Agent](https://docs.aws.amazon.com/application-discovery/latest/userguide/discovery-agent.html) ·
[AWS Agentless Collector](https://docs.aws.amazon.com/application-discovery/latest/userguide/agentless-collector.html) ·
[AWS Agentless network module](https://docs.aws.amazon.com/application-discovery/latest/userguide/agentless-collector-gs-network-data-collection.html)

### 2026-07-29: IAM Identity Center Versus Cognito

#### Original question — Full Mock 002, Question 1

> A company has 80 AWS accounts in AWS Organizations. Employees authenticate
> through a corporate identity provider. Platform administrators need to assign
> different access levels to groups across selected accounts without creating
> IAM users or maintaining a separate federation role manually in every
> account.
>
> Which solution meets these requirements with the LEAST operational overhead?
>
> A. Create IAM users in the management account and allow them to assume roles
> in each member account.<br>
> B. Integrate the identity provider with IAM Identity Center, define permission
> sets, and create group-to-account assignments.<br>
> C. Create Amazon Cognito identity pools for the employees and map each pool to
> an IAM role in every account.<br>
> D. Store corporate credentials in AWS Secrets Manager and rotate them from a
> central security account.

```text
Question theme: Central workforce access to many AWS accounts through corporate groups
SAP-C02 domain: Domain 1 - Design Solutions for Organizational Complexity
Question number: Full mock 002, Question 1
My answer: C - Amazon Cognito identity pools
Correct answer: B - IAM Identity Center with permission sets and group-to-account assignments
My answer pattern: Chose an application-user federation service for a workforce multi-account access requirement.
Correct answer pattern: Connect the workforce identity source to IAM Identity Center, define permission sets, and assign groups to the required AWS accounts.
Why correct: IAM Identity Center centrally manages workforce access and provisions the corresponding account roles from permission sets.
Why my answer was wrong: Cognito identity pools issue temporary AWS credentials to application identities; they are not the standard AWS workforce account-assignment control plane.
Exam trap: Treating every federated-human identity scenario as Cognito without identifying whether the human is workforce or an application customer.
Service comparison: IAM Identity Center versus Amazon Cognito.
Error category: Service-selection and identity-pattern gap.
Action: Reproduce the workforce-versus-customer identity rule from memory and include it in the post-2026-07-31 spaced retest.
Review status: Focused spaced retest Questions 1 and 2 were correct on 2026-08-01. Full Mock 003 Question 12 independently retained the workforce permission-set and permissions-boundary pattern; continue ordinary recurrence monitoring.
```

References: [local full mock 002 review](sap-c02-full-mock-002-review-20260729.md) ·
[local spaced-retest assessment](sap-c02-full-mock-002-spaced-retest-review-20260801.md) ·
[AWS Identity Center account assignments](https://docs.aws.amazon.com/singlesignon/latest/userguide/assignusers.html) ·
[AWS Identity Center permission sets](https://docs.aws.amazon.com/singlesignon/latest/userguide/permissionsets.html) ·
[AWS Cognito identity pools](https://docs.aws.amazon.com/cognito/latest/developerguide/cognito-identity.html)

### 2026-07-29: Migration Hub Home Region Versus Data Transfer

#### Original question — Full Mock 002, Question 28

> A migration programme uses several AWS and partner tools. The programme
> office wants a single view of applications, migration waves, and status.
> Discovery data was collected in one AWS Region, but the office is viewing
> Migration Hub in a different Region and cannot see it.
>
> What should the team do?
>
> A. Copy the discovery database with AWS DataSync to every Region.<br>
> B. View and manage the migration programme in the configured Migration Hub
> home Region and connect the migration tools there.<br>
> C. Reinstall every discovery agent in the Region that hosts the target VPC.<br>
> D. Use AWS Config aggregators instead of Migration Hub.

```text
Question theme: Missing migration-discovery and tracking data when viewing the wrong AWS Region
SAP-C02 domain: Domain 4 - Accelerate Workload Migration and Modernization
Question number: Full mock 002, Question 28
My answer: A - copy the discovery database by using AWS DataSync
Correct answer: B - view and manage the programme in the configured Migration Hub home Region
My answer pattern: Chose a workload-data transfer tool to repair a service-control-plane Region mismatch.
Correct answer pattern: Use Migration Hub in its selected home Region and connect discovery and migration tools to that tracking surface.
Why correct: Migration Hub discovery and tracking data is associated with the home Region; changing it requires recollection because collected data is not migrated.
Why my answer was wrong: DataSync transfers supported files and objects. It does not copy Migration Hub's service-managed discovery database between Regions.
Exam trap: Selecting a data mover whenever the wording includes missing data, without checking whether the data belongs to a managed control plane.
Service comparison: AWS Migration Hub versus AWS DataSync.
Error category: Service-boundary and Region-scope gap.
Action: Reproduce home Region, tracking role, and data-movement boundary from memory and include it in the post-2026-07-31 spaced retest.
Review status: Focused spaced retest Questions 3 and 4 were correct on 2026-08-01. Full Mock 003 Questions 8 and 24 independently separated physical-server agents, VMware agentless inventory, MGN, and Migration Hub; continue ordinary recurrence monitoring.
```

References: [local migration lesson](revision-notes/targeted-lessons/aws-migration-discovery-transfer-tracking-sap-c02-key-lessons-20260728.md) ·
[local full mock 002 review](sap-c02-full-mock-002-review-20260729.md) ·
[local spaced-retest assessment](sap-c02-full-mock-002-spaced-retest-review-20260801.md) ·
[AWS Migration Hub home Region](https://docs.aws.amazon.com/migrationhub/latest/ug/home-region.html) ·
[AWS DataSync scope](https://docs.aws.amazon.com/datasync/latest/userguide/what-is-datasync.html)

### 2026-07-29: DAX Cluster and Client

#### Original question — Full Mock 002, Question 47

> A gaming application reads player-profile items from DynamoDB millions of
> times per second. Most reads are eventually consistent, the same items are
> requested repeatedly, and the company needs microsecond read latency with
> minimal changes to DynamoDB data access. The cache must remain available after
> one cache node fails.
>
> Which TWO actions meet the requirements?
>
> A. Request strongly consistent reads through DAX for every request.<br>
> B. Replace the DynamoDB table with ElastiCache for Memcached as the system of
> record.<br>
> C. Deploy a multi-node, Multi-AZ DAX cluster.<br>
> D. Change the application to use the DAX client for supported DynamoDB read
> operations.<br>
> E. Add a local secondary index with the same sort key.

```text
Question theme: Highly available microsecond DynamoDB read caching with minimal application change
SAP-C02 domain: Domain 3 - Continuous Improvement for Existing Solutions
Question number: Full mock 002, Question 47
My answer: C
Correct answer: C, D
My answer pattern: Selected the available DAX cluster but omitted the application request path through the DAX client.
Correct answer pattern: Deploy a multi-node DAX cluster across Availability Zones and use the DAX client for supported DynamoDB operations.
Why correct: The cluster supplies cache availability; at runtime the DAX client directs the application's DynamoDB API requests to that cluster.
Why my answer was wrong: DAX is API-compatible but not transparent. Creating the cluster alone does not redirect application calls.
Exam trap: Interpreting drop-in compatibility as zero integration and stopping after one valid choice in a Choose TWO item.
Service comparison: DynamoDB direct SDK path versus DynamoDB Accelerator client path.
Error category: Integration-boundary and exact-match completeness error.
Action: State both halves—cluster and client—then include the pattern in the post-2026-07-31 spaced retest.
Review status: Focused spaced retest Questions 5 and 6 were correct on 2026-08-01. Full Mock 003 Question 45 independently retained the DAX-client write-through request path; continue ordinary recurrence monitoring.
```

References: [local database lesson](revision-notes/targeted-lessons/aws-non-relational-databases-sap-c02-key-lessons-20260724.md) ·
[local full mock 002 review](sap-c02-full-mock-002-review-20260729.md) ·
[local spaced-retest assessment](sap-c02-full-mock-002-spaced-retest-review-20260801.md) ·
[AWS DAX request path](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DAX.concepts.html) ·
[AWS DAX cluster components](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DAX.concepts.cluster.html)

### 2026-07-29: S3 Interface Versus Gateway Endpoint

#### Original question — Full Mock 002, Question 73

> On-premises applications connected through Direct Connect must access S3
> over private IP addresses. The company cannot route this traffic through a
> NAT gateway or the public S3 endpoint. The endpoint must be reachable from
> the on-premises network.
>
> Which solution is MOST appropriate?
>
> A. A Route 53 public hosted zone that points to an S3 gateway endpoint<br>
> B. An S3 gateway endpoint only, because gateway endpoints are directly
> reachable from on-premises networks<br>
> C. An internet gateway in the on-premises data centre<br>
> D. An S3 interface VPC endpoint reachable through the private network path,
> with the required DNS and endpoint policy configuration

```text
Question theme: Private S3 access from on premises over Direct Connect
SAP-C02 domain: Domain 2 - Design for New Solutions
Question number: Full mock 002, Question 73
My answer: B - S3 gateway endpoint
Correct answer: D - S3 interface endpoint with private connectivity, DNS, and endpoint-policy configuration
My answer pattern: Applied the endpoint-first VPC default without checking where the client traffic originates.
Correct answer pattern: Use an S3 interface endpoint for private-IP access from on premises over Direct Connect or Site-to-Site VPN.
Why correct: Interface endpoints expose private IP addresses that on-premises clients can reach through the private network path.
Why my answer was wrong: Gateway endpoint routes apply to traffic originating in associated VPC route tables and cannot be extended through Direct Connect, VPN, transit gateway, or VPC peering.
Exam trap: Memorizing “S3 equals gateway endpoint” as an absolute rule instead of checking client origin and reachability.
Service comparison: S3 gateway endpoint versus S3 interface endpoint.
Error category: Hybrid-connectivity scope error.
Action: Reproduce VPC-origin versus on-premises-origin endpoint selection from memory and include it in the post-2026-07-31 spaced retest.
Review status: Focused spaced retest Questions 7 and 8 were correct on 2026-08-01. Full Mock 003 Question 71 independently separated gateway-endpoint VPC traffic from interface-endpoint on-premises access; continue ordinary recurrence monitoring.
```

References: [local Networking lesson](revision-notes/targeted-lessons/aws-networking-sap-c02-key-lessons-20260717.md) ·
[local full mock 002 review](sap-c02-full-mock-002-review-20260729.md) ·
[local spaced-retest assessment](sap-c02-full-mock-002-spaced-retest-review-20260801.md) ·
[AWS S3 gateway endpoint limits and hybrid interface path](https://docs.aws.amazon.com/vpc/latest/privatelink/vpc-endpoints-s3.html)

### 2026-08-05: Logically Air-Gapped Backup and Restore Evidence

#### Original question — Full Mock 004, Question 13

> A security team wants backups that are stored in an AWS service-owned account,
> are protected by Vault Lock in compliance mode, and can be shared to a separate
> recovery account for restore testing.<br><br>
> Which AWS Backup feature is designed for this requirement?<br><br>
> A. A logically air-gapped vault<br>
> B. A default backup vault with governance-mode Vault Lock<br>
> C. An EBS Recycle Bin retention rule<br>
> D. An S3 Glacier vault

#### Original question — Full Mock 004, Question 75

> An enterprise wants stronger ransomware recovery evidence for workloads
> protected by AWS Backup. Backups must resist administrator deletion, survive a
> source-account compromise, and be regularly demonstrated as restorable.<br><br>
> Which THREE actions best meet these requirements?<br><br>
> A. Store the only recovery point in the workload account's default vault.<br>
> B. Use AWS Backup Vault Lock in compliance mode for the protected vault.<br>
> C. Treat successful backup-job status as proof that the workload can be
> restored within its RTO.<br>
> D. Maintain supported cross-account or logically air-gapped backup copies with
> separately controlled recovery access.<br>
> E. Give workload administrators permission to delete all recovery points
> during an incident.<br>
> F. Configure AWS Backup restore testing plans and validate restored resources.

```text
Question theme: Backup immutability, administrative isolation, and restore evidence
SAP-C02 domain: Domain 3 - Continuous Improvement for Existing Solutions
Question numbers: Full Mock 004, Questions 13 and 75
My answers: Question 13 B; Question 75 A, B, and F
Correct answers: Question 13 A; Question 75 B, D, and F
My answer pattern: Recognized Vault Lock and restore testing but did not distinguish governance-mode locking from service-owned-account isolation, and retained the only copy in the compromised workload boundary.
Correct answer pattern: Use a logically air-gapped vault when the scenario explicitly requires AWS Backup service-owned-account storage, compliance-mode locking, and named-account sharing. For ransomware recovery, combine immutability, separate administrative control, and demonstrated restores.
Why correct: A logically air-gapped vault has compliance-mode Vault Lock, stores backups in an AWS Backup service-owned account, and can be shared with named accounts through AWS RAM. Restore testing creates periodic restore-job evidence and supports validation.
Why my answers were wrong: Governance-mode Vault Lock can be removed by a sufficiently privileged identity and does not provide the stated service-owned-account isolation. Keeping the only recovery point in the workload account preserves the compromise blast radius.
Exam trap: Treating every locked vault as equivalent, or treating successful backup status as proof of recoverability.
Service comparison: Standard backup vault with governance lock versus compliance lock versus logically air-gapped vault versus cross-account copy.
Error category: Backup-isolation and control-mode selection error.
Action: Reproduce the three-part ransomware model and the vault comparison from memory; validate through a fresh exact-match retest no earlier than 2026-08-07.
Review status: Focused spaced retest Questions 1 and 2 were correct on 2026-08-07; focused remediation passed and both learner-marked uncertain answers were correct.
```

References: [local Resilience/DR lesson](revision-notes/targeted-lessons/aws-resilience-dr-sap-c02-key-lessons-20260718.md#aws-backup-isolation-and-restore-evidence) ·
[local Full Mock 004 review](sap-c02-full-mock-004-review-20260805.md) ·
[AWS logically air-gapped vault](https://docs.aws.amazon.com/aws-backup/latest/devguide/logicallyairgappedvault.html) ·
[AWS Backup Vault Lock](https://docs.aws.amazon.com/aws-backup/latest/devguide/vault-lock.html) ·
[AWS Backup restore testing](https://docs.aws.amazon.com/aws-backup/latest/devguide/restore-testing.html)

### 2026-08-05: CloudFront Origin-Failover Method Boundary

#### Original question — Full Mock 004, Question 14

> A global application uses CloudFront with an origin group. Reads fail over
> successfully when the primary origin is unavailable, but `POST` requests do
> not fail over to the secondary origin.<br><br>
> What is the correct explanation?<br><br>
> A. CloudFront origin failover requires Lambda@Edge for every HTTP method.<br>
> B. CloudFront origin failover applies only to eligible `GET`, `HEAD`, and
> `OPTIONS` requests, not write methods such as `POST`.<br>
> C. The secondary origin must be in the same Availability Zone.<br>
> D. CloudFront can fail over only between two S3 website endpoints.

```text
Question theme: CloudFront origin-group failover method eligibility
SAP-C02 domain: Domain 3 - Continuous Improvement for Existing Solutions
Question number: Full Mock 004, Question 14
My answer: A - Lambda@Edge is required for every HTTP method
Correct answer: B - built-in origin failover is limited to eligible GET, HEAD, and OPTIONS requests
My answer pattern: Initially looked for an edge-compute prerequisite rather than checking the HTTP-method constraint. In the spaced retest, correctly recalled the GET/HEAD/eligible OPTIONS boundary but genuinely misunderstood Lambda@Edge as able to expand built-in origin failover to write methods.
Correct answer pattern: CloudFront origin groups can fail over GET, HEAD, and OPTIONS; write-method failover needs a separate application architecture.
Why correct: AWS documents that CloudFront sends requests to the secondary origin only for those three methods.
Why my answer was wrong: Lambda@Edge is not the absent prerequisite and does not change the built-in origin-group method boundary.
Exam trap: Extending successful read failover into an unsupported multi-Region write-failover assumption.
Service comparison: CloudFront origin-group read failover versus application-level write routing, idempotency, and data consistency.
Error category: Service capability-boundary error.
Action: Retain two separate rules: CloudFront origin-group read-method failover, and an independently designed write-continuity path with routing, idempotency, retries, and data consistency. Use Full Mock 005 or a later independent mock as the transfer check.
Review status: Independent transfer passed in Full Mock 005 Question 5 on 2026-08-07. The learner correctly rejected Lambda@Edge as a method-expansion mechanism and selected a separately engineered write-continuity path; focused remediation is closed.
```

References: [local Resilience/DR lesson](revision-notes/targeted-lessons/aws-resilience-dr-sap-c02-key-lessons-20260718.md#cloudfront-origin-failover-boundary) ·
[local Full Mock 004 review](sap-c02-full-mock-004-review-20260805.md) ·
[local Mock 004 spaced-retest review](sap-c02-full-mock-004-spaced-retest-review-20260807.md) ·
[AWS CloudFront origin failover](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/high_availability_origin_failover.html)

### 2026-08-05: DynamoDB MRSC Versus TTL

#### Original question — Full Mock 004, Question 47

> A team proposes an MRSC DynamoDB global table for sessions that require
> strongly consistent reads in multiple Regions. The same table design depends
> on DynamoDB TTL to expire sessions automatically.<br><br>
> What should the architect conclude?<br><br>
> A. The design is valid because MRSC supports TTL in every replica.<br>
> B. TTL is supported only when the MRSC table has a witness Region.<br>
> C. The design must change because TTL is not supported for MRSC global tables.<br>
> D. TTL becomes synchronous when strongly consistent reads are enabled.

```text
Question theme: DynamoDB MRSC feature restrictions
SAP-C02 domain: Domain 2 - Design for New Solutions
Question number: Full Mock 004, Question 47
My answer: B - a witness Region enables TTL
Correct answer: C - MRSC global tables do not support TTL
My answer pattern: Correctly recognized the witness topology but incorrectly treated it as a feature-enablement mechanism.
Correct answer pattern: MRSC requires an exactly-three-Region topology but remains subject to feature restrictions; TTL is supported only for MREC global tables.
Why correct: Current DynamoDB documentation explicitly limits TTL support to MREC global tables.
Why my answer was wrong: A witness participates in MRSC consistency and topology; it does not make TTL available.
Exam trap: Assuming that a valid MRSC topology removes unrelated MRSC feature restrictions.
Service comparison: MREC flexibility and asynchronous convergence versus MRSC cross-Region strong consistency, latency, topology, and feature limits.
Error category: Feature-compatibility retrieval error.
Action: Reproduce the MREC/MRSC table from memory, including TTL and transaction constraints; validate no earlier than 2026-08-07.
Review status: Focused spaced retest Questions 7 and 8 were correct on 2026-08-07; focused remediation passed.
```

References: [local Non-Relational Database lesson](revision-notes/targeted-lessons/aws-non-relational-databases-sap-c02-key-lessons-20260724.md#global-tables-mrec-versus-mrsc) ·
[local Full Mock 004 review](sap-c02-full-mock-004-review-20260805.md) ·
[AWS DynamoDB global-table security](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/globaltables-security.html)

### 2026-08-07: ARC Routing-Control Over-Selection

#### Original question — Full Mock 005, Question 47

> A global active-passive application uses Route 53 Application Recovery
> Controller routing controls. Operators need a highly available mechanism for
> changing whether Route 53 records are considered healthy during a Regional
> failover.<br><br>
> How do routing controls provide this capability?<br><br>
> A. They edit every client's DNS cache directly.<br>
> B. They use ARC cluster endpoints to change routing-control state, which is
> represented through Route 53 health checks used by the records.<br>
> C. They replace the Route 53 data plane with an Application Load Balancer.<br>
> D. They wait for the primary Region's CloudFormation stack to be deleted.

```text
Question theme: ARC routing-control mechanism and single-response discipline
SAP-C02 domain: Domain 3 - Continuous Improvement for Existing Solutions
Question number: Full Mock 005, Question 47
My answer: B and C
Correct answer: B
Why correct: ARC cluster endpoints provide a highly available data-plane path for changing routing-control state. Routing-control health checks expose that state to Route 53 failover records.
Why my answer was wrong: B already described the correct mechanism. C added a false architecture: ARC does not replace Route 53 with an Application Load Balancer. The item requested one response.
Exam trap: Adding a second attractive-sounding component to a single-response item after already selecting the complete answer.
Error category: Single-response over-selection / reading discipline.
Action: Enforce one option unless the heading explicitly says Choose TWO or Choose THREE; retain the cluster -> routing control -> health check -> Route 53 record chain.
Review status: Independent transfer passed in Full Mock 006 Question 32 on 2026-08-12; the learner marked the question uncertain and correctly selected the ARC cluster data-plane endpoint rather than the ordinary Route 53 control-plane API.
```

References: [core edge and DNS note](revision-notes/core/03-load-balancing-dns-edge.md#application-recovery-controller-routing-controls) ·
[local Mock 005 review](sap-c02-full-mock-005-review-20260807.md) ·
[AWS ARC routing control](https://docs.aws.amazon.com/r53recovery/latest/dg/routing-control.html)

### 2026-08-07: Transfer Family AS2 Versus Amazon MQ

#### Original question — Full Mock 005, Question 56

> A company must automate structured business-to-business message exchange with
> partners using the AS2 protocol, with messages stored in S3.<br><br>
> Which managed service should it use?<br><br>
> A. AWS Transfer Family AS2<br>
> B. AWS DataSync<br>
> C. AWS Application Migration Service<br>
> D. Amazon MQ for MQTT

```text
Question theme: Named partner-transfer protocol versus managed message broker
SAP-C02 domain: Domain 4 - Accelerate Workload Migration and Modernization
Question number: Full Mock 005, Question 56
My answer: D - Amazon MQ for MQTT
Correct answer: A - AWS Transfer Family AS2
Why correct: Transfer Family supports AS2 partner profiles, certificates, agreements, connectors, signed/encrypted exchange, MDNs, S3-backed files, and CloudWatch audit records.
Why my answer was wrong: Amazon MQ provides managed ActiveMQ or RabbitMQ brokers. The word message did not override the explicitly named AS2 partner-file protocol.
Exam trap: Matching on the generic word message instead of the named transfer protocol.
Service comparison: Transfer Family AS2 versus Amazon MQ versus DataSync.
Error category: Genuine service-comparison retrieval gap.
Action: Recall AS2 -> Transfer Family; ActiveMQ/RabbitMQ broker -> Amazon MQ; online storage copy -> DataSync. Validate through a later independent mock or very small spaced check without replacing Mock 006.
Review status: Independent transfer passed twice in Full Mock 006 Questions 11 and 51 on 2026-08-12; both service selection and AS2 profile/agreement composition were correct.
```

References: [core migration note](revision-notes/core/10-migration-modernization.md#transfer-family-and-as2) ·
[local Mock 005 review](sap-c02-full-mock-005-review-20260807.md) ·
[AWS Transfer Family AS2](https://docs.aws.amazon.com/transfer/latest/userguide/send-as2-messages.html) ·
[Amazon MQ architecture](https://docs.aws.amazon.com/amazon-mq/latest/developer-guide/amazon-mq-broker-architecture.html)

### 2026-08-12: OAC Plus Dynamic Origin Selection

```text
Question theme: Private S3 origin and request-dependent CloudFront origin selection
SAP-C02 domain: Domain 2 - Design for New Solutions
Question number: Full Mock 006, Question 9
My answer: C,D - path-only cache behaviors plus Lambda@Edge origin selection
Correct answer: B,D - Origin Access Control plus Lambda@Edge origin selection
Why correct: OAC and the bucket policy satisfy the private-origin requirement; origin-request logic handles the country, device, and cookie combination.
Why my answer was wrong: Path behavior did not satisfy the direct-origin security requirement and could not express the stated combined request attributes.
Error category: Rushed exact-match requirement-completeness error; not a learner-reported knowledge gap.
Confidence: Confident miss.
Action: Map each response to a distinct requirement before final submission; transfer check in Full Mock 007 on 2026-08-15.
Review status: Independent transfer passed in Full Mock 007 Question 12 on 2026-08-15; both the private-origin and dynamic-selection requirements were selected.
```

References: [local Mock 006 review](sap-c02-full-mock-006-review-20260812.md#miss-1---question-9-private-origin-plus-dynamic-origin-selection) ·
[CloudFront OAC](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/private-content-restricting-access-to-s3.html) ·
[Lambda@Edge request events](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/lambda-edge-event-request-response.html)

### 2026-08-12: Regional EFS Versus Periodic EBS Copies

```text
Question theme: Concurrent multi-AZ NFS access to one coherent namespace
SAP-C02 domain: Domain 2 - Design for New Solutions
Question number: Full Mock 006, Question 17
My answer: A - independent EBS volumes synchronized by DataSync
Correct answer: D - Regional EFS with mount targets in the required zones
Why correct: Regional EFS supplies shared NFS semantics and stores data redundantly across Availability Zones.
Why my answer was wrong: Periodic copies do not create a single concurrently writable filesystem or shared locking semantics.
Error category: Requirement-parsing error around the controlling coherent-POSIX-namespace constraint.
Confidence: Confident miss.
Action: Identify the positive access-semantic requirement before using the exclusion constraint; transfer check in Full Mock 007 on 2026-08-15.
Review status: Independent transfer passed in Full Mock 007 Question 18 on 2026-08-15; the coherent POSIX namespace correctly selected Regional EFS.
```

References: [local Mock 006 review](sap-c02-full-mock-006-review-20260812.md#miss-2---question-17-shared-filesystem-versus-periodic-copies) ·
[EFS features](https://docs.aws.amazon.com/efs/latest/ug/features.html) ·
[EFS mount targets](https://docs.aws.amazon.com/efs/latest/ug/accessing-fs.html)

### 2026-08-12: Batch EC2 Custom AMI Plus Spot

```text
Question theme: Managed batch scheduling with a custom host AMI and interruption-tolerant jobs
SAP-C02 domain: Domain 2 - Design for New Solutions
Question number: Full Mock 006, Question 29
My answer: A,B - Batch Fargate plus Batch managed EC2
Correct answer: B,E - Batch managed EC2 plus diversified Spot capacity
Why correct: The custom compute-resource AMI requires the EC2 environment, while checkpointed restartable work fits Spot.
Why my answer was wrong: Fargate does not use the required custom EC2 host AMI, and the answer omitted the stated cost mechanism.
Error category: Genuine compute-environment service-boundary knowledge gap and exact-match composition error.
Confidence: Confident miss.
Action: Recall custom host AMI -> Batch EC2; restartable queue -> Spot; transfer check in Full Mock 007 on 2026-08-15.
Review status: Independent transfer passed twice in Full Mock 007 Questions 20 and 33 on 2026-08-15; both selected Batch managed EC2 with the custom AMI and diversified Spot.
```

References: [local Mock 006 review](sap-c02-full-mock-006-review-20260812.md#miss-3---question-29-batch-custom-ami-plus-spot-capacity) ·
[Batch managed EC2 compute environments](https://docs.aws.amazon.com/batch/latest/userguide/create-compute-environment-managed-ec2.html) ·
[Batch custom AMIs](https://docs.aws.amazon.com/batch/latest/userguide/create-batch-ami.html)

### 2026-08-12: Warm-Standby Routing Completeness

```text
Question theme: End-to-end regional failover orchestration
SAP-C02 domain: Domain 2 - Design for New Solutions
Question number: Full Mock 006, Question 45
My answer: E - automate and test data, application, and capacity recovery
Correct answer: C,E - add health-based routing or routing controls to the tested recovery automation
Why correct: A recovery stack is not serving failed-over users until the traffic-movement layer is also configured and exercised.
Why my answer was wrong: The selected automation omitted the failover-routing component required by the scenario.
Error category: Rushed exact-match recovery-chain completeness error; not a learner-reported knowledge gap.
Confidence: Confident miss.
Action: Check decision -> data -> application/capacity -> traffic -> validate before final submission; transfer check in Full Mock 007 on 2026-08-15.
Review status: Independent transfer passed in Full Mock 007 Question 24 on 2026-08-15; both traffic movement and dependency-aware recovery orchestration were selected.
```

References: [local Mock 006 review](sap-c02-full-mock-006-review-20260812.md#miss-4---question-45-warm-standby-failover-completeness) ·
[Cross-Region failover guidance](https://docs.aws.amazon.com/solutions/cross-region-failover-and-graceful-failback-on-aws/)
