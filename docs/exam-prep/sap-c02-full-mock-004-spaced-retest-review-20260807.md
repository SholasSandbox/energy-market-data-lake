# SAP-C02 Full Mock 004 Spaced Retest — Independent Review

<!-- markdownlint-disable MD013 MD060 -->

**Reviewed:** 2026-08-07
**Submission:** [frozen eight-question retest](sap-c02-full-mock-004-spaced-retest-8q-20260807.md)
**Result:** **7/8 (87.5%)**
**Time:** 22 minutes

## Evidence Boundary

This is an answer-bearing assessment. The learner froze all eight answers
before marking. Exact-match scoring applies to every multiple-response item.
This focused result is remediation evidence, not a full mock and not booking
evidence.

## Marking

| Question | Submitted | Correct | Result | Theme |
|---:|---|---|---|---|
| 1 | C | C | Correct | Logically air-gapped vault |
| 2 | ACD | ACD | Correct; uncertain | Ransomware recovery controls |
| 3 | A | A | Correct | CloudFront method boundary |
| 4 | AD | AC | Incorrect | Separate read and write continuity |
| 5 | A | A | Correct | ECS blue/green validation |
| 6 | ABC | ABC | Correct; uncertain | Complete CodeDeploy configuration |
| 7 | C | C | Correct | MRSC TTL restriction |
| 8 | AB | AB | Correct | MREC/MRSC workload mapping |

| Question type | Correct | Total | Score |
|---|---:|---:|---:|
| Single response | 4 | 4 | 100% |
| Multiple response, exact match | 3 | 4 | 75% |
| Learner-marked uncertain | 2 | 2 | 100% |
| **Overall** | **7** | **8** | **87.5%** |

## Theme Assessment

| Theme | Evidence | Status |
|---|---|---|
| Backup isolation and restore evidence | Questions 1 and 2 correct | Focused remediation passed |
| CloudFront origin failover and write continuity | Question 3 correct; Question 4 submitted `AD` instead of `AC` | Narrow gap remains open |
| ECS blue/green completeness | Questions 5 and 6 correct, including the exact-match configuration set | Focused remediation passed |
| DynamoDB MREC/MRSC and TTL | Questions 7 and 8 correct | Focused remediation passed |

## Question 4 — Genuine Misunderstanding

The submitted answer correctly selected **A** but selected **D** instead of
**C**. This was not merely an option-count error. The learner understood that
CloudFront origin groups handle eligible read-method failover, but genuinely
misunderstood Lambda@Edge as a mechanism that could expand the built-in
origin-failover method set.

The correct mental model is:

```text
CloudFront origin group
    -> built-in failover for GET, HEAD, and eligible cached OPTIONS

Lambda@Edge
    -> runs code at supported CloudFront trigger points
    -> can inspect or modify requests and responses
    -> does not make built-in origin-group failover support POST, PUT, or DELETE

Write continuity
    -> separate routing and failover decision
    -> idempotency and retry controls
    -> data replication and consistency design
```

Therefore **A and C** are the exact-match answer. **D** loses because adding
Lambda@Edge does not change the documented method boundary of CloudFront
origin failover.

## Decision Rules

1. **Read failover:** CloudFront origin groups can fail over `GET`, `HEAD`, and
   eligible cached `OPTIONS` requests when the primary meets configured
   failure conditions.
2. **Write continuity:** design `POST`, `PUT`, `PATCH`, and `DELETE` continuity
   separately; do not infer safe write replay from edge routing.
3. **Lambda@Edge:** treat it as programmable request/response processing at
   CloudFront events, not as an override for an unsupported origin-failover
   method.
4. **ECS blue/green:** CodeDeploy requires the controller, two target groups,
   production listener, optional test listener when pre-traffic validation is
   required, and an AppSpec replacement-task definition.
5. **DynamoDB global tables:** MRSC supplies cross-Region strong consistency
   and zero RPO in exactly three Regions, but TTL remains an MREC-only feature.

## Evidence-Led Next Step

Do not add another immediate focused retest. Review the Question 4 mental model
once, then use Full Mock 005 or a later independent mock as the transfer check.
The three other Mock 004 themes have passed focused remediation. Full Mock 005
remains the next tracked priority and the two-full-mock cadence is unchanged.

## Official References

- [CloudFront origin failover](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/high_availability_origin_failover.html)
- [Lambda@Edge event structure](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/lambda-event-structure.html)
- [AWS Backup logically air-gapped vault](https://docs.aws.amazon.com/aws-backup/latest/devguide/logicallyairgappedvault.html)
- [ECS CodeDeploy blue/green deployments](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/deployment-type-bluegreen.html)
- [DynamoDB global-table MREC/MRSC and TTL rules](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/globaltables-security.html)
