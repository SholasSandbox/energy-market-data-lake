# SAP-C02 Full Mock 006 - Independent Review

<!-- markdownlint-disable MD013 MD060 -->

**Reviewed:** 2026-08-12
**Submission:** [frozen Full Mock 006](sap-c02-full-mock-006-75q-20260812.md)
**Result:** **71/75 (94.7%)**
**Wall-clock time:** **190 minutes**
**Interruption qualification:** approximately 10 minutes of short interruptions
near the end; estimated active time approximately 180 minutes

## Evidence boundary

The learner froze all 75 responses before marking. Multiple-response questions
were scored only for exact matches. The question set was complexity-calibrated
before the attempt and contained no answer key, domain labels, rationales, or
scoring hints.

The 190-minute wall-clock duration is exact from the supplied start and end
times. The approximately 10-minute interruption adjustment is learner-reported
and was not independently measured. Record the score as full-mock knowledge and
selection evidence, but qualify this attempt's pacing evidence rather than
claiming a clean uninterrupted sub-180-minute completion.

## Score summary

| Measure | Correct | Total | Score |
|---|---:|---:|---:|
| Single response | 47 | 48 | 97.9% |
| Multiple response, exact match | 24 | 27 | 88.9% |
| Learner-marked uncertain | 11 | 11 | 100% |
| **Overall** | **71** | **75** | **94.7%** |

All four misses were outside the learner's uncertainty list. This means the
result contains four confident decision-rule errors rather than errors already
identified by the learner during the attempt.

| SAP-C02 domain | Correct | Total | Score |
|---|---:|---:|---:|
| Domain 1 - Design Solutions for Organizational Complexity | 20 | 20 | 100% |
| Domain 2 - Design for New Solutions | 18 | 22 | 81.8% |
| Domain 3 - Continuous Improvement for Existing Solutions | 18 | 18 | 100% |
| Domain 4 - Accelerate Workload Migration and Modernization | 15 | 15 | 100% |
| **Overall** | **71** | **75** | **94.7%** |

The mock's primary-domain assignment is 20 Domain 1, 22 Domain 2, 18 Domain 3,
and 15 Domain 4 questions. Cross-domain concepts still appear within individual
scenarios; the table assigns each question to its principal exam task.

## Answer key and marking

```text
 1 A   correct      26 AE  correct      51 BE  correct
 2 BD  correct      27 C   correct      52 D   correct
 3 A   correct      28 D   correct      53 A   correct
 4 ACE correct      29 BE  incorrect    54 BE  correct
 5 B   correct      30 C   correct      55 C   correct
 6 D   correct      31 AD  correct      56 A   correct
 7 AE  correct      32 B   correct      57 AD  correct
 8 C   correct      33 C   correct      58 C   correct
 9 BD  incorrect    34 CE  correct      59 B   correct
10 B   correct      35 A   correct      60 ADF correct
11 D   correct      36 A   correct      61 D   correct
12 ACE correct      37 BE  correct      62 B   correct
13 D   correct      38 D   correct      63 CE  correct
14 A   correct      39 C   correct      64 A   correct
15 BD  correct      40 ACF correct      65 B   correct
16 A   correct      41 C   correct      66 ACF correct
17 D   incorrect    42 BD  correct      67 D   correct
18 BE  correct      43 D   correct      68 C   correct
19 B   correct      44 A   correct      69 BE  correct
20 BDF correct      45 CE  incorrect    70 C   correct
21 C   correct      46 B   correct      71 A   correct
22 A   correct      47 B   correct      72 BD  correct
23 CE  correct      48 AD  correct      73 B   correct
24 B   correct      49 D   correct      74 C   correct
25 D   correct      50 B   correct      75 ACE correct
```

Incorrect submissions were Question 9 `CD` instead of `BD`, Question 17 `A`
instead of `D`, Question 29 `AB` instead of `BE`, and Question 45 `E` instead
of `CE`.

## Miss 1 - Question 9: private origin plus dynamic origin selection

**Submitted:** `C,D`<br>
**Correct:** `B,D`

Two independent requirements had to be satisfied:

1. Origin Access Control plus the S3 bucket policy restricts the private S3
   origin to the CloudFront distribution.
2. Lambda@Edge on the origin-request path can select an origin dynamically from
   request attributes when path-based behaviors alone cannot express the rule.

`D` correctly solved dynamic selection. `C` did not secure the origin and could
not evaluate the stated country, device, and cookie combination as a path-only
cache-behavior rule. Omitting `B` left the direct-origin-access requirement
unsatisfied.

**Error category:** exact-match architecture composition and security-boundary
error.<br>
**Confidence:** confident miss.<br>
**Decision rule:** for a multi-requirement edge scenario, map each selected
control to a separate requirement: `OAC + bucket policy -> private S3 origin`;
`origin-request edge logic -> dynamic origin selection`.

## Miss 2 - Question 17: shared filesystem versus periodic copies

**Submitted:** `A`<br>
**Correct:** `D`

Two EBS volumes synchronized periodically by DataSync are two copies, not one
concurrently writable NFS namespace. They do not provide shared file locking or
the same live filesystem view across application instances. A Regional EFS
filesystem stores data redundantly across Availability Zones and provides NFS
mount targets for clients in the relevant zones.

**Error category:** storage-semantics and service-boundary error.<br>
**Confidence:** confident miss.<br>
**Decision rule:** `concurrent NFS clients + one coherent namespace + multi-AZ`
points to Regional EFS; DataSync moves or synchronizes data but does not turn
independent block volumes into a shared filesystem.

This is a recurrence of the EFS-versus-independent-EBS-copies boundary exposed
by the Skill Builder assessment.

## Miss 3 - Question 29: Batch custom AMI plus Spot capacity

**Submitted:** `A,B`<br>
**Correct:** `B,E`

The required custom compute-resource AMI selects an EC2-based AWS Batch managed
compute environment, not Fargate. The checkpointed, interruption-tolerant jobs
then justify diversified Spot capacity. `B` satisfied the custom-AMI and managed
scheduling requirement; `E` satisfied the stated cost and interruption model.
`A` did not satisfy the custom EC2 AMI requirement.

**Error category:** compute-environment boundary and exact-match composition
error.<br>
**Confidence:** confident miss.<br>
**Decision rule:** `custom host AMI -> Batch EC2`; `restartable queued batch ->
diversified Spot`. Do not select Fargate when the scenario requires control of
the compute-resource AMI.

This is a recurrence of the Skill Builder custom-AMI AWS Batch miss.

## Miss 4 - Question 45: warm-standby failover completeness

**Submitted:** `E`<br>
**Correct:** `C,E`

`E` correctly supplied tested automation for data failover, capacity scale-up,
and application recovery. The architecture also required a preconfigured
health-aware mechanism to move traffic to the recovery Region, supplied by
`C`. Recovery automation without the traffic-routing or routing-control layer
does not complete failover.

**Error category:** exact-match DR orchestration completeness error.<br>
**Confidence:** confident miss.<br>
**Decision rule:** regional failover is an end-to-end chain:

```text
health or authorized decision
  -> data failover
  -> application activation and scale-up
  -> traffic movement
  -> validation and rollback or failback
```

This is a recurrence of the Skill Builder pilot-light orchestration miss, where
recovery actions were recognized but failover routing was omitted.

## Transfer evidence

- ARC transfer passed: Question 32 correctly used the highly available ARC
  cluster endpoint rather than the ordinary Route 53 control-plane API. The
  question was marked uncertain and answered correctly.
- AS2 transfer passed twice: Questions 11 and 51 correctly selected AWS
  Transfer Family AS2 and its profile/agreement composition rather than Amazon
  MQ.
- Domain 3 transfer was clean at 18/18 after the focused 25/25 diagnostic.
- Domain 1 and Domain 4 were also clean at 20/20 and 15/15.
- The Skill Builder transfer gate is not fully closed: Questions 17, 29, and 45
  repeat three earlier service-composition boundaries, and Question 9 is a new
  exact-match composition miss around OAC plus dynamic origin selection.

## Learner causal review - 2026-08-14

The frozen answers and 71/75 score do not change. After reviewing the four
misses, the learner refined their causes:

- Question 29 is a genuine knowledge gap: Fargate supports selected runtime OS
  families and architectures but does not accept the required custom EC2 host
  AMI; the custom compute-resource AMI points to Batch on EC2.
- Question 17 is a requirement-parsing error. The phrase excluding one zonal
  block volume across zones distracted from the controlling requirement: every
  host must see one coherent POSIX namespace. Periodically synchronized EBS
  volumes do not provide that namespace.
- Questions 9 and 45 are rushed requirement-completeness errors, not missing
  service knowledge. The learner reports that a final review would have added
  OAC for the private origin and traffic movement for the recovery path.

These are learner-reported causal classifications, not changes to the frozen
marking. They narrow remediation to one service-boundary correction, one
constraint-parsing check, and a repeatable final requirement-to-selection
review.

## Readiness interpretation and next action

The 94.7% score on the more complex Mock 006 remains strong and keeps every
domain above 80%. The perfect uncertain-answer result shows good recovery when
the learner recognizes ambiguity. The 2026-08-14 causal review narrows the
remaining work to one genuine service-boundary gap, one constraint-parsing
error, and two rushed submission-completeness errors.

Do not restart a broad Domain 2 syllabus. Full Mock 007 on **2026-08-15** is the
next fresh independent transfer check after the completed review. Preserve the
two-full-mock cadence with Mocks 008 and 009 in the week beginning 2026-08-17.
The go/no-go booking decision now occurs after Full Mock 009.

## Official references

- [Restrict access to an S3 origin with CloudFront OAC](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/private-content-restricting-access-to-s3.html)
- [Lambda@Edge request and origin behavior](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/lambda-edge-event-request-response.html)
- [Amazon EFS features and Regional availability](https://docs.aws.amazon.com/efs/latest/ug/features.html)
- [Amazon EFS mount targets](https://docs.aws.amazon.com/efs/latest/ug/accessing-fs.html)
- [AWS Batch managed EC2 compute environments](https://docs.aws.amazon.com/batch/latest/userguide/create-compute-environment-managed-ec2.html)
- [AWS Batch custom compute-resource AMIs](https://docs.aws.amazon.com/batch/latest/userguide/create-batch-ami.html)
- [Cross-Region failover and warm-standby guidance](https://docs.aws.amazon.com/solutions/cross-region-failover-and-graceful-failback-on-aws/)
