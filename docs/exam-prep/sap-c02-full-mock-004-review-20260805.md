# SAP-C02 Full Mock 004 - Independent Review

<!-- markdownlint-disable MD013 MD060 -->

- **Last revised:** 2026-08-06
- **Assessment source:**
  [`sap-c02-full-mock-004-75q-20260804.md`](sap-c02-full-mock-004-75q-20260804.md)
- **Document role:** answer-bearing exact-match assessment; open only after the
  attempt is frozen.

## Verdict

**Result: 70/75 (93.3%).**

This is a fourth strong full-length result. The attempt finished in 113 of 180
minutes, retained 67 minutes, scored 45/48 on single-response questions, and
scored 25/27 on exact-match multiple-response questions. Every domain remains
above the 75% floor, although Domain 3 at 77.8% is now the clear remediation
priority.

The five misses are Questions 13, 14, 31, 47, and 75. Questions 13 and 75 test
the same underlying AWS Backup isolation model, so the result exposes four
themes rather than five unrelated gaps:

1. logically air-gapped vault versus a standard governance-locked vault;
2. CloudFront origin-failover method eligibility; and
3. complete ECS blue/green selection: CodeDeploy plus production/test listener
   configuration; and
4. the lack of TTL support on DynamoDB MRSC global tables.

This result preserves the high-confidence trend but does not change the chosen
booking boundary: Mocks 005-007 remain required before the first booking
decision.

## Submission Evidence

| Field | Result |
|---|---|
| Submission status | Frozen in the assessment document |
| Start / end | 21:25 / 23:18 |
| Time allowed / used / remaining | 180 / 113 / 67 minutes |
| Questions | 75 |
| Correct / incorrect / unanswered | 70 / 5 / 0 |
| Overall score | 93.3% |
| Average time per question | Approximately 1 minute 30 seconds |
| Uncertain questions | 3, 4, 5, 7, 14, 18, 23, 35, 38, 42, 43, 47, 52, 58, 65 |
| Uncertain answers correct | 13/15 |

## Score Breakdown

| Question type | Correct | Total | Score |
|---|---:|---:|---:|
| Single response | 45 | 48 | 93.8% |
| Multiple response, exact match | 25 | 27 | 92.6% |
| **Overall** | **70** | **75** | **93.3%** |

| Exam segment | Correct | Total | Score |
|---|---:|---:|---:|
| Questions 1-25 | 23 | 25 | 92% |
| Questions 26-50 | 23 | 25 | 92% |
| Questions 51-75 | 24 | 25 | 96% |

| SAP-C02 domain | Correct | Total | Score |
|---|---:|---:|---:|
| Domain 1: Design Solutions for Organizational Complexity | 20 | 20 | 100% |
| Domain 2: Design for New Solutions | 21 | 22 | 95.5% |
| Domain 3: Continuous Improvement for Existing Solutions | 14 | 18 | 77.8% |
| Domain 4: Accelerate Workload Migration and Modernization | 15 | 15 | 100% |

### Domain Allocation Used for Reconciliation

| Domain | Questions |
|---|---|
| Domain 1 | 1, 4, 5, 9, 17, 18, 21, 22, 23, 25, 29, 33, 37, 42, 45, 49, 53, 57, 61, 69 |
| Domain 2 | 2, 7, 10, 30, 34, 38, 41, 46, 47, 48, 54, 58, 60, 62, 63, 65, 66, 67, 70, 72, 73, 74 |
| Domain 3 | 6, 11, 13, 14, 15, 19, 26, 27, 31, 35, 39, 43, 50, 51, 55, 59, 71, 75 |
| Domain 4 | 3, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 52, 56, 64, 68 |

## Independently Reconciled Key

```text
1:B  2:AE  3:D  4:BCD  5:A  6:C  7:BE  8:B  9:AE  10:D
11:C  12:AEF  13:A  14:B  15:BE  16:D  17:A  18:CE  19:C  20:BDF
21:B  22:D  23:AD  24:C  25:A  26:CE  27:D  28:B  29:CD  30:C
31:AE  32:A  33:D  34:CD  35:C  36:B  37:BE  38:A  39:D  40:ADF
41:C  42:B  43:CE  44:D  45:A  46:BD  47:C  48:B  49:AE  50:D
51:C  52:BDF  53:A  54:D  55:CE  56:B  57:C  58:AD  59:A  60:D
61:BCE  62:C  63:B  64:AD  65:A  66:D  67:BE  68:C  69:B  70:CD
71:D  72:A  73:AE  74:C  75:BDF
```

## Miss Review

### Question 13 - Logically air-gapped vault

- **Submitted:** B - default backup vault with governance-mode Vault Lock
- **Correct:** A - logically air-gapped vault
- **Why A wins:** This vault type stores backups in an AWS Backup service-owned
  account, includes Vault Lock in compliance mode, supports AWS-owned or
  customer-managed encryption, and can be shared to named recovery accounts
  through AWS RAM.
- **Why B loses:** Governance mode can be removed by a sufficiently privileged
  identity and does not supply the service-owned-account isolation or sharing
  model stated in the question.
- **Decision rule:** service-owned backup-account storage plus compliance lock
  plus recovery-account sharing means logically air-gapped vault.
- **Plain-language mechanism:** this is not offline tape. You operate the vault
  through AWS Backup, while AWS stores the protected backup data in an AWS
  Backup service-owned account outside the workload administrator's ownership
  boundary. See the [mechanism-first vault model](aws-resilience-dr-sap-c02-key-lessons-20260718.md#what-logically-air-gapped-actually-means).
- **Source:** [AWS Backup logically air-gapped vault](https://docs.aws.amazon.com/aws-backup/latest/devguide/logicallyairgappedvault.html)

### Question 14 - CloudFront origin-failover methods

- **Submitted:** A - Lambda@Edge is required for every method
- **Correct:** B - origin failover applies only to eligible `GET`, `HEAD`, and
  `OPTIONS` requests
- **Why B wins:** CloudFront does not perform origin-group failover for write
  methods such as `POST`.
- **Why A loses:** Lambda@Edge is not the missing prerequisite; the request
  method is outside the built-in origin-failover eligibility boundary.
- **Decision rule:** CloudFront origin groups fail over read-style requests,
  not write methods; design application-level write failover separately.
- **Source:** [CloudFront origin failover](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/high_availability_origin_failover.html)

### Question 31 - Complete ECS blue/green configuration

- **Submitted:** A - AWS CodeDeploy with ECS blue/green deployment
- **Correct:** A and E - CodeDeploy plus production and test listeners or
  listener rules associated with the deployment
- **Why A and E win:** CodeDeploy orchestrates the ECS task-set deployment and
  traffic shift. The production and optional test listener paths provide the
  traffic-routing and validation surfaces required by the question.
- **Why the submission loses:** The deployment controller was correct, but a
  Choose TWO answer is incomplete without the listener configuration. Exact-
  match grading awards no credit for a partial set.
- **Decision rule:** ECS blue/green with pre-traffic validation means
  CodeDeploy **and** the load-balancer listener/rule configuration; an alarm can
  then stop or roll back the deployment.
- **Source:** [Amazon ECS blue/green deployments with CodeDeploy](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/deployment-type-bluegreen.html)

### Question 47 - DynamoDB MRSC and TTL

- **Submitted:** B - TTL is supported only with a witness Region
- **Correct:** C - the design must change because MRSC does not support TTL
- **Why C wins:** TTL is supported for MREC global tables but not for MRSC
  global tables, regardless of whether the three-Region topology uses three
  replicas or two replicas and a witness.
- **Why B loses:** A witness satisfies an MRSC topology option; it does not add
  TTL support.
- **Decision rule:** choose MRSC for cross-Region strong consistency only after
  checking its topology and feature restrictions; implement session expiry by
  another mechanism if MRSC is required.
- **Source:** [DynamoDB global-table security and TTL support](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/globaltables-security.html)

### Question 75 - Backup isolation and restore evidence

- **Submitted:** A, B, and F
- **Correct:** B, D, and F
- **Why B, D, and F win:** Compliance-mode Vault Lock resists deletion;
  separately controlled cross-account or logically air-gapped copies reduce
  the source-account blast radius; restore testing provides actual restore-job
  evidence and permits additional validation.
- **Why A loses:** Keeping the only recovery point in the workload account's
  default vault leaves the sole copy inside the administrative boundary whose
  compromise is part of the scenario.
- **Decision rule:** ransomware recovery needs immutability, administrative
  isolation, and demonstrated restoration; no one control proves all three.
- **Sources:** [AWS Backup Vault Lock](https://docs.aws.amazon.com/aws-backup/latest/devguide/vault-lock.html),
  [logically air-gapped vault](https://docs.aws.amazon.com/aws-backup/latest/devguide/logicallyairgappedvault.html),
  and [AWS Backup restore testing](https://docs.aws.amazon.com/aws-backup/latest/devguide/restore-testing.html)

## Uncertainty Analysis

| Question | Result | Tested distinction |
|---:|---|---|
| 3 | Correct | Repurchase rather than moving existing code or servers |
| 4 | Correct | Organization trail, protected destination, and KMS/S3 delivery policies |
| 5 | Correct | [VPC Lattice](aws-networking-sap-c02-key-lessons-20260717.md#vpc-lattice-application-networking-not-a-bigger-router): published application services, DNS/request routing, and IAM authorization rather than general Layer 3 transit |
| 7 | Correct | On-premises S3 access through interface endpoint plus hybrid DNS |
| 14 | Incorrect | CloudFront origin-failover method eligibility |
| 18 | Correct | Cross-account EventBridge bus resource policy plus sender permission |
| 23 | Correct | OAM sink and source-account links |
| 35 | Correct | Composite alarms for alarm-noise reduction |
| 38 | Correct | Narrow Standard `StartExecution` idempotency condition |
| 42 | Correct | Transit Gateway as a transitive routing hub |
| 43 | Correct | Planned Aurora switchover versus unplanned failover RPO |
| 47 | Incorrect | MRSC topology does not restore TTL support |
| 52 | Correct | Repurchase, rehost, and retire mappings |
| 58 | Correct | S3 Multi-Region Access Point plus failover controls |
| 65 | Correct | AWS Config recording prerequisite for Firewall Manager |

The uncertainty list identified two of the five misses. Three misses were not
marked uncertain, so the remediation should emphasize hard decision rules and
exact-match completeness, not confidence alone.

## Prior-Gap and New-Material Transfer

| Earlier topic | Mock 004 evidence | Result |
|---|---|---|
| IAM Identity Center workforce model | Question 9 selected account assignment plus provisioned role | Held |
| Migration Hub home Region | Question 16 selected the configured home Region | Held |
| DAX cluster plus application client | Questions 19 and 67 selected the DAX endpoint/client and Multi-AZ cluster | Held |
| On-premises S3 private access | Question 7 selected S3 interface endpoint plus Resolver inbound endpoint | Held |
| MGN test and cutover lifecycle | Questions 28 and 64 correctly separated test launch, healthy replication, cutover validation, and finalization | Held |
| Direct Connect BGP decisions | Questions 21 and 73 correctly applied longest-prefix match and local-preference communities | Held |
| ECS blue/green deployment | Question 31 selected CodeDeploy but omitted the required listener configuration in a Choose TWO response | Recurred as an exact-match completeness error |

The Mock 002 gaps remain transferred. The new migration and BGP additions also
held. The MRSC miss occurred despite the restriction being present in the
database lesson; classify it as retrieval/selection failure rather than missing
revision content.

## Bounded Remediation and Next Action

Do not restart Resilience/DR or DynamoDB study. Reproduce these rules from
memory:

1. Logically air-gapped vault = AWS Backup service-owned account storage,
   compliance-mode Vault Lock, and named-account sharing for recovery.
2. Ransomware recovery = immutable copy + separate administrative boundary +
   restore evidence.
3. CloudFront origin-group failover = `GET`, `HEAD`, and `OPTIONS`, not `POST`.
4. ECS blue/green validation = CodeDeploy plus the production/test listener or
   listener-rule configuration; select every required component.
5. DynamoDB MRSC = strong multi-Region reads subject to feature restrictions;
   TTL is unsupported.

Use a short, fresh exact-match retest of all four themes no earlier than
2026-08-07. It must not replace Full Mock 005 or the two-mock weekly cadence.
Booking remains deferred until the post-Mock-007 evidence review.

## Evidence Boundary

This is independently scored learner-recall evidence from an original practice
mock. It is not an AWS examination score and does not prove live Lakehouse,
backup, CloudFront, DynamoDB, or governance implementation.
