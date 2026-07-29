# SAP-C02 Full Mock 002 - Independent Review

<!-- markdownlint-disable MD013 MD060 -->

- **Last revised:** 2026-07-29
- **Assessment source:**
  [`sap-c02-full-mock-002-75q-20260728.md`](sap-c02-full-mock-002-75q-20260728.md)
- **Document role:** answer-bearing exact-match assessment; open only after the
  attempt is frozen.

## Verdict

**Result: 71/75 (94.7%).**

This is a second strong full-length result. All four domain scores are above
93%, multiple-response performance improved to 26/27, and pacing retained 41
minutes. The four misses are specific decision gaps rather than evidence of a
broad domain weakness.

The minimum two-mock booking-score gate is now met. The broader high-confidence
recommendation is still pending because the learner chose an eight-additional-
mock validation programme and wants repeated evidence rather than an optimistic
inference from the minimum gate.

## Submission evidence

| Field | Result |
|---|---|
| Submission status | Frozen in the assessment document |
| Start / end | 12:28 / 14:47 |
| Time allowed / used / remaining | 180 / 139 / 41 minutes |
| Questions | 75 |
| Correct / incorrect / unanswered | 71 / 4 / 0 |
| Overall score | 94.7% |
| Average time per question | Approximately 1 minute 51 seconds |
| Uncertain questions | 1, 10, 16, 28, 33, 44, 62, 65, 66, 67 |
| Uncertain answers correct | 8/10 |

## Score breakdown

| Question type | Correct | Total | Score |
|---|---:|---:|---:|
| Single response | 45 | 48 | 93.8% |
| Multiple response, exact match | 26 | 27 | 96.3% |
| Overall | 71 | 75 | 94.7% |

| Exam segment | Correct | Total | Score |
|---|---:|---:|---:|
| Questions 1-25 | 24 | 25 | 96% |
| Questions 26-50 | 23 | 25 | 92% |
| Questions 51-75 | 24 | 25 | 96% |

| SAP-C02 domain | Correct | Total | Score |
|---|---:|---:|---:|
| Domain 1: Design Solutions for Organizational Complexity | 18 | 19 | 94.7% |
| Domain 2: Design for New Solutions | 21 | 22 | 95.5% |
| Domain 3: Continuous Improvement for Existing Solutions | 18 | 19 | 94.7% |
| Domain 4: Accelerate Workload Migration and Modernization | 14 | 15 | 93.3% |

## Independently reconciled key

```text
1:B  2:ADF  3:D  4:A  5:BD  6:C  7:C  8:CE  9:B  10:A
11:AD  12:D  13:B  14:BE  15:C  16:A  17:BDF  18:D  19:C  20:CE
21:A  22:B  23:AC  24:D  25:C  26:BD  27:A  28:B  29:AE  30:D
31:C  32:CD  33:A  34:B  35:BCE  36:D  37:C  38:AD  39:A  40:B
41:ACF  42:D  43:C  44:BE  45:A  46:B  47:CD  48:D  49:C  50:AE
51:A  52:B  53:BD  54:D  55:C  56:BCE  57:A  58:B  59:AD  60:D
61:C  62:CE  63:A  64:B  65:BD  66:D  67:C  68:ACF  69:A  70:CE
71:B  72:ADF  73:D  74:BC  75:BDE
```

## Miss review

### Question 1 - Workforce access versus application identity

- **Submitted:** C - Amazon Cognito identity pools
- **Correct:** B - IAM Identity Center, permission sets, and group-to-account
  assignments
- **Why B wins:** IAM Identity Center centrally assigns workforce users and
  groups to AWS accounts through permission sets. It creates and manages the
  corresponding roles in the assigned accounts.
- **Why C loses:** Cognito identity pools provide identities and temporary AWS
  credentials primarily for application users. They are not the normal
  multi-account workforce access and permission-set control plane.
- **Decision rule:** workforce to AWS accounts means IAM Identity Center;
  application customers or application identities can point toward Cognito.
- **Source:** [Manage AWS accounts with permission sets](https://docs.aws.amazon.com/singlesignon/latest/userguide/permissionsetsconcept.html)

### Question 28 - Migration Hub home Region

- **Submitted:** A - copy the discovery database with DataSync
- **Correct:** B - use the configured Migration Hub home Region
- **Why B wins:** Migration Hub discovery and tracking data is associated with
  the selected home Region. Viewing another Region does not make the collected
  portfolio data appear there.
- **Why A loses:** DataSync transfers file and object data; it does not relocate
  Migration Hub's service-managed discovery database. Changing home Region
  requires recollecting data because the existing data does not migrate.
- **Decision rule:** Migration Hub organizes and tracks migration evidence in
  one home Region; it does not move workload data.
- **Source:** [Changing the Migration Hub home Region](https://docs.aws.amazon.com/migrationhub/latest/ug/change-home-region.html)

### Question 47 - DAX requires both the cluster and the DAX client

- **Submitted:** C
- **Correct:** C and D
- **Why C and D win:** A replicated, multi-node DAX cluster supplies the cache
  availability, while the application must use the DAX client so supported
  DynamoDB API requests are directed to that cluster.
- **Why the submission loses:** C selected the correct cache architecture but
  omitted the required application integration. Under exact-match grading, an
  incomplete Choose TWO response receives no credit.
- **Decision rule:** DAX is API-compatible, not magically transparent: deploy
  the DAX cluster and direct supported requests through the DAX client.
- **Source:** [DAX: How it works](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DAX.concepts.html)

### Question 73 - S3 access from on premises

- **Submitted:** B - S3 gateway endpoint
- **Correct:** D - S3 interface endpoint with the required private network,
  DNS, and endpoint-policy configuration
- **Why D wins:** S3 interface endpoints use private IP addresses and are
  reachable from on-premises applications through Direct Connect or Site-to-
  Site VPN.
- **Why B loses:** A gateway endpoint is route-table scoped to resources
  originating in its VPC. Its connectivity cannot be extended across Direct
  Connect, VPN, transit gateway, or VPC peering.
- **Decision rule:** S3 from a VPC normally favours the free gateway endpoint;
  S3 from on premises through private IPs requires an interface endpoint.
- **Sources:** [S3 gateway-endpoint considerations](https://docs.aws.amazon.com/vpc/latest/privatelink/vpc-endpoints-s3.html),
  [AWS PrivateLink for S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/privatelink-interface-endpoints.html)

## Uncertainty analysis

| Question | Result | Interpretation |
|---:|---|---|
| 1 | Incorrect | Genuine workforce-identity service-selection gap |
| 10 | Correct | Direct Connect longest-prefix transfer held |
| 16 | Correct | Application Migration Service selection held |
| 28 | Incorrect | Genuine Migration Hub home-Region gap |
| 33 | Correct | FSx for Lustre and S3-linked HPC selection held |
| 44 | Correct | DataSync agent plus task/location model held |
| 62 | Correct | Transit Gateway segmentation and appliance-mode model held |
| 65 | Correct | Idempotency plus compensation model held |
| 66 | Correct | Direct Connect MACsec selection held |
| 67 | Correct | Lambda reserved-concurrency control held |

The uncertainty list is useful: it identified two of the four misses without
showing a confidence collapse. Six uncertain answers tested recent migration,
networking, resilience, and service-boundary material and were correct.

## Bounded remediation

Do not restart any domain from scratch. Review and reproduce these four rules
from memory:

1. IAM Identity Center assigns workforce groups to accounts through permission
   sets; Cognito serves application-user identity patterns.
2. Migration Hub is the home-Region tracking surface; it is not a data-transfer
   engine.
3. DAX requires both an available cluster and the DAX client path.
4. S3 gateway endpoints serve originating VPC traffic; interface endpoints
   serve private on-premises access over Direct Connect or VPN.

Create a fresh, close-distractor spaced retest no earlier than 2026-07-31. Keep
it short enough that it does not replace either weekly full mock or the review
of that mock. Full mock 003 remains the next broad independent transfer check
after this bounded remediation.

## Evidence boundary

This result is timed learner-recall evidence from an original mock, not an AWS
exam score. It supports SAP-C02 readiness across all four domains and satisfies
the tracker's minimum two-qualifying-mock score gate. It does not replace the
learner's chosen repeated-mock consistency programme or prove live Lakehouse
implementation.
