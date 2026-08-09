<!-- markdownlint-disable MD013 -->

# AWS Skill Builder SAP-C02 Assessment Review

**Last revised:** 2026-08-09

**Source:** `AWS Skill Builder SAP-C02 Assessment.xlsx`

**Assessment attempt:** 2

**Result:** Passed, scaled score 775; passing threshold 750

**Question result:** 45 correct, 30 incorrect, 0 skipped
**Recorded time:** 12 hours 29 minutes 10 seconds; average 9 minutes 59 seconds per question

## Evidence boundary

The workbook contains all 75 questions, the learner's selections, AWS's keyed answers, rationales and source links. It does not include the four domain-score values even though the page labels a Domain Scores section.

This is valuable official-practice calibration, but it is **not** booking-gate evidence:

- the recorded duration shows a paused/review-style assessment rather than a three-hour simulation;
- the scaled score is not interchangeable with the repository's independently marked raw mock percentages; and
- the tracker now requires Full Mock 008 before the first booking decision so
  the added service-boundary material receives spaced transfer evidence.

## Corrections to the earlier independent review

The complete workbook corrects several conclusions reached before the official key was available:

1. **Mandatory organization tags:** tag policies standardize supplied tags, Resource Groups reports existing noncompliance, and an SCP is needed where supported to deny resource creation that omits required tags. AWS Config remediation alone does not prevent future untagged creates.
2. **Consolidated security findings:** the least-operations answer is Organizations-integrated Security Hub with an administrator/member model, not Audit Manager. For a current multi-Region implementation, also configure central configuration or a home Region with linked Regions.
3. **Organization-wide SSH enforcement:** where the requirement is packet enforcement across accounts despite varied security groups, the assessment selects a Firewall Manager Network Firewall policy with ordered stateless rules. Content-audit security-group policy remains the answer only when the stem explicitly asks to audit/remediate security-group rule content.
4. **Db2 managed replatform:** the assessment keys MGN for the non-modernizable Windows server and DMS plus SCT for Db2-to-RDS conversion. My earlier `A,D` recommendation was wrong; the learner's `A,E` selection was correct. “SCT replication agent” is imprecise wording, but the intended architecture is SCT/schema conversion plus DMS data replication.
5. **Secrets Manager rotation:** hardcoded-password failure plus least development effort points to single-user rotation. Alternating users is justified by an explicit highest-availability-during-rotation requirement, not merely by the fact that an old hardcoded password failed.

## Dated-key exceptions

Do not memorize two assessment keys literally:

- **Question 7:** the assessment assumes Object Lock can be enabled only on a new bucket. Since November 2023, AWS supports enabling Object Lock on an existing bucket. Default retention protects new versions; apply retention explicitly or through S3 Batch Operations to existing versions.
- **Question 10:** the assessment treats FIPS Level 3 as a CloudHSM-only discriminator. Current KMS HSMs are FIPS 140-3 Security Level 3 validated. For a managed key that must be cryptographically unavailable outside a schedule, scheduled `DisableKey`/`EnableKey` is the current low-operations pattern. Editing key policies is still the wrong mechanism.

## Actionable wrong-answer register

The table records transferable rules rather than copying the assessment questions.
The companion [answer-difference audit](aws-skill-builder-sap-c02-answer-difference-audit-20260809.md)
adds the scenario context, supplied rationale, independent key verdict and
specific learner takeaway for all 30 mismatches. It also uses Question 74 as a
control case where the learner answer was correct and the earlier independent
review was not.

| Q | Selected | Assessment key | Reusable rule |
|---:|:---:|:---:|---|
| 5 | A | D | Tightly coupled grid/HPC network timeouts point to a cluster placement group; adding ordinary fleet capacity does not reduce east-west latency. |
| 7 | C | B | Date-qualified: compliance mode is required, but current S3 supports enabling Object Lock on an existing bucket; existing versions still need explicit retention. |
| 8 | C | B | Unknown mixed access during the first 180 days plus a six-hour restore ceiling points to Intelligent-Tiering with Archive Access, not a blanket lifecycle transition. |
| 10 | B | C | Date-qualified: policy edits are not a key-state schedule. Current low-operations KMS answer is scheduled disable/enable; CloudHSM is for explicit dedicated-HSM requirements. |
| 11 | D,E,F | B,E,F | Tightly coupled HPC combines single-AZ placement, EFA-capable instances and, when stated, disabling hyperthreading; PV AMIs are not the performance requirement. |
| 12 | D | C | Concurrent cross-AZ shared file access requires regional EFS; DataSync between independent EBS volumes is not a coherent shared filesystem. |
| 13 | B | C | Keep a suitable steady web baseline on reserved capacity, burst unpredictably with On-Demand, and use diversified compute-optimized Spot for restartable queued video analysis. |
| 19 | A | D | Put the more restrictive OU beneath the common-policy OU and move accounts inside the organization; do not remove and reinvite accounts. |
| 20 | C | D | A custom AMI requires an EC2 Batch compute environment; managed EC2 plus Spot fits a restartable monthly workload. |
| 21 | C,D,F | B,C,D | Connect supplies telephony/contact flows, Lex recognizes intent, and Lambda integrates business systems; Alexa for Business is not the backend integration component. |
| 23 | A | C | Repeated DynamoDB hot-key reads justify DAX; a predictable daily load can use provisioned capacity with auto scaling. DynamoDB has no Savings Plans. |
| 25 | C | A | For least development effort, move the hardcoded RDS credential to Secrets Manager and use single-user rotation; reserve alternating users for explicit highest availability. |
| 26 | C | B | Unexpected ALB demand with no CPU correlation should scale dynamically on `RequestCountPerTarget`, not on a schedule. |
| 41 | D | B | AD Connector is deployed in the WorkSpaces VPC and proxies authentication to on-premises AD over DX/VPN; it is not deployed on premises. |
| 43 | B | C | MemoryDB provides durable Redis-compatible storage; seed it from a supported Redis snapshot while MGN migrates only the application servers. |
| 45 | C | A | Standardize with tag policy, find existing problems with Resource Groups, correct through owning services, and use SCPs for supported mandatory-tag create controls. |
| 46 | B | A | Security Hub consolidates findings; Audit Manager assembles audit evidence. Use Organizations integration for lower multi-account overhead and add home/linked Regions when required. |
| 47 | B,D | B,E | Use an ASG across AZs for compute elasticity and Redis OSS with Multi-AZ automatic failover for a fault-tolerant cache; fixed extra instances and Memcached do not meet native failover. |
| 48 | A | B | An internet gateway is already regional and redundant; deploy one NAT gateway per AZ and route each private subnet to its local NAT gateway. |
| 50 | C | A | For low-volume private cross-Region access to a large single S3 dataset, use an S3 interface endpoint in the bucket Region over supported inter-Region VPC connectivity instead of duplicating all data. |
| 51 | D | B | An RDS standby cannot occupy another Region; Aurora Global Database provides the cross-Region PostgreSQL topology and managed RPO control. |
| 53 | D | C | Pilot-light automation needs both recovery actions and failover routing: health detection, replica promotion, ASG activation and Route 53 failover. Latency routing is not the budgeted passive pattern. |
| 54 | C | B | Put both infrastructure and repeatable bootstrap in the deployment artifact: CloudFormation plus EC2 user data or managed configuration, not an updated manual runbook. |
| 58 | C | B | Aurora Global write forwarding lets a secondary-Region application use one local cluster endpoint; managed planned failover is the routine DR test while both Regions are healthy. |
| 59 | B | A | For organization-wide packet enforcement, use Firewall Manager Network Firewall policy; stateless rules evaluate from the lowest numeric priority, so pass trusted SSH before the default/drop path. |
| 64 | A | C | A Standard queue cannot be converted in place to FIFO. Create a FIFO queue, update producers/consumers, and scale workers on backlog per instance rather than raw queue depth. |
| 67 | A | C | Memcached does not automatically rebalance an unaware client after nodes are added; use Auto Discovery or update the client with all node endpoints. |
| 72 | C | D | S3 prefixes are not policy-bearing folders. S3 access points provide scalable prefix-scoped access policies and aliases. |
| 73 | B | C | EC2 hibernation must be enabled at launch and requires supported instances plus an encrypted root volume; create an encrypted AMI and relaunch before hibernating. |
| 75 | A | B | Lambda@Edge can select an origin dynamically from request attributes, location and exception cookies/headers; CloudFront path behaviors alone cannot express that combined rule. |

## Priority interpretation

Eleven misses were marked **Confident**: Questions 20, 23, 48, 50, 51, 54, 58, 59, 67, 73 and 75. These deserve more attention than educated-guess misses because they reveal incorrect decision rules rather than acknowledged uncertainty.

The most useful clusters are:

1. **Service boundary and orchestration:** Batch custom AMI, CloudFormation bootstrap, Connect/Lex/Lambda and Lambda@Edge origin selection.
2. **Network and availability layer:** cluster placement, NAT AZ scope, S3 interface endpoints, Network Firewall versus security-group policy, and hibernation prerequisites.
3. **Database/cache mechanics:** DAX capacity economics, MemoryDB import, Aurora Global Database, Redis failover, and Memcached client discovery.
4. **Governance enforcement:** tag policy versus SCP, Security Hub versus Audit Manager, and OU inheritance.

These rules have been folded into the canonical revision notes. They should be tested through Full Mock 006 and later independent mocks rather than triggering another large content expansion.

## Current official references used for reconciliation

- S3 Object Lock on existing buckets: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-configure.html
- KMS key protection: https://docs.aws.amazon.com/kms/latest/developerguide/data-protection.html
- Security Hub with Organizations: https://docs.aws.amazon.com/securityhub/latest/userguide/securityhub-accounts-orgs.html
- Security Hub central configuration: https://docs.aws.amazon.com/securityhub/latest/userguide/central-configuration-intro.html
- Tag policy enforcement: https://docs.aws.amazon.com/organizations/latest/userguide/orgs_manage_policies_tag-policies-enforcement.html
- Network Firewall stateless rules: https://docs.aws.amazon.com/network-firewall/latest/developerguide/stateless-rule-groups-standard.html
- Secrets Manager rotation strategies: https://docs.aws.amazon.com/secretsmanager/latest/userguide/rotation-strategy.html
- Db2 as an AWS DMS source: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.DB2.html
- DMS Schema Conversion: https://docs.aws.amazon.com/dms/latest/userguide/schema-conversion-convert.html
