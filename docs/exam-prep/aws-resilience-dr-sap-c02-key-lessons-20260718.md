# AWS Resilience and Disaster Recovery: SAP-C02 Key Lessons - 2026-07-18

<!-- markdownlint-disable MD013 MD060 -->

**Last revised:** 2026-08-06<br>

## Purpose and Role

**Document role:** source-backed lesson and required DR pattern matrix. Return
to the [Exam-Prep Revision Hub](README.md) to choose a learn, test, review, or
audit workflow.

This lesson follows the completed Networking slice and teaches how to select a
recovery strategy from business requirements. It is documentation-only and
does not authorize backup, replication, recovery, failover, multi-Region, or
other AWS changes.

Use the companion
[RTO/RPO decision table](../planning/domain-2-rto-rpo-decision-table-20260718.md)
to turn business impact into objectives, check dependency constraints, and
record designed versus tested recovery capability without inventing targets.

## How to Revise This Lesson

| Time available | Revision route |
|---|---|
| 10 minutes | Read [Core Mental Model](#core-mental-model), [DR Pattern Matrix](#dr-pattern-matrix), and [High-Value Traps](#high-value-sap-c02-traps), then answer the Recall Check. |
| 25 minutes | Add the recovery-control and AWS-service decision tables, Lakehouse boundary, and testing rules. |
| 45 minutes | Read the full lesson, map one scenario to RTO/RPO and a pattern, then answer every recall question without notes. |

## Topic Navigation

| If the scenario says... | Go to |
|---|---|
| Maximum downtime or data-loss tolerance | [Core Mental Model](#core-mental-model) |
| Business ownership, dependency alignment, or an objective worksheet | [RTO/RPO decision table](../planning/domain-2-rto-rpo-decision-table-20260718.md) |
| Hours, tens of minutes, minutes, or near-zero recovery | [DR Pattern Matrix](#dr-pattern-matrix) |
| Backup, replication, high availability, or disaster recovery | [Recovery Controls Are Different](#recovery-controls-are-different) |
| AWS Backup, DRS, S3, RDS/Aurora, or DynamoDB | [AWS Service Decision Map](#aws-service-decision-map) |
| Immutable or isolated backups, Vault Lock, or ransomware recovery | [AWS Backup Isolation and Restore Evidence](#aws-backup-isolation-and-restore-evidence) |
| CloudFront origin-group failover | [CloudFront Origin-Failover Boundary](#cloudfront-origin-failover-boundary) |
| Regional outage, deletion, corruption, or ransomware | [Failure-Type Decision Map](#failure-type-decision-map) |
| Restore testing, failover, or failback | [Recovery Testing and Operations](#recovery-testing-and-operations) |

## Core Mental Model

- **Recovery Time Objective (RTO):** maximum acceptable delay between an
  interruption and restoration of service.
- **Recovery Point Objective (RPO):** maximum acceptable time since the last
  recoverable data point; it represents tolerated data loss in time.
- **High availability (HA):** keeps a workload operating through expected local
  failures, often across Availability Zones.
- **Disaster recovery (DR):** restores a workload after an event exceeds the
  primary design's normal availability boundary.

Start with business impact and dependencies, then choose a pattern. Do not pick
an attractive service first and invent the objectives afterward.

```text
business impact
  -> RTO and RPO
  -> failure scope
  -> recovery pattern
  -> service mechanisms
  -> tested runbook and failback
```

Exam rule: a more aggressive RTO/RPO normally increases standing capacity,
automation, replication, testing, and cost. Near-zero targets are not default
best practice when the business can tolerate a slower, cheaper recovery.

## DR Pattern Matrix

The time ranges below are qualitative scenario cues, not service guarantees.
Actual recovery depends on workload size, dependencies, automation, quotas,
data consistency, and testing.

| Pattern | Typical scenario cue | Running DR footprint | Relative RTO/RPO | Cost and complexity | Main activation work | Common trap |
|---|---|---|---|---|---|---|
| Backup and restore | Hours of downtime/data loss are acceptable; lowest standing cost is important | Backups, infrastructure definitions, and recovery access; little or no application capacity | Highest RTO and RPO of the four patterns | Lowest standing cost; restore duration and orchestration can be substantial | Restore data, deploy infrastructure, configure dependencies, validate, and redirect traffic | Calling a stored backup a proven recovery without a restore test |
| Pilot light | Core data/services stay replicated, while most application capacity is off or absent | Minimal critical core plus replicated data | Lower than backup/restore; often tens-of-minutes class in exam comparisons | More cost and synchronization work than backup/restore | Provision/scale application tiers, attach dependencies, validate, and redirect traffic | Confusing a pilot light with a fully running scaled-down stack |
| Warm standby | A complete but reduced-capacity environment is always running | Full functional stack at smaller scale | Minutes-class cue with lower data-loss tolerance | Higher standing cost; continuous deployment and scaling automation required | Scale up, confirm health/capacity, and redirect traffic | Choosing it when only backups or a data core exist in the DR Region |
| Multi-site active/active | Near-zero interruption is a justified business requirement; both sites serve traffic | Production environments actively serve traffic in multiple sites/Regions; capacity is sized for normal demand and the intended failure scenario | Lowest RTO/RPO potential | Highest cost and operational complexity; conflict, consistency, and dependency design are critical | Traffic shifts around failure rather than constructing the environment | Assuming active/active automatically prevents data conflicts or dependency failures |

### Pattern Selection Shortcut

1. If long recovery is acceptable and cost dominates: **backup and restore**.
2. If data/core services must be ready but application capacity can be created:
   **pilot light**.
3. If a complete reduced stack must already serve limited traffic: **warm
   standby**.
4. If both Regions actively serve and near-zero recovery is justified:
   **multi-site active/active**.

Do not memorize only the time labels. Identify what is already running in the
recovery Region and what must happen after failure.

## Recovery Controls Are Different

| Control | Primary job | Protects against | Does not prove |
|---|---|---|---|
| Multi-AZ design | Continue through an AZ-level infrastructure failure | Local infrastructure failure when the service/application supports failover | Regional recovery or recovery from logical corruption |
| Versioning/PITR | Recover an earlier data state | Accidental overwrite/delete or logical corruption within the retention window | Application reconstruction or Regional availability |
| Backup | Preserve independent recovery points | Deletion, corruption, and longer-term recovery needs according to policy | That the workload can restore within RTO |
| Cross-Region replication | Maintain a geographically separate copy | Regional loss and geographic requirements | Protection from every bad write/delete unless versioning or isolation also exists |
| Cross-account copy | Separate recovery ownership and blast radius | Source-account compromise or accidental source-side deletion, subject to policy | Regional independence unless the copy is also cross-Region |
| Immutable/locked backup | Restrict alteration/deletion for a retention period | Malicious or accidental backup deletion | Application availability or automated failover |
| DR orchestration | Create, scale, configure, and redirect the recovered workload | Manual delay and configuration error | Data correctness without validation and testing |

Exam trap: replication optimizes availability and recovery point freshness, but
can also replicate a bad application write. Pair availability replication with
point-in-time or immutable recovery where the threat model requires it.

## AWS Service Decision Map

| Requirement | AWS mechanism to assess first | Boundary |
|---|---|---|
| Centralized policy-based backups across supported AWS resources | AWS Backup plans, vaults, lifecycle, monitoring, and restore testing | Service/feature support varies; a plan is not a successful restore |
| Separate backups from a compromised workload account | AWS Backup cross-account copy to a destination vault in the same Organization | KMS, vault policy, Organizations, and resource support must align |
| Store protected copies in an AWS Backup service-owned account with compliance-mode locking and named-account recovery sharing | AWS Backup logically air-gapped vault | The vault supplies isolation and immutability; restoration still needs permissions, supported resources, testing, and application validation |
| Geographic backup recovery | AWS Backup cross-Region copy or service-native cross-Region backup | Copy frequency and restore time must meet the stated objectives |
| Recover server workloads with continuous block replication | AWS Elastic Disaster Recovery | Designed for server recovery; it is not the general backup answer for every managed service |
| Recover deleted or overwritten S3 objects | S3 Versioning plus deliberate recovery permissions and lifecycle | Versioning is same-bucket recovery, not Regional DR |
| Replicate S3 objects to another Region | S3 Cross-Region Replication; assess S3 Replication Time Control when a predictable replication target is required | Versioning, IAM/KMS, existing-object handling, delete behavior, monitoring, and cost remain explicit decisions |
| Relational database local HA | RDS Multi-AZ or Aurora replicas according to engine/design | Multi-AZ is not automatically cross-Region DR and read replicas differ from synchronous standby behavior |
| Relational database cross-Region recovery | Cross-Region read replica, Aurora Global Database, snapshots/backups, or DMS depending on engine and objectives | Promotion, endpoint/traffic change, write consistency, lag, and failback must be designed |
| DynamoDB point-in-time recovery | DynamoDB PITR or backups | Restores produce a table recovery path; they do not make the current table active in another Region |
| DynamoDB multi-Region availability | DynamoDB global tables | Consistency mode, conflict semantics, application routing, quotas, and cost matter |

### AWS Backup Versus Elastic Disaster Recovery

- **AWS Backup:** centralized data-protection policy and recovery points across
  supported resources.
- **AWS DRS:** continuous block-level replication and recovery orchestration for
  supported server workloads into AWS.

Exam shortcut: if the scenario is about recovering whole servers quickly after
a site/Region failure, assess DRS. If it is about centralized backup policy,
retention, vault separation, or recovery points across AWS services, assess AWS
Backup.

### AWS Backup Isolation and Restore Evidence

#### What “logically air-gapped” actually means

A traditional **physical air gap** means the recovery copy is on storage with
no active network path to the production environment. AWS Backup does not
unplug a disk or move a tape offline. The word **logically** means that AWS
creates the separation through service-enforced ownership, access, retention,
and recovery controls:

```text
workload account
    |
    | AWS Backup creates a recovery point
    v
logically air-gapped vault
    | vault settings and recovery workflow remain visible to you
    | compliance-mode Vault Lock enforces retention
    v
backup data stored in an AWS Backup service-owned account
    | you do not administer or sign in to this storage account
    |
    +-- optional AWS RAM share to a named recovery account
            |
            +-- authorized restore operation
```

The useful mental model is therefore:

> You manage the vault through AWS Backup, but the protected backup data is
> held outside your customer-account ownership boundary and cannot be made
> mutable by a workload-account administrator.

That helps when ransomware or a compromised administrator can damage the
workload account. The vault combines three properties:

1. **Administrative separation:** AWS Backup stores the backup data in an AWS
   Backup service-owned account rather than another account that the workload
   administrator controls.
2. **Immutability:** compliance-mode Vault Lock is included, so the retention
   controls cannot be removed after they become effective.
3. **Controlled recovery:** the vault can be shared through AWS RAM with named
   recovery accounts so an authorized recovery team can restore supported
   recovery points.

It is **not** a physically offline copy, an automatic cross-Region design, or
proof that the application can be recovered. You must still design recovery
permissions, decide whether a separate Region is required, confirm resource
support, run restore testing, and validate the recovered application.

#### Compare the isolation mechanisms

| Mechanism | Protection supplied | Exam boundary |
|---|---|---|
| Standard vault plus Vault Lock governance mode | Restricts changes to identities without the required lock-management permissions | A sufficiently privileged identity can remove governance-mode locking |
| Standard vault plus Vault Lock compliance mode | Makes the lock and retained recovery points immutable after the grace period | The vault remains inside its owning customer account; configure retention carefully because the lock cannot then be removed |
| Logically air-gapped vault | Stores backups in an AWS Backup service-owned account, includes compliance-mode Vault Lock, uses an AWS-owned key by default or an optional customer-managed key, and supports sharing to named accounts through AWS RAM | It is not automatically proof of a successful restore or application recovery; resource support and recovery access still matter |
| Cross-account copy | Places a copy under a separate customer-account administrative boundary | It is not automatically cross-Region or immutable; destination vault, KMS, policy, lock, and Organizations support must align |
| AWS Backup restore testing | Periodically starts real restore jobs, records duration and result, and can run validation | Restore-job success does not by itself prove application health, dependency readiness, or end-to-end RTO |

For a ransomware scenario, look for all three properties:

```text
immutability
    + administrative isolation
    + demonstrated restore and application validation
```

Keeping the only recovery point in the workload account's default vault does
not isolate it from compromise of that account. A successful backup job proves
neither restorability nor recovery within the business objective.

Exam trigger: choose a **logically air-gapped vault** when the question combines
AWS Backup service-owned storage, compliance-mode locking, and recovery from a
named account. Choose a **cross-account copy** when the requirement instead
calls for a separate customer-owned backup account and its own destination
vault, policy, KMS, and lock design.

### CloudFront Origin-Failover Boundary

CloudFront origin groups provide built-in origin failover only when the viewer
request method is `GET`, `HEAD`, or `OPTIONS`. CloudFront does not fail over
write methods such as `POST`, `PUT`, `PATCH`, or `DELETE` to the secondary
origin.

```text
read-style request: GET / HEAD / OPTIONS
    -> eligible for CloudFront origin-group failover

write request: POST / PUT / PATCH / DELETE
    -> design application-level routing, retry, idempotency, and data consistency
```

Lambda@Edge does not remove this built-in method restriction. Do not infer
multi-Region write failover merely because CloudFront reads can fail over.

## Failure-Type Decision Map

| Failure | First design question | Likely control family |
|---|---|---|
| Instance or AZ failure | Can the running architecture fail over automatically? | Multi-AZ HA, load balancing, managed-service failover |
| Regional outage | Is data and deployable capacity present elsewhere, and how is traffic redirected? | Cross-Region replication plus one of the four DR patterns |
| Accidental deletion or bad deployment | Can a known-good point be selected without using the corrupted current state? | Versioning, PITR, backup, rollback, immutable artifacts |
| Account compromise or ransomware | Is recovery data isolated from the compromised administrative boundary? | Cross-account vault, restrictive policy, Vault Lock/logically air-gapped design where justified |
| Dependency outage | Can the workload meet its objective if DNS, identity, secrets, keys, queues, or external services are unavailable? | Dependency-aware objectives, replicated configuration, fallback, and tested runbook |

## Recovery Testing and Operations

A credible DR plan covers:

1. failure declaration authority and escalation;
2. data recovery point selection and integrity checks;
3. infrastructure deployment or scale-up;
4. identity, secrets, KMS keys, certificates, quotas, DNS, and network paths;
5. application smoke tests and business validation;
6. traffic redirection and monitoring;
7. communication and audit evidence;
8. failback, reconciliation, and cleanup; and
9. periodic recovery tests or game days.

Important distinctions:

- a **backup job succeeded** does not prove the data is usable;
- a **restore job succeeded** does not prove the application works;
- a **failover succeeded** does not prove failback or data reconciliation;
- a **runbook exists** does not prove operators can meet the objective.

Measure actual restore and recovery time during tests. Compare the results with
the approved RTO/RPO, record gaps, and revise automation or objectives.

## Lakehouse Application Boundary

### Current Evidence

The Energy Data Lakehouse currently has:

- an evidenced single-Region data and processing path;
- S3 versioning and noncurrent-version lifecycle controls;
- infrastructure definitions and operational evidence; and
- data that may be re-ingestable or reproducible from source feeds, subject to
  source availability and retention.

The repository does not prove cross-Region S3 replication, an AWS Backup vault
or plan for the Lakehouse, a tested restore, approved RTO/RPO values, or a
multi-Region application environment.

### Decision Gate

Do not select warm standby or active/active merely to make the architecture
look more resilient. First determine:

1. which raw data can be reacquired and which data is irreplaceable;
2. the acceptable outage and data-loss windows for ingestion, transformation,
   query, and reporting;
3. whether a Regional event or account compromise is in scope;
4. whether infrastructure, catalog metadata, scripts, permissions, keys, and
   query configuration can be recreated inside the RTO; and
5. whether the additional replication, backup, KMS, transfer, and standby costs
   are justified for this low-volume portfolio workload.

Until those objectives are approved, backup/restore is a pattern to assess—not
an accepted live design—and no paid multi-Region resource is authorized.

## High-Value SAP-C02 Traps

1. **RTO versus RPO:** time to restore versus tolerated data loss in time.
2. **HA versus DR:** Multi-AZ continuity does not by itself solve Regional loss.
3. **Backup versus replication:** recoverable historical point versus fresher
   secondary copy; both may be required.
4. **Pilot light versus warm standby:** critical core only versus complete
   reduced-capacity stack.
5. **Warm standby versus active/active:** scaled-down passive capacity versus
   multiple sites actively serving.
6. **AWS Backup versus DRS:** policy-driven recovery points versus server
   block-replication and launch orchestration.
7. **Route 53 failover:** redirects DNS answers; it does not replicate data or
   rebuild the application.
8. **Successful backup:** does not prove restore integrity or RTO.
9. **Cross-Region only:** may not isolate recovery from an account compromise.
10. **Logically air-gapped vault:** “air-gapped” means service-enforced
    separation, not offline media; service-owned-account storage and compliance
    locking still do not replace restore testing or application validation.
11. **CloudFront origin failover:** applies to eligible `GET`, `HEAD`, and
    `OPTIONS` requests, not write methods such as `POST`.
12. **Over-engineering:** the lowest RTO/RPO pattern is not automatically the
    correct answer when cost and business tolerance favor a simpler design.

## Recall Check

Answer without looking above:

1. What is the difference between RTO and RPO?
2. What is already running in pilot light versus warm standby?
3. Why is Multi-AZ not the same as multi-Region DR?
4. When does backup and restore beat warm standby?
5. Why can replication fail to protect against a bad application write?
6. When should AWS DRS be selected instead of AWS Backup?
7. What separate risks do cross-Region and cross-account copies address?
8. Why does a successful backup job not prove that RTO can be met?
9. What additional design work does active/active require for data consistency?
10. Which Lakehouse facts must be known before selecting a live DR pattern?
11. What distinguishes governance-mode Vault Lock, compliance-mode Vault Lock,
    and a logically air-gapped vault?
12. Why do CloudFront origin groups not supply automatic `POST` failover?

## Official AWS References

- [Define recovery objectives for downtime and data loss](https://docs.aws.amazon.com/wellarchitected/latest/framework/rel_planning_for_recovery_objective_defined_recovery.html)
- [Plan for disaster recovery](https://docs.aws.amazon.com/wellarchitected/latest/framework/rel-13.html)
- [Defining a disaster-recovery strategy](https://docs.aws.amazon.com/prescriptive-guidance/latest/strategy-database-disaster-recovery/defining.html)
- [What is AWS Backup?](https://docs.aws.amazon.com/aws-backup/latest/devguide/whatisbackup.html)
- [Managing AWS Backup across accounts](https://docs.aws.amazon.com/aws-backup/latest/devguide/manage-cross-account.html)
- [Creating backup copies across accounts](https://docs.aws.amazon.com/aws-backup/latest/devguide/create-cross-account-backup.html)
- [AWS Backup Vault Lock](https://docs.aws.amazon.com/aws-backup/latest/devguide/vault-lock.html)
- [AWS Backup logically air-gapped vault](https://docs.aws.amazon.com/aws-backup/latest/devguide/logicallyairgappedvault.html)
- [AWS Backup restore testing](https://docs.aws.amazon.com/aws-backup/latest/devguide/restore-testing.html)
- [AWS Elastic Disaster Recovery concepts](https://docs.aws.amazon.com/drs/latest/userguide/CloudEndure-Concepts.html)
- [CloudFront origin failover](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/high_availability_origin_failover.html)
- [S3 Cross-Region Replication requirements](https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication-requirements.html)
- [S3 Replication Time Control](https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication-time-control.html)
- [DynamoDB disaster-recovery strategy](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamodbDisasterRecoveryStrategy.html)
