<!-- markdownlint-disable MD060 -->

# SAP-C02 Readiness Tracker

**Owner:** [redacted-owner]  
**Created:** 2026-06-12  
**Target exam:** AWS Certified Solutions Architect – Professional, SAP-C02  
**Target attempt window:** Late November to mid-December 2026  
**Booking decision date:** 2026-11-15  
**Weekly capacity assumption:** 10–12 focused hours while not working  
**Controlling principle:** SAP-C02 is the steering architecture. The Energy Data Lakehouse is the practical case study. Everything else must support exam readiness, lakehouse credibility, or job-market positioning.
**Last repository reconciliation:** 2026-06-18
**Last practice evidence update:** 2026-06-19
**Last tutorial evidence update:** 2026-06-21

---

## 0. Steering Rules

### Non-negotiable rule

Every study/build session must produce at least one artifact:

- code commit
- architecture diagram
- Architecture Decision Record (ADR)
- service comparison table
- IAM/SCP policy example
- wrong-answer log entry
- exam-domain note
- operational runbook/checklist

### Scope filter

Before adding any new topic, answer:

1. Does it improve SAP-C02 readiness?
2. Does it strengthen the Energy Data Lakehouse case study?
3. Does it improve job-market positioning within 4 weeks?
4. Is it required for IAM, governance, networking, resilience, migration, or cost?

If the answer is **no**, defer it.

### Hard deferrals until after SAP-C02 attempt

- Deep Kubernetes / EKS
- AI orchestration beyond light conceptual notes
- Polished UI/dashboard
- Full Control Tower deployment unless cheap and quick
- Complex microservices platform
- Deep REMIT workflow build-out
- Excessive Python refinement beyond reliable AWS automation
- Non-essential portfolio polish

### Status and evidence rules

| Status | Meaning |
|---|---|
| Verified | Implemented and supported by current repository or live evidence |
| Implemented | Code or configuration exists, but current end-to-end or live evidence is incomplete |
| Partial | Some of the outcome exists, but a material design, security, evidence, or completion gap remains |
| In progress | The item is the active programme focus and still has open completion criteria |
| Not started | No sufficient implementation or evidence was found |
| Deferred | Intentionally frozen by the scope rules |

Status must follow evidence, not intention. A Terraform resource proves an
implemented configuration path; it does not by itself prove that a live
resource exists. Existing dashboard and AI capabilities may be maintained, but
new expansion remains deferred unless the tracker is explicitly changed.

### Planning authority

1. This tracker controls scope, milestones, and next-step priority.
2. `README.md` describes current implemented platform truth.
3. `PLANS.md` preserves delivery history and translates this tracker into the
   current execution sequence.
4. Phase documents and evidence files prove individual outcomes; they do not
   override tracker deferrals or sequencing.

### Programme workspace and sequence control

#### Python/serverless tutorial workspace

Purpose: advance practical AWS serverless implementation skill and SAP-C02
readiness through Lambda, Step Functions, S3, DynamoDB, EventBridge, SQS, IAM,
observability, testing, and production-oriented handler patterns.

Source of truth: `/Users/[redacted-user]/Kiro-Workspace/handlers/LEARNING-PLAN.md`.
This is a separate tutorial workspace, not part of the Energy Data Lakehouse
implementation. Its sessions count as study/lab work only when mapped to an
exam domain or documented weak area.

#### Energy Data Lakehouse repository

Purpose: provide the portfolio and SAP-C02 case study for governed AWS data
architecture, including S3, Glue, Athena, Parquet, IAM, KMS, CloudTrail, AWS
Config, cost controls, and Organizations/SCP guardrails.

Source of truth: this repository, its README, its evidence files, and the
lakehouse sections of this tracker. Its repo-side June-July closure milestone is
complete; the remaining pre-governance work is study cadence, practice review,
and carry-forward review before the scheduled governance phase.

#### Scheduled AWS governance and multi-account work

Purpose: add professional-level governance competence through AWS
Organizations, OUs, SCPs, IAM Identity Center, centralized logging, audit
controls, and cost governance.

This work supports both workspaces where relevant, but it is sequenced rather
than maintained as a third simultaneous backlog. IAM, KMS, logging, and cost
decisions needed for lakehouse closure may proceed now. The dedicated
Organizations/SCP phase begins on 2026-07-13 unless the tracker is explicitly
changed.

#### Parked work

- Docker and containers
- new AI orchestration expansion

Do not start parked work unless the task directly supports a current
SAP-C02 milestone and is explicitly approved as a short exception. Maintenance,
observation, cost control, and rollback work for the already-proven lakehouse AI
workflow are allowed when required by this tracker.

#### Cross-workspace evidence rule

Tutorial code must not be described as lakehouse implementation evidence,
copied into this repository by default, or used to bypass the lakehouse closure
sequence. Before promoting a tutorial pattern into the lakehouse:

1. Identify the tracker closure gap or SAP-C02 weak area it addresses.
2. Record why it belongs in the lakehouse rather than remaining a standalone
   exercise.
3. Adapt and test it against lakehouse contracts and IAM boundaries; do not
   copy tutorial code unchanged.
4. Update a lakehouse status only after repository evidence exists.

### 2026-06-12 repository reconciliation findings

| Tracker area that was behind | Repository reality | Reconciliation |
|---|---|---|
| Domain 1 marked `Not started` | Workload IAM, logging, tagging, alerting, and cost evidence exist | Changed to `Partial`; enterprise governance remains open |
| Domain 3 marked `Not started` | Parquet, lifecycle, validation, observability, public-access controls, and cost guardrails exist | Changed to `Partial`; formal improvement evidence remains open |
| Raw and curated described as separate buckets | The implemented lake uses one data bucket with `raw/` and `curated/` zones | Corrected architecture and checklist wording; ADR accepted on 2026-06-13 |
| Entire lakehouse baseline marked `Not started` | S3 zones, Glue Catalog/ETL, Parquet, Athena, logging, tags, and diagrams are implemented or verified | Replaced blanket statuses with evidence-backed row statuses |
| Glue IAM described only as missing | A dedicated role exists, but its data-bucket access is too broad | Changed to `Partial`; least-privilege hardening added to closure checklist |
| Athena query access described only as missing infrastructure | The Athena workgroup exists, but no dedicated analyst/query role is defined | Split query-layer proof from the remaining access-policy gap |
| Governance cost controls marked `Not started` | Terraform tags and a live managed-workflow AWS Budget exist | Changed tags and budget rows to `Partial`; broader account coverage remains open |
| Lakehouse booking gate marked simply `Not met` | The core path is already proven, but security and consolidation gaps remain | Changed to `Partially met` with named blockers |
| AI and dashboard listed only as future deferred ideas | Both are already implemented; the managed workflow is scheduled and budget-guarded | Reframed as frozen existing baselines with maintenance only |
| `PLANS.md` and `README.md` still directed work toward Phase 17 continuation | The new programme makes this tracker controlling | Added an active SAP-C02 sequence and preserved old phases as history |
| Phase 1 checklist assumes the platform is still unimplemented | Mixed-energy ingestion, ETL, Athena, dashboard, and evidence already exist | Added reconciliation of that checklist as a June-July closure task |

---

## 1. Target Exam Date

| Item | Target |
|---|---|
| Internal readiness decision | 2026-11-15 |
| Earliest exam attempt | 2026-11-25 |
| Preferred exam window | 2026-11-25 to 2026-12-15 |
| Latest practical exam attempt | 2026-12-20 |
| Exam booking rule | Book only after two timed practice exams at 80%+ or one 80%+ plus narrow, well-understood weak areas |

### Booking decision criteria

| Practice score by 2026-11-15 | Decision |
|---:|---|
| 80%+ twice timed | Book late November / early December |
| 75–79% timed | Book December only if weak areas are narrow and fixable |
| 65–74% timed | High-risk attempt; decide based on finances and confidence |
| Below 65% timed | Do not book unless accepting likely failure |

---

## 2. Weekly Hours Logged

Target: **10–12 focused hours/week**.

During June-July, record Python/serverless tutorial sessions as study/lab hours
and Energy Data Lakehouse implementation as build hours. Use the Notes column
to identify the workspace and artifact. Do not count one artifact as evidence
for both workspaces.

### Programme kickoff

| Date | Session | Planned artifact | Status |
|---|---|---|---|
| Sunday, 2026-06-14 target | Lakehouse architecture decision | Shared S3-zone and bucket naming/ownership ADR | Completed 2026-06-13 |

| Week starting | Target hours | Actual hours | Build hours | Study hours | Practice hours | Notes |
|---|---:|---:|---:|---:|---:|---|
| 2026-06-15 | 10–12 | 20 | 6 | 10 | 4 | Tutorial Lesson 26 evidence: `/Users/[redacted-user]/Kiro-Workspace/handlers/learning-summary.md`; Glue/Athena IAM evidence: `docs/evidence/glue-athena-iam-live-verification-20260615.md`; practice blocks: Sections 8 and 9 below |
| 2026-06-22 | 10–12 |  |  |  |  | Tutorial dependency and handler-boundary hardening + lakehouse IAM hardening |
| 2026-06-29 | 10–12 |  |  |  |  | Serverless resilience + Glue/Athena IAM hardening |
| 2026-07-06 | 10–12 |  |  |  |  | Tutorial evidence + lakehouse Domain 2 closure review |
| 2026-07-13 | 10–12 |  |  |  |  | IAM foundation |
| 2026-07-20 | 10–12 |  |  |  |  | Organizations/SCP design |
| 2026-07-27 | 10–12 |  |  |  |  | Logging/governance |
| 2026-08-03 | 10–12 |  |  |  |  | Governance hardening |
| 2026-08-10 | 10–12 |  |  |  |  | Governance review |
| 2026-08-17 | 10–12 |  |  |  |  | Networking start |
| 2026-08-24 | 10–12 |  |  |  |  | VPC/TGW/PrivateLink |
| 2026-08-31 | 10–12 |  |  |  |  | Hybrid connectivity |
| 2026-09-07 | 10–12 |  |  |  |  | Resilience/DR |
| 2026-09-14 | 10–12 |  |  |  |  | DR + backup |
| 2026-09-21 | 10–12 |  |  |  |  | Migration services |
| 2026-09-28 | 10–12 |  |  |  |  | Migration decision matrix |
| 2026-10-05 | 10–12 |  |  |  |  | Cost optimization |
| 2026-10-12 | 10–12 |  |  |  |  | Migration/cost consolidation; containers parked |
| 2026-10-19 | 10–12 |  |  |  |  | First full timed exam |
| 2026-10-26 | 10–12 |  |  |  |  | Remediation |
| 2026-11-02 | 10–12 |  |  |  |  | Full timed exam |
| 2026-11-09 | 10–12 |  |  |  |  | Final readiness review |
| 2026-11-16 | 10–12 |  |  |  |  | Booking/exam prep |
| 2026-11-23 | 10–12 |  |  |  |  | Exam window |
| 2026-11-30 | 10–12 |  |  |  |  | Exam window |
| 2026-12-07 | 10–12 |  |  |  |  | Exam window |
| 2026-12-14 | 10–12 |  |  |  |  | Final exam window |

### External tutorial evidence register

| Date | Workspace lesson | Status | SAP-C02 mapping | Evidence and boundary |
|---|---|---|---|---|
| 2026-06-21 | Python/serverless Lesson 26: idempotency and duplicate persistence protection | Completed locally | Domain 2 resilience; Domain 3 continuous improvement | `/Users/[redacted-user]/Kiro-Workspace/handlers/LEARNING-PLAN.md` and `learning-summary.md`; 168 tests passed; fake conditional-check exception injected at `persist_trade_status_record`; no AWS deployment; not lakehouse implementation evidence |

---

## 3. SAP-C02 Domain Coverage

Official SAP-C02 domains:

| Domain | Weight | Status | Evidence required |
|---|---:|---|---|
| Domain 1: Design Solutions for Organizational Complexity | 26% | Partial | Workload IAM, logging, tagging, budget evidence, Organizations membership, selected Cost Allocation Tag activation, and governance preflight evidence exist; final OU design, SCPs, Identity Center, central logging, and enterprise networking remain open |
| Domain 2: Design for New Solutions | 29% | In progress | Lakehouse readiness closure, repository-side Domain 2 consolidation, two 20-question practice blocks, and separate Lesson 26 idempotency tutorial evidence are complete; practice review and later networking/DR decisions remain open |
| Domain 3: Continuous Improvement for Existing Solutions | 25% | Partial | Parquet, lifecycle, validation, observability, public-access controls, alerting, cost guardrails, and separate Lesson 26 idempotency tutorial evidence exist; systematic improvement notes and remaining hardening are open |
| Domain 4: Accelerate Workload Migration and Modernization | 20% | Partial | Exercise 002 exposed a rehost-vs-refactor/MGN weak area; 6 Rs, MGN, DMS, DataSync, Snow Family, Storage Gateway, and migration playbook artifacts remain open |

### Weekly domain focus

| Period | Primary domain focus | Secondary focus |
|---|---|---|
| 2026-06-15 to 2026-07-12 | Domain 2 | Domain 3 |
| 2026-07-13 to 2026-08-09 | Domain 1 | Domain 3 |
| 2026-08-10 to 2026-09-13 | Domain 1 | Domain 2 |
| 2026-09-14 to 2026-10-04 | Domain 4 | Domain 3 |
| 2026-10-05 to 2026-10-18 | Domain 3 | Domain 2 |
| 2026-10-19 onward | All domains | Practice exam remediation |

---

## 4. Energy Data Lakehouse Readiness Status

### Target architecture

```text
Energy Data Lakehouse
│
├── S3 data bucket
│   ├── raw/ zone
│   └── curated/ zone
├── Glue Data Catalog
├── Glue ETL to Parquet
├── Athena query layer
├── IAM roles and policies
├── KMS encryption
├── CloudWatch logs
├── CloudTrail / AWS Config design
├── Cost tags and budgets
└── Governance guardrails using Organizations/SCPs
```

### Implemented baseline and closure status

| Item | Status | Evidence |
|---|---|---|
| S3 raw zone available | Verified | Shared data bucket with `raw/` prefix; `README.md`; `docs/entsog-gas-build-plan.md`; `docs/evidence/phase14d-lambda-reconciliation-apply-summary-20260521.md` |
| S3 curated zone available | Verified | Shared data bucket with `curated/` prefix; curated Parquet and `docs/evidence/athena-gas-query-summary-20260506.md` |
| Bucket naming and ownership standard defined | Verified | `docs/adr/0001-shared-s3-data-bucket.md` accepts the current name, defines the naming pattern, and distinguishes workload ownership from Terraform management |
| Data bucket versioning posture | Verified | Versioning was enabled on 2026-06-15 after lifecycle protection was applied; see `docs/evidence/s3-versioning-tagging-apply-20260615.md` |
| Encryption model and KMS target defined | Verified | `docs/adr/0002-encryption-and-kms-design.md` retains SSE-S3 for the current public-data lab and defines promotion triggers, ownership, rotation, migration, rollback, and cost controls; `docs/policies/kms-lakehouse-key-policy.example.json` provides the policy design |
| Data bucket Public Access Block | Verified | All four bucket-level public-access controls were enabled on 2026-06-14 |
| Data bucket lifecycle protection | Verified | Raw transitions remain 30/90/180 days; noncurrent versions expire after 30 days, expired delete markers are removed, and incomplete multipart uploads abort after 7 days |
| Glue Data Catalog created | Verified | `aws_glue_catalog_database.lakehouse`, raw/curated crawlers, and Phase 9 import evidence |
| Glue ETL job converts raw to Parquet | Verified | `glue/etl_raw_to_parquet.py`, `aws_glue_job.raw_to_parquet`, and ENTSOG curated Parquet evidence |
| Athena can query curated data | Verified | Athena workgroup plus schema/query evidence under `docs/evidence/athena-*` |
| IAM role for Glue least privilege | Verified | Terraform restricts listing and reads to `raw/`, `curated/`, and `scripts/`, with writes/deletes limited to `curated/`; live Glue crawler/job verification passed on 2026-06-15 |
| IAM role or policy for Athena query access | Verified | Dedicated role is workgroup-scoped, catalog-read-only, limited to `curated/` reads and `athena-results/` writes; assumed-role Athena query passed and raw-prefix list was denied on 2026-06-15 |
| CloudWatch logging enabled | Implemented | Lambda log groups, Glue continuous logging, Glue metrics, and Athena workgroup metrics are configured |
| Cost and ownership tags applied | Verified | Eight live data-bucket tags were verified on 2026-06-15; selected Billing Cost Allocation Tags were activated from the Organizations management account on 2026-06-17 |
| Architecture diagram created | Verified | Current and target diagrams exist under `diagrams/`; `docs/target-operating-model.md` documents the target posture |

### Lakehouse closure gaps

The core lakehouse path is implemented, and the June-July closure gaps are
complete with evidence links:

- [x] On 2026-06-13, record an ADR confirming one shared data bucket
  with `raw/` and `curated/` prefixes, including when separate buckets would be
  preferable: `docs/adr/0001-shared-s3-data-bucket.md`.
- [x] In the same ADR, record the bucket naming and ownership standard,
  including the current referenced-but-not-Terraform-managed posture.
- [x] On 2026-06-14, verify the live data bucket's versioning, encryption,
  public-access block, lifecycle, and tags using read-only commands:
  `docs/evidence/s3-data-bucket-posture-20260614.md`.
- [x] On 2026-06-15, enable versioning after applying 30-day noncurrent-version
  expiry, expired delete-marker cleanup, and 7-day multipart-upload cleanup.
- [x] On 2026-06-15, apply and verify the approved ownership, classification,
  environment, workload, and cost-attribution tags:
  `docs/evidence/s3-versioning-tagging-apply-20260615.md`.
- [x] On 2026-06-17, activate selected user-defined cost-allocation tags in
  AWS Billing from the Organizations management account:
  `docs/evidence/cost-allocation-tag-activation-20260617.md`.
- [x] On 2026-06-14, decide and document SSE-S3 versus SSE-KMS for raw,
  curated, Athena results, logs, and dashboard artifacts:
  `docs/adr/0002-encryption-and-kms-design.md`.
- [x] In the same ADR, design KMS key ownership, key policy, rotation,
  service-role access, migration, rollback, cost, and monitoring controls.
- [x] Record SSE-KMS implementation as conditional on a documented promotion
  trigger and explicit approval; it is not a current closure requirement.
- [x] On 2026-06-15, restrict the Glue role in local Terraform to required
  prefixes and actions; validate the boundary with
  `scripts/check_lakehouse_iam_policies.py`, ADR 0004, and
  `docs/evidence/glue-athena-iam-preflight-20260615.md`.
- [x] On 2026-06-15, add a dedicated Athena query role in local Terraform with
  bounded workgroup, catalog, curated-data, and query-result permissions;
  record the access model in ADR 0004 and the preflight evidence file.
- [x] On 2026-06-15, review and explicitly approve the IAM Terraform plan, apply the Glue and
  Athena policy changes using `docs/glue-athena-iam-deployment-runbook.md`,
  and capture live role/service verification evidence:
  `docs/evidence/glue-athena-iam-live-verification-20260615.md`.
- [x] On 2026-06-15, capture one current raw -> Glue -> curated Parquet ->
  Athena validation evidence chain:
  `docs/evidence/glue-athena-iam-live-verification-20260615.md`.
- [x] On 2026-06-16, reconcile
  `docs/phase-1-stabilize-ingestion-lakehouse.md` against the current
  mixed-energy implementation and close its stale checks.
- [x] On 2026-06-17, mark the lakehouse readiness closure complete after all
  required June-July closure gaps above had evidence links.

### Lakehouse scope boundaries

| Must have before exam | Existing optional baseline | Defer new expansion |
|---|---|---|
| S3 raw/curated | Lake Formation | New UI/dashboard expansion |
| Glue ETL | Existing EventBridge schedules, maintenance only | New AI orchestration expansion |
| Parquet | Step Functions orchestration | Deep REMIT workflow |
| Athena | DynamoDB metadata | Complex API |
| IAM | Basic data quality checks | Multi-region deployment |
| KMS |  |  |
| CloudWatch |  |  |
| CloudTrail/Config design |  |  |
| Cost tags |  |  |

---

## 5. Governance Readiness Status

### Multi-account target

```text
AWS Organization
│
├── Management Account
│   ├── AWS Organizations
│   ├── Billing
│   ├── IAM Identity Center
│   └── SCP administration
│
├── Security / Logging Design
│   ├── CloudTrail organization trail
│   ├── AWS Config aggregation
│   ├── GuardDuty / Security Hub concept
│   └── central log archive design
│
└── Workload Account
    ├── Energy Lakehouse
    ├── Serverless workflows
    ├── ECS/Fargate mini-lab
    └── VPC/networking labs
```

### Governance checklist

| Item | Status | Evidence |
|---|---|---|
| AWS Organizations enabled | Verified | Management account and member accounts verified in `docs/evidence/cost-allocation-tag-activation-20260617.md` |
| OU structure designed | Design accepted | Target OU model recorded in `docs/adr/0005-aws-organizations-governance-design.md`; no live OU changes approved |
| Management account rules documented | Design accepted | Control-plane account rules recorded in `docs/adr/0005-aws-organizations-governance-design.md`; implementation boundary remains future approval |
| Workload account purpose defined | Design accepted | Lakehouse workload and sandbox account boundaries recorded in `docs/adr/0005-aws-organizations-governance-design.md` |
| Security/log archive account design documented | Partial | Target security/log archive boundary recorded in `docs/adr/0005-aws-organizations-governance-design.md`; detailed CloudTrail/log archive design and implementation remain open |
| IAM Identity Center access model documented | Design accepted | Permission-set candidates and account targets recorded in `docs/adr/0005-aws-organizations-governance-design.md`; detailed matrix recorded in `docs/planning/identity-center-permission-set-matrix-20260619.md`; no permission sets implemented |
| Permission sets defined | Design accepted | Permission-set matrix recorded in `docs/planning/identity-center-permission-set-matrix-20260619.md`; final AWS-managed/custom policy choice and assignments remain open |
| Break-glass access model documented | Design accepted | Break-glass target recorded in ADR 0005 and procedure recorded in `docs/runbooks/break-glass-access-procedure.md`; live principal, alerting, and review implementation remain open |
| SCP catalogue drafted | Design accepted | Accepted SCP catalogue recorded in ADR 0005; example policy files recorded in `docs/policies/scp/`; no SCPs attached or tested |
| CloudTrail organization trail design documented | Partial | Organization trail direction recorded in `docs/adr/0005-aws-organizations-governance-design.md`; detailed log archive/KMS/retention design and live enablement remain open |
| AWS Config design documented | Partial | Organization aggregation direction recorded in `docs/adr/0005-aws-organizations-governance-design.md`; recorder scope, managed rules, cost controls, and live enablement remain open |
| GuardDuty/Security Hub concept documented | Partial | Security-service sequencing recorded in `docs/adr/0005-aws-organizations-governance-design.md`; delegated-admin and enablement decisions remain open |
| Cost allocation tags defined | Verified | Common Terraform tags exist; selected Billing Cost Allocation Tags were activated from the Organizations management account on 2026-06-17 |
| Budget alarms configured | Partial | A live `$1` managed-workflow AWS Budget with notifications is verified; broader workload/account budget design remains open |

### SCP catalogue

| SCP | Purpose | Status |
|---|---|---|
| Deny disabling CloudTrail | Protect audit evidence | Partial |
| Deny deleting log buckets | Protect log archive | Partial |
| Deny public S3 exposure | Reduce data leakage risk | Partial |
| Deny unapproved regions | Cost/compliance control | Partial |
| Deny root-user actions except emergencies | Reduce blast radius | Partial |
| Require encryption where feasible | Improve compliance posture | Partial |
| Deny leaving AWS Organization | Prevent governance bypass | Partial |

Critical note: **SCPs do not grant permissions.** They define maximum allowed permissions. IAM policies still grant permissions.

Example SCP files now exist in `docs/policies/scp/`, but no SCP is attached,
tested, or authorized for live use.

---

## 6. Networking Weak Areas

### Required comparison matrix

| Topic | Current confidence | Required by exam? | Evidence required |
|---|---:|---|---|
| VPC fundamentals | Medium | Yes | VPC/subnet/route table diagram |
| Security groups vs NACLs | Medium | Yes | Comparison note |
| VPC peering | Low | Yes | Use-case and limitation note; compact comparison started in `docs/planning/domain-2-network-access-patterns-20260621.md` |
| Transit Gateway | Low | Yes | Hub-and-spoke diagram; compact comparison started in `docs/planning/domain-2-network-access-patterns-20260621.md` |
| PrivateLink | Low | Yes | Comparison with peering/TGW; compact comparison started in `docs/planning/domain-2-network-access-patterns-20260621.md` |
| VPC endpoints | Medium | Yes | S3/DynamoDB endpoint lab or diagram |
| NAT Gateway | Medium | Yes | Cost and routing note |
| Direct Connect | Low | Yes | Hybrid connectivity decision table; compact comparison started in `docs/planning/domain-2-network-access-patterns-20260621.md` |
| Site-to-Site VPN | Low | Yes | DX vs VPN comparison; compact comparison started in `docs/planning/domain-2-network-access-patterns-20260621.md` |
| Route 53 Resolver | Low: missed in exercise 002 | Yes | Hybrid DNS diagram; compact comparison started in `docs/planning/domain-2-network-access-patterns-20260621.md` |
| Centralized inspection VPC | Low | Yes | Architecture sketch |

### Networking deliverables

| Deliverable | Due | Status |
|---|---|---|
| VPC connectivity comparison matrix | 2026-08-31 | Partial: compact carry-forward note recorded in `docs/planning/domain-2-network-access-patterns-20260621.md`; full milestone matrix remains open |
| Transit Gateway hub-and-spoke diagram | 2026-09-07 | Not started |
| PrivateLink vs peering vs TGW decision table | 2026-09-07 | Not started |
| Direct Connect vs VPN decision table | 2026-09-14 | Not started |
| Route 53 Resolver hybrid DNS diagram | 2026-09-14 | Not started |
| NAT Gateway cost warning note | 2026-09-14 | Not started |

---

## 7. Migration Weak Areas

### Required services

| Service / concept | Current confidence | Evidence required |
|---|---:|---|
| 6 Rs migration strategy | Medium | Decision table |
| AWS Application Migration Service | Low: missed in exercise 002 | Rehost use-case note |
| AWS Database Migration Service | Medium | Homogeneous vs heterogeneous examples |
| AWS DataSync | Low | Storage transfer use-case note |
| Snow Family | Low | Offline transfer decision note |
| Storage Gateway | Low | Hybrid storage use-case note |
| Migration Hub | Low | Migration tracking note |
| AWS Backup | Low | Lakehouse backup strategy |
| Elastic Disaster Recovery | Low | DR use-case note |
| RDS/Aurora migration paths | Medium | DMS/RDS/Aurora comparison |

### Migration deliverables

| Deliverable | Due | Status |
|---|---|---|
| 6 Rs migration matrix | 2026-09-21 | Not started |
| Data migration service comparison | 2026-09-28 | Not started |
| Database migration decision table | 2026-09-28 | Not started |
| DR pattern matrix | 2026-10-05 | Not started |
| RTO/RPO decision table | 2026-10-05 | Not started |

---

## 8. Practice Question Scores

### Rule

Start with small question blocks immediately. Full timed exams begin in late October.

| Date | Source | Mode | Score | Domain weakness | Action |
|---|---|---|---:|---|---|
| 2026-06-19 | `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/exercises/exercise-001.md` | Untimed 20 questions | 20/20 | None identified | User-confirmed 20/20; no wrong-answer logging required |
| 2026-06-19 | `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/exercises/sap-c02-exercise-002-marking-and-revision-log.md` | Untimed 20 questions | 18/20 | Hybrid DNS; migration strategy selection | Wrong answers logged; drill Route 53 Resolver and rehost/MGN scenario wording |
|  |  | Timed 30 questions |  |  |  |
|  |  | Full timed exam |  |  |  |
|  |  | Full timed exam |  |  |  |

### Score interpretation

| Score | Interpretation |
|---:|---|
| <60% | Knowledge gap, not exam-ready |
| 60–69% | Some foundations, but weak professional judgement |
| 70–74% | Nearing readiness, but risky |
| 75–79% | Potential December attempt if weak areas are narrow |
| 80%+ | Bookable if repeated under timed conditions |

---

## 9. Wrong-Answer Log

Use this format for every missed question.

```text
Date:
Question theme:
SAP-C02 domain:
My answer:
Correct answer:
Why correct:
Why my answer was wrong:
Exam trap:
Service comparison:
Action:
```

### Wrong-answer table

| Date | Theme | Domain | Trap | Remediation |
|---|---|---|---|---|
| 2026-06-19 | Hybrid DNS: AWS and on-premises private name resolution | Domain 1 / networking | Confused AWS Config aggregation with DNS forwarding | Use Route 53 Resolver inbound/outbound endpoints and forwarding rules; see exercise 002 revision log |
| 2026-06-19 | Migration strategy: urgent data-centre exit with minimal change | Domain 4 | Chose the attractive long-term refactor answer instead of the constraint-led rehost answer | Use AWS Application Migration Service for rehost first, then optimize; see exercise 002 revision log |

---

## 10. Booking Decision Criteria

### Must be true before booking

| Criterion | Status |
|---|---|
| Two timed practice exams at 80%+ OR one 80%+ and one 75–79% with narrow weak areas | Not met |
| Domain 1 governance notes complete | Partially met: governance preflight, Organizations governance ADR, SCP examples, permission-set matrix, and break-glass procedure are documented; logging/security-service runbooks and live-readiness evidence remain open |
| Networking comparison matrix complete | Not met |
| Migration matrix complete | Not met |
| Lakehouse readiness closure complete and documented | Met: core path, encryption, versioning, lifecycle, bucket tags, Billing Cost Allocation Tag activation, IAM, current end-to-end evidence, and stale Phase 1 reconciliation are complete |
| IAM/Organizations/SCP design complete | Partially met: target Organizations, OU, Identity Center, SCP catalogue, SCP examples, and break-glass procedure are documented; exception tests, assignment decisions, rollback plans, and implementation evidence remain open |
| Wrong-answer log reviewed twice | Not met |
| No major unknowns in VPC, TGW, PrivateLink, DX/VPN, DR, migration | Not met |

### Final booking decision

| Date | Decision | Reason |
|---|---|---|
| 2026-11-15 | Pending |  |

---

## 11. Weekly Operating Template

### Monday to Friday

| Day | Session | Timebox | Output |
|---|---|---:|---|
| Monday | SAP-C02 study | 60 min | Notes + 5 review questions |
| Tuesday | Build/lab | 60–90 min | Code commit or config artifact |
| Wednesday | Practice questions | 60 min | Score + wrong-answer log |
| Thursday | Build/documentation | 60–90 min | Diagram/policy/ADR |
| Friday | Weak-area review | 60 min | Updated tracker |

### Weekend

| Block | Timebox | Output |
|---|---:|---|
| Saturday deep block | 3–4 hrs | Main build milestone |
| Sunday review block | 2–3 hrs | Diagrams, remediation, next-week plan |

---

## 12. Monthly Milestones

| Month | Main objective | Exit criteria |
|---|---|---|
| June–July | Python/serverless tutorial hardening + lakehouse readiness closure | Closeout complete: lakehouse code, documentation, diagrams, and evidence now agree with two 20-question practice blocks, wrong-answer logging, and separate tutorial evidence; remaining pre-governance work is carry-forward review |
| August | IAM, Organizations, SCPs, logging, governance | OU/SCP/logging/IAM design complete |
| September | Networking, hybrid connectivity, resilience | TGW/PrivateLink/DX/VPN/DR comparison artifacts complete |
| October | Migration, modernization, and cost optimization | Migration and cost artifacts complete; first full practice exam; containers remain parked unless readiness is already on track and a short exception is approved |
| November | Practice exams and remediation | Booking decision based on timed scores |
| December | Exam attempt | Attempt only if readiness criteria are met |

---

## 13. Parking Lot

Use this to capture attractive distractions without acting on them.

| Idea | Why attractive | Decision | Revisit date |
|---|---|---|---|
| Further AI orchestration expansion | Existing managed path is already proven and scheduled | Freeze at current baseline; maintenance and cost control only | After SAP-C02 |
| Deep EKS | Interesting but not critical | Defer | After SAP-C02 |
| Docker/container implementation | Useful skill, but not required for the current readiness path | Defer unless a short milestone-linked exception is approved | After SAP-C02 |
| Further UI/dashboard expansion | Existing hosted dashboard is already proven | Freeze at current baseline; maintenance only | After SAP-C02 |
| Complex REMIT workflow | Domain-relevant but large | Defer | After SAP-C02 |

---

## 14. Repository Reconciliation Checklist

Use this checklist to prevent the tracker, platform documentation, and old
phase plans from drifting apart again.

### Immediate repository reset

- [x] Merge the completed Phase 17AU evidence branch.
- [x] Synchronize local `main` with `origin/main`, including the resolved
  repository governance instructions.
- [x] Confirm the worktree contains no conflict markers or accidental
  untracked governance file.
- [x] Reconcile tracker status labels against repository evidence.
- [x] Make the tracker the explicit planning authority in `README.md` and
  `PLANS.md`.
- [x] Replace the old AI/dashboard continuation path with the SAP-C02 active
  sequence while preserving completed phase history.

### Weekly progress control

- [x] Log actual, build, study, and practice hours for the current week: see
  the 2026-06-15 row in Weekly Hours Logged above.
- [x] Produce at least one required artifact for every study/build session: see
  `/Users/[redacted-user]/Kiro-Workspace/handlers/learning-summary.md`,
  `docs/evidence/glue-athena-iam-live-verification-20260615.md`, and Sections 8
  and 9 below.
- [x] Run one practice-question block and record misses in the wrong-answer
  log.
- [x] Update changed checklist rows with an evidence link, not only a status:
  see the 2026-06-15 weekly hours row above, the External tutorial evidence
  register, Sections 8 and 9 below, and
  `docs/planning/domain-2-lakehouse-consolidation-20260617.md`.
- [ ] Review hard deferrals before opening a new implementation branch.
- [ ] Verify that any AWS-changing task has explicit approval and a cost/rollback
  boundary.

### June-July exit checklist

- [x] Complete every item in the Lakehouse closure gaps checklist.
- [x] Confirm Domain 2 evidence includes storage, transformation, query,
  security, observability, cost, and resilience decisions:
  `docs/planning/domain-2-lakehouse-consolidation-20260617.md`.
- [x] Complete at least two 20-question practice blocks and log all wrong
  answers: see Sections 8 and 9 below,
  `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/exercises/exercise-001.md`,
  and
  `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/exercises/sap-c02-exercise-002-marking-and-revision-log.md`.
- [x] Record Python/serverless Lesson 26 idempotency evidence separately from
  lakehouse implementation evidence; 168 local tests passed and no AWS
  resources were deployed: see the External tutorial evidence register above,
  `/Users/[redacted-user]/Kiro-Workspace/handlers/LEARNING-PLAN.md`, and
  `/Users/[redacted-user]/Kiro-Workspace/handlers/learning-summary.md`.
- [x] Review Domain 2 weak areas and update the next four-week plan:
  `docs/planning/domain-2-lakehouse-consolidation-20260617.md`.
- [x] Change the June-July milestone to complete only when code,
  documentation, diagrams, evidence, practice blocks, and separate tutorial
  evidence agree: see Section 12 above, `README.md`, `PLANS.md`, and
  `docs/planning/domain-2-lakehouse-consolidation-20260617.md`.

### Domain 1 governance preflight

- [x] Record a repo-only governance preflight that aligns with the tracker and
  keeps the Python/serverless tutorial workspace separate:
  `docs/planning/domain-1-governance-preflight-20260618.md`.
- [x] Draft current account structure, target OU shape, management-account
  rules, workload boundaries, Identity Center permission-set candidates, SCP
  catalogue, logging/security-service outline, and budget/tagging governance
  without making live AWS changes.
- [x] Convert the preflight into an accepted Organizations governance design
  ADR with explicit decisions, trade-offs, rejected alternatives, SAP-C02
  implications, revisit conditions, and implementation boundaries:
  `docs/adr/0005-aws-organizations-governance-design.md`.
- [x] Create SCP policy examples for the accepted catalogue:
  `docs/policies/scp/`.
- [x] Create an IAM Identity Center permission-set matrix:
  `docs/planning/identity-center-permission-set-matrix-20260619.md`.
- [x] Document the break-glass access procedure:
  `docs/runbooks/break-glass-access-procedure.md`.
- [ ] In the next governance batch, document CloudTrail/log archive design, AWS
  Config and GuardDuty design, and the Security Hub defer/adopt decision.
- [ ] During the scheduled governance phase, convert the accepted design into
  live-readiness evidence and explicitly approved implementation changes.

---

## 15. Acronym Legend

| Acronym | Meaning |
|---|---|
| AWS | Amazon Web Services |
| SAP-C02 | AWS Certified Solutions Architect – Professional exam version |
| IAM | Identity and Access Management |
| SCP | Service Control Policy |
| OU | Organizational Unit |
| S3 | Simple Storage Service |
| KMS | Key Management Service |
| VPC | Virtual Private Cloud |
| TGW | Transit Gateway |
| DX | Direct Connect |
| VPN | Virtual Private Network |
| DNS | Domain Name System |
| DR | Disaster Recovery |
| RTO | Recovery Time Objective |
| RPO | Recovery Point Objective |
| ECS | Elastic Container Service |
| ECR | Elastic Container Registry |
| EKS | Elastic Kubernetes Service |
| ALB | Application Load Balancer |
| ADR | Architecture Decision Record |
| REMIT | Regulation on Wholesale Energy Market Integrity and Transparency |
