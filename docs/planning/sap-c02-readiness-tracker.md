<!-- markdownlint-disable MD060 -->

# SAP-C02 Readiness Tracker

**Owner:** Shola  
**Created:** 2026-06-12  
**Target exam:** AWS Certified Solutions Architect – Professional, SAP-C02  
**Target attempt window:** Late November to mid-December 2026  
**Booking decision date:** 2026-11-15  
**Weekly capacity assumption:** 10–12 focused hours while not working  
**Controlling principle:** SAP-C02 is the steering architecture. The Energy Data Lakehouse is the practical case study. Everything else must support exam readiness, lakehouse credibility, or job-market positioning.
**Last repository reconciliation:** 2026-06-15

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

Source of truth: `/Users/shola/Kiro-Workspace/handlers/LEARNING-PLAN.md`.
This is a separate tutorial workspace, not part of the Energy Data Lakehouse
implementation. Its sessions count as study/lab work only when mapped to an
exam domain or documented weak area.

#### Energy Data Lakehouse repository

Purpose: provide the portfolio and SAP-C02 case study for governed AWS data
architecture, including S3, Glue, Athena, Parquet, IAM, KMS, CloudTrail, AWS
Config, cost controls, and Organizations/SCP guardrails.

Source of truth: this repository, its README, its evidence files, and the
lakehouse sections of this tracker. Its readiness closure remains the active
June-July repository sequence.

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
| 2026-06-15 | 10–12 |  |  |  |  | Tutorial consolidation + Glue/Athena IAM design |
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

---

## 3. SAP-C02 Domain Coverage

Official SAP-C02 domains:

| Domain | Weight | Status | Evidence required |
|---|---:|---|---|
| Domain 1: Design Solutions for Organizational Complexity | 26% | Partial | Workload IAM, logging, tagging, and budget evidence exist; Organizations, SCPs, Identity Center, central logging, and enterprise networking remain open |
| Domain 2: Design for New Solutions | 29% | In progress | Lakehouse and serverless workflow are substantially implemented; IAM deployment/service verification, resilience decisions, and consolidated evidence remain open |
| Domain 3: Continuous Improvement for Existing Solutions | 25% | Partial | Parquet, lifecycle, validation, observability, public-access controls, alerting, and cost guardrails exist; systematic improvement notes and remaining hardening are open |
| Domain 4: Accelerate Workload Migration and Modernization | 20% | Not started | 6 Rs, MGN, DMS, DataSync, Snow Family, Storage Gateway, migration playbook |

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
| Cost and ownership tags applied | Partial | Eight live data-bucket tags were verified on 2026-06-15; Billing cost-allocation activation and broader resource coverage remain open |
| Architecture diagram created | Verified | Current and target diagrams exist under `diagrams/`; `docs/target-operating-model.md` documents the target posture |

### Lakehouse closure gaps

The core lakehouse path is already implemented. The remaining June-July work
is to close and evidence the readiness gaps:

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
- [ ] Activate selected user-defined cost-allocation tags in AWS Billing only
  after separate account-governance approval.
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
- [ ] Reconcile `docs/phase-1-stabilize-ingestion-lakehouse.md` against the
  current mixed-energy implementation and close or replace its stale checks.
- [ ] Mark the June-July milestone complete only after all required gaps above
  have evidence links.

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
| AWS Organizations enabled | Not started |  |
| OU structure designed | Not started |  |
| Management account rules documented | Not started |  |
| Workload account purpose defined | Not started |  |
| Security/log archive account design documented | Not started |  |
| IAM Identity Center access model documented | Not started |  |
| Permission sets defined | Not started |  |
| Break-glass access model documented | Not started |  |
| SCP catalogue drafted | Not started |  |
| CloudTrail organization trail design documented | Not started |  |
| AWS Config design documented | Not started |  |
| GuardDuty/Security Hub concept documented | Not started |  |
| Cost allocation tags defined | Partial | Common Terraform tags exist; account-level activation and coverage evidence remain open |
| Budget alarms configured | Partial | A live `$1` managed-workflow AWS Budget with notifications is verified; broader workload/account budget design remains open |

### SCP catalogue

| SCP | Purpose | Status |
|---|---|---|
| Deny disabling CloudTrail | Protect audit evidence | Not started |
| Deny deleting log buckets | Protect log archive | Not started |
| Deny public S3 exposure | Reduce data leakage risk | Not started |
| Deny unapproved regions | Cost/compliance control | Not started |
| Deny root-user actions except emergencies | Reduce blast radius | Not started |
| Require encryption where feasible | Improve compliance posture | Not started |
| Deny leaving AWS Organization | Prevent governance bypass | Not started |

Critical note: **SCPs do not grant permissions.** They define maximum allowed permissions. IAM policies still grant permissions.

---

## 6. Networking Weak Areas

### Required comparison matrix

| Topic | Current confidence | Required by exam? | Evidence required |
|---|---:|---|---|
| VPC fundamentals | Medium | Yes | VPC/subnet/route table diagram |
| Security groups vs NACLs | Medium | Yes | Comparison note |
| VPC peering | Low | Yes | Use-case and limitation note |
| Transit Gateway | Low | Yes | Hub-and-spoke diagram |
| PrivateLink | Low | Yes | Comparison with peering/TGW |
| VPC endpoints | Medium | Yes | S3/DynamoDB endpoint lab or diagram |
| NAT Gateway | Medium | Yes | Cost and routing note |
| Direct Connect | Low | Yes | Hybrid connectivity decision table |
| Site-to-Site VPN | Low | Yes | DX vs VPN comparison |
| Route 53 Resolver | Low | Yes | Hybrid DNS diagram |
| Centralized inspection VPC | Low | Yes | Architecture sketch |

### Networking deliverables

| Deliverable | Due | Status |
|---|---|---|
| VPC connectivity comparison matrix | 2026-08-31 | Not started |
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
| AWS Application Migration Service | Low | Rehost use-case note |
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
|  |  | Untimed 20 questions |  |  |  |
|  |  | Untimed 20 questions |  |  |  |
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
|  |  |  |  |  |

---

## 10. Booking Decision Criteria

### Must be true before booking

| Criterion | Status |
|---|---|
| Two timed practice exams at 80%+ OR one 80%+ and one 75–79% with narrow weak areas | Not met |
| Domain 1 governance notes complete | Not met |
| Networking comparison matrix complete | Not met |
| Migration matrix complete | Not met |
| Lakehouse readiness closure complete and documented | Partially met: core path, encryption, versioning, lifecycle, bucket tags, IAM, and current end-to-end evidence are verified; cost-allocation tag activation and stale Phase 1 reconciliation remain open |
| IAM/Organizations/SCP design complete | Not met |
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
| June–July | Python/serverless tutorial hardening + lakehouse readiness closure | Tutorial milestones evidenced separately; S3, Glue, Parquet, Athena, IAM, KMS decisions, and lakehouse closure evidence complete |
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

- [ ] Log actual, build, study, and practice hours for the current week.
- [ ] Produce at least one required artifact for every study/build session.
- [ ] Run one practice-question block and record misses in the wrong-answer
  log.
- [ ] Update changed checklist rows with an evidence link, not only a status.
- [ ] Review hard deferrals before opening a new implementation branch.
- [ ] Verify that any AWS-changing task has explicit approval and a cost/rollback
  boundary.

### June-July exit checklist

- [ ] Complete every item in the Lakehouse closure gaps checklist.
- [ ] Confirm Domain 2 evidence includes storage, transformation, query,
  security, observability, cost, and resilience decisions.
- [ ] Complete at least two 20-question practice blocks and log all wrong
  answers.
- [ ] Review Domain 2 weak areas and update the next four-week plan.
- [ ] Change the June-July milestone to complete only when code, documentation,
  diagrams, and evidence agree.

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
