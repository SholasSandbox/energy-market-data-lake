<!-- markdownlint-disable MD060 -->

# SAP-C02 Readiness Tracker

**Owner:** [redacted-owner]  
**Created:** 2026-06-12  
**Target exam:** AWS Certified Solutions Architect – Professional, SAP-C02  
**Target attempt window:** Late November to mid-December 2026  
**Booking decision date:** 2026-11-15  
**Weekly capacity assumption:** 10–12 focused hours while not working  
**Controlling principle:** SAP-C02 is the steering architecture. The Energy Data Lakehouse is the practical case study. Everything else must support exam readiness, lakehouse credibility, or job-market positioning.
**Last repository reconciliation:** 2026-07-01
**Last practice evidence update:** 2026-07-01
**Last tutorial evidence update:** 2026-07-01
**Last governance study evidence update:** 2026-07-01

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
decisions needed for lakehouse closure may proceed now. The planned live-change
Organizations/SCP phase begins on 2026-07-13 unless the tracker is explicitly
changed. Repo-only design artifacts for a later state may begin earlier once
the prior state is complete and no architectural, design, structural, or
sequencing blocker remains, and no explicit approval dependency is still open
for a design decision that cannot be responsibly assumed or has not yet been
recorded in an ADR or equivalent design artifact with explicit trade-offs.

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
| 2026-06-22 | 10–12 |  |  |  |  | Tutorial hardening evidence now includes Lesson 28 boundary isolation and the Ruff formatting baseline in `/Users/[redacted-user]/Kiro-Workspace/handlers/LEARNING-PLAN.md` and `learning-summary.md`; lakehouse IAM hardening |
| 2026-06-29 | 10–12 |  |  |  |  | Governance study evidence now includes SAP-C02 mental-model diagrams, OAM vs CloudTrail log archive vs AWS Config aggregator comparison, and local practice blocks 003-006 in `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance`; Serverless Architecture evidence now includes Lessons 29-33 and 217 local tests in `/Users/[redacted-user]/Kiro-Workspace/handlers`; no new lakehouse implementation evidence |
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
| 2026-06-21 | Python/serverless Lesson 27: consolidation review | Completed locally | Domain 3 operational excellence | `/Users/[redacted-user]/Kiro-Workspace/handlers/LEARNING-PLAN.md` and `learning-summary.md`; naming/import/formatting cleanup recorded; README/git-baseline note refreshed; 168 tests passed; no AWS deployment; not lakehouse implementation evidence |
| 2026-06-25 | Python/serverless Lesson 28: persistence handler boundary hardening | Completed locally | Domain 3 operational excellence; Domain 2 resilience | `/Users/[redacted-user]/Kiro-Workspace/handlers/LEARNING-PLAN.md` and `learning-summary.md`; strict input validation, typed helpers, structured error responses, and 36 new handler-boundary tests recorded; 204 tests passed; no AWS deployment; not lakehouse implementation evidence |
| 2026-06-25 | Python/serverless Ruff formatting baseline | Completed locally | Domain 3 operational excellence | `/Users/[redacted-user]/Kiro-Workspace/handlers/LEARNING-PLAN.md` and `learning-summary.md`; `ruff` added to `pyproject.toml`; consistent style applied across 20 tutorial source/test files; 204 tests passed with no behaviour changes; no AWS deployment; not lakehouse implementation evidence |
| 2026-06-26 | Python/serverless Lesson 29: persistence failure ordering | Completed locally | Domain 2 resilience; Domain 3 continuous improvement | `/Users/[redacted-user]/Kiro-Workspace/handlers/docs/lessons/lesson-29-persistence-failure-ordering.md`, `tests/test_trade_persistence_workflow.py`, `LEARNING-PLAN.md`, and `learning-summary.md`; S3-success/DynamoDB-failure and deterministic retry key behavior documented; no AWS deployment; not lakehouse implementation evidence |
| 2026-06-26 | Python/serverless Lesson 30: least-privilege IAM checklist for persistence | Completed locally | Domain 1 secure architecture; Domain 3 security improvement | `/Users/[redacted-user]/Kiro-Workspace/handlers/docs/iam/persistence-handler-iam-checklist.md`, `LEARNING-PLAN.md`, and `learning-summary.md`; Lambda vs Step Functions role boundary, S3/DynamoDB/log permissions, and encryption cautions documented; no AWS deployment; not lakehouse implementation evidence |
| 2026-06-27 | Python/serverless Lesson 31: retry-safe persistence and reconciliation | Completed locally | Domain 2 resilience; Domain 3 operational excellence | `/Users/[redacted-user]/Kiro-Workspace/handlers/docs/lessons/lesson-31-retry-safety-and-reconciliation.md`, `LEARNING-PLAN.md`, and `learning-summary.md`; retry, catch, fail, reconciliation, and no-default-delete compensation guidance documented; no AWS deployment; not lakehouse implementation evidence |
| 2026-06-27 | Python/serverless Lesson 32: S3 key design and encryption assumptions | Completed locally | Domain 1 security boundaries; Domain 2 storage design; Domain 3 improvement | `/Users/[redacted-user]/Kiro-Workspace/handlers/docs/lessons/lesson-32-s3-key-design-and-encryption.md`, `LEARNING-PLAN.md`, and `learning-summary.md`; accepted/rejected prefixes, deterministic keys, overwrite behavior, and bucket default encryption assumptions documented; no AWS deployment; not lakehouse implementation evidence |
| 2026-07-01 | Python/serverless Lesson 33: Step Functions timeout and terminal failure | Completed locally | Domain 1 role-boundary reasoning; Domain 2 resilience; Domain 3 continuous improvement | `/Users/[redacted-user]/Kiro-Workspace/handlers/step-functions/persistence-task-timeout-terminal-failure.asl.json`, `tests/test_step_functions_timeout_terminal_failure_definition.py`, `docs/lessons/lesson-33-step-functions-timeout-and-terminal-failure.md`, `LEARNING-PLAN.md`, and `learning-summary.md`; timeout, bounded retry, catch, reconciliation routing, explicit fail state, and Lambda-only Step Functions role boundary verified; 217 tests passed; no AWS deployment; not lakehouse implementation evidence |

### External governance study evidence register

| Date | Governance study artifact | Status | SAP-C02 mapping | Evidence and boundary |
|---|---|---|---|---|
| 2026-06-27 | SAP-C02 mental-model reference diagrams | Committed to governance repo | Domains 1-4 mental-model consolidation | `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/SAP-C02_Mental_Model_Reference_Diagrams.md`; committed in governance repo `d068a65`; external revision evidence, not lakehouse implementation evidence |
| 2026-06-28 | OAM vs CloudTrail log archive vs AWS Config aggregator comparison | Committed to governance repo | Domain 1 governance; Domain 3 observability and improvement | `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/SAP-C02_Security_Observability_Comparison.md`; committed in governance repo `d068a65`; supports the Security Tooling vs Log Archive split recorded in ADR 0005 |
| 2026-07-01 | SAP-C02 practice review blocks 003-006 | Committed and pushed to governance repo | Domains 1-4 practice remediation | `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/exercises/sap-c02-exercise-block-003-review.md` through `sap-c02-exercise-block-006-review.md`; committed and pushed in governance repo `5f6158e`; updated notes include Block 006's answer-distribution quality caveat |

---

## 3. SAP-C02 Domain Coverage

Official SAP-C02 domains:

| Domain | Weight | Status | Evidence required |
|---|---:|---|---|
| Domain 1: Design Solutions for Organizational Complexity | 26% | Partial | Workload IAM, logging, tagging, budget evidence, Organizations membership, selected Cost Allocation Tag activation, governance preflight evidence, Security Tooling vs Log Archive separation, external governance study diagrams, and the first two Lakehouse Workloads OU SCP guardrails exist; Security Tooling account implementation, broader Identity Center model, and enterprise networking remain open |
| Domain 2: Design for New Solutions | 29% | In progress | Lakehouse readiness closure, repository-side Domain 2 consolidation, two 20-question practice blocks, separate tutorial evidence through Lesson 33, and later practice blocks 003-006 are complete locally; practice review and later networking/DR decisions remain open |
| Domain 3: Continuous Improvement for Existing Solutions | 25% | Partial | Parquet, lifecycle, validation, observability, public-access controls, alerting, cost guardrails, separate Lessons 26-33 tutorial hardening evidence, the 217-test tutorial baseline, and OAM vs CloudTrail vs Config study evidence exist; systematic improvement notes and remaining hardening are open |
| Domain 4: Accelerate Workload Migration and Modernization | 20% | Partial | Exercise 002 exposed a rehost-vs-refactor/MGN weak area; later practice blocks show stronger mixed-domain performance but Kinesis/SQS distinction and migration playbook artifacts remain open |

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
│   ├── Log Archive storage-only boundary
│   ├── future Security Tooling account
│   ├── AWS Config aggregation migration target
│   ├── GuardDuty / Security Hub concept
│   ├── OAM / cross-account observability concept
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
| AWS Organizations enabled | Verified | Management account and member accounts verified in `docs/evidence/cost-allocation-tag-activation-20260617.md`; prechange root/account/service-access inventory is recorded in `docs/evidence/domain1-governance-org-inventory-summary-20260621.md`; live OU creation and lakehouse account move are recorded in `docs/evidence/domain1-governance-lakehouse-workloads-ou-change-note-20260621.md` and `docs/evidence/domain1-governance-lakehouse-account-move-change-note-20260622.md` |
| OU structure designed | Partial | Target OU model recorded in `docs/adr/0005-aws-organizations-governance-design.md`; current-to-target placement decision recorded in `docs/planning/domain-1-ou-account-placement-decision-20260621.md`; live evidence now shows `Container Sandbox`, `Lakehouse Workloads OU`, and `Security OU` exist under root, and the lakehouse account has been moved into `ou-gbyf-m6ppfmpq`; see `docs/evidence/domain1-governance-security-ou-change-note-20260622.md` and `docs/evidence/domain1-governance-lakehouse-account-move-change-note-20260622.md` |
| Management account rules documented | Design accepted | Control-plane account rules recorded in `docs/adr/0005-aws-organizations-governance-design.md`; implementation boundary remains future approval |
| Workload account purpose defined | Design accepted | Lakehouse workload and sandbox account boundaries recorded in `docs/adr/0005-aws-organizations-governance-design.md` |
| Security/log archive account design documented | Verified | Target security/log archive boundary recorded in `docs/adr/0005-aws-organizations-governance-design.md`; detailed design recorded in `docs/planning/domain-1-cloudtrail-log-archive-design-20260621.md`; the first bounded baseline/change note is recorded in `docs/evidence/domain1-governance-cloudtrail-log-archive-change-note-20260622.md`; the design-to-implementation boundary is recorded in `docs/planning/domain-1-security-log-archive-account-implementation-boundary-20260622.md`; `Security OU`, the dedicated `Security Log Archive` account, Account Management trusted access, alternate contacts, the dedicated log-archive bucket, and the customer-managed KMS key are now live via `docs/evidence/domain1-governance-cloudtrail-log-archive-storage-change-note-20260624.md`; long-term design now keeps `Security Log Archive` storage-only and moves active delegated security tooling to a future `Security Tooling` account |
| IAM Identity Center access model documented | Partial live evidence | Permission-set candidates and account targets recorded in `docs/adr/0005-aws-organizations-governance-design.md`; detailed matrix recorded in `docs/planning/identity-center-permission-set-matrix-20260619.md`; same-day evidence now confirms one active IAM Identity Center instance, the live management-account admin principal `org-admin-principal` / `[redacted-email]`, the dedicated emergency principal `breakglass-principal` / `[redacted-email]`, and current management/sandbox account assignments in `docs/evidence/domain1-governance-identity-center-current-state-20260625.md`; follow-on live cleanup removed inherited `AdministratorAccess` from `breakglass-principal` by removing its `cloud-lab-aws-admins` group membership in `docs/evidence/domain1-governance-breakglass-group-membership-cleanup-20260702.md`; the broader governance permission-set model remains open |
| Permission sets defined | Partial live evidence | Permission-set matrix recorded in `docs/planning/identity-center-permission-set-matrix-20260619.md`; same-day evidence now confirms two live permission sets, `AdministratorAccess` and `BreakGlassAdmin`, plus the first direct management-account emergency assignment for `breakglass-principal` in `docs/evidence/domain1-governance-identity-center-current-state-20260625.md`; the target `OrganizationAdmin`, `BillingAdmin`, `SecurityAudit`, `LakehouseOperator`, `LakehouseReadOnly`, and later hardening of `BreakGlassAdmin` remain open |
| Break-glass access model documented | Partial live evidence | Break-glass target recorded in ADR 0005 and procedure recorded in `docs/runbooks/break-glass-access-procedure.md`; same-day IAM Identity Center evidence in `docs/evidence/domain1-governance-identity-center-current-state-20260625.md` now distinguishes the documented emergency owner from the currently live management-account admin principal and records that the dedicated break-glass principal exists with MFA plus a management-account emergency permission-set assignment; follow-on cleanup evidence in `docs/evidence/domain1-governance-breakglass-group-membership-cleanup-20260702.md` confirms the emergency user now has no group memberships and retains only the direct `BreakGlassAdmin` management-account path; root MFA for workload account `464975959576` is confirmed in `docs/evidence/domain1-governance-root-mfa-readiness-check-20260702.md`; second Identity Center MFA for `breakglass-principal` is confirmed in `docs/evidence/domain1-governance-breakglass-mfa2-readiness-check-20260703.md`; emergency SMS notification reachability is confirmed in `docs/evidence/domain1-governance-notification-reachability-check-20260703.md`; recovery-code readability is confirmed in `docs/evidence/domain1-governance-recovery-code-readability-check-20260703.md`; light procedural validation is confirmed in `docs/evidence/domain1-governance-breakglass-procedural-validation-20260703.md`; post-use review implementation remains open for any actual emergency use |
| SCP catalogue drafted | Partial live evidence | Accepted SCP catalogue recorded in ADR 0005; example policy files recorded in `docs/policies/scp/`; the first live OU-targeted `DenyLeavingOrganization` attempt, rollback, root policy-type enablement, and successful retry are recorded in `docs/evidence/domain1-governance-deny-leaving-organization-change-note-20260622.md`, `docs/evidence/domain1-governance-enable-scp-root-change-note-20260622.md`, and `docs/evidence/domain1-governance-deny-leaving-organization-attach-success-change-note-20260622.md`; the second live OU-targeted guardrail, `DenyRootUserActions-LakehouseWorkloads`, is recorded in `docs/evidence/domain1-governance-deny-root-user-actions-attach-success-change-note-20260703.md` |
| CloudTrail organization trail design documented | Verified | Organization trail direction recorded in `docs/adr/0005-aws-organizations-governance-design.md`; detailed log archive/KMS/retention/delete-protection design recorded in `docs/planning/domain-1-cloudtrail-log-archive-design-20260621.md`; earlier baseline evidence is recorded in `docs/evidence/domain1-governance-cloudtrail-log-archive-change-note-20260622.md`; fresh post-account baseline evidence is recorded in `docs/evidence/domain1-governance-cloudtrail-management-sts-prechange-20260624.json`, `docs/evidence/domain1-governance-cloudtrail-service-access-prechange-20260624.json`, `docs/evidence/domain1-governance-cloudtrail-list-prechange-20260624.json`, and the paired security-account prechange files; exact policy examples are recorded in `docs/policies/s3-cloudtrail-log-archive-bucket-policy.example.json`, `docs/policies/kms-cloudtrail-log-archive-key-policy.example.json`, and `docs/policies/s3-cloudtrail-log-archive-encryption.example.json`; live storage evidence is recorded in `docs/evidence/domain1-governance-cloudtrail-log-archive-storage-change-note-20260624.md`; live trusted-access, organization-trail, and first delivered log/digest evidence are now recorded in `docs/evidence/domain1-governance-cloudtrail-organization-trail-change-note-20260624.md` |
| AWS Config design documented | Partial live evidence | Organization aggregation direction recorded in `docs/adr/0005-aws-organizations-governance-design.md`; detailed recorder scope, aggregation, rule, and cost-control design recorded in `docs/planning/domain-1-config-guardduty-design-20260621.md`; fresh baseline evidence is recorded in `docs/evidence/domain1-governance-config-*-prechange-20260624.json`, the follow-on lakehouse prechange evidence is recorded in `docs/evidence/domain1-governance-config-lakehouse-*-prechange-20260625.json`, the follow-on security-account prechange evidence is recorded in `docs/evidence/domain1-governance-config-security-*-prechange-20260625.json`, and the follow-on sandbox prechange evidence is recorded in `docs/evidence/domain1-governance-config-sandbox-*-prechange-20260625.json`; exact storage and role-trust policy examples are recorded in `docs/policies/s3-config-log-archive-bucket-policy.example.json`, `docs/policies/kms-config-log-archive-key-policy.example.json`, `docs/policies/s3-config-log-archive-encryption.example.json`, and `docs/policies/iam-config-organization-aggregator-role-trust-policy.example.json`; live storage evidence is recorded in `docs/evidence/domain1-governance-config-log-archive-storage-change-note-20260624.md`; live trusted access, delegated administration, and organization aggregation are recorded in `docs/evidence/domain1-governance-config-organization-aggregation-change-note-20260624.md`; the live management-account recorder rollout is recorded in `docs/evidence/domain1-governance-config-management-recorder-change-note-20260624.md`; the live lakehouse-account recorder rollout is recorded in `docs/evidence/domain1-governance-config-lakehouse-recorder-change-note-20260625.md`; the live security-account recorder rollout is recorded in `docs/evidence/domain1-governance-config-security-recorder-change-note-20260625.md`; the first live organization CloudTrail rule, the sandbox-recorder gap, the management-account `AWSServiceRoleForConfigMultiAccountSetup` blocker, the management-account SLR fix, the temporary sandbox exclusion, and final successful deployment across management, lakehouse, security, and sandbox accounts are recorded in `docs/evidence/domain1-governance-config-org-cloudtrail-rule-change-note-20260625.md`; the live sandbox-account recorder rollout plus the central bucket/KMS policy extension needed to enable it are recorded in `docs/evidence/domain1-governance-config-sandbox-recorder-change-note-20260625.md`; additional Config rules remain open, but the first organization CloudTrail detective control now covers all four intended accounts; accepted future state migrates Config delegated administration and aggregation first into a separate `Security Tooling` account after the root-user emergency-only SCP is live |
| GuardDuty/Security Hub/OAM concept documented | Partial | Security-service sequencing recorded in `docs/adr/0005-aws-organizations-governance-design.md`; detailed GuardDuty delegated-admin and cost-control design plus Security Hub defer/adopt decision recorded in `docs/planning/domain-1-config-guardduty-design-20260621.md`; OAM vs CloudTrail log archive vs AWS Config aggregator study note recorded in `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/SAP-C02_Security_Observability_Comparison.md`; live GuardDuty, Security Hub, and OAM enablement remain open |
| Cost allocation tags defined | Verified | Common Terraform tags exist; selected Billing Cost Allocation Tags were activated from the Organizations management account on 2026-06-17 |
| Budget alarms configured | Partial | A live `$1` managed-workflow AWS Budget with notifications is verified; broader workload/account budget design remains open |

### SCP catalogue

| SCP | Purpose | Status |
|---|---|---|
| Deny disabling CloudTrail | Protect audit evidence | Partial |
| Deny deleting log buckets | Protect log archive | Partial |
| Deny public S3 exposure | Reduce data leakage risk | Partial |
| Deny unapproved regions | Cost/compliance control | Partial |
| Deny root-user actions except emergencies | Reduce blast radius | Live for `Lakehouse Workloads OU` as `DenyRootUserActions-LakehouseWorkloads` / `p-dv2ss5us` |
| Require encryption where feasible | Improve compliance posture | Partial |
| Deny leaving AWS Organization | Prevent governance bypass | Live for `Lakehouse Workloads OU` |

Critical note: **SCPs do not grant permissions.** They define maximum allowed permissions. IAM policies still grant permissions.

Example SCP files now exist in `docs/policies/scp/`. The first bounded
OU-targeted SCP change note records the initial failed attach attempt and
rollback, the root-level policy-type enablement change note records how the
structural blocker was resolved, and the retry change note records the
successful attach to `Lakehouse Workloads OU`:
`docs/evidence/domain1-governance-deny-leaving-organization-change-note-20260622.md`,
`docs/evidence/domain1-governance-enable-scp-root-change-note-20260622.md`,
and
`docs/evidence/domain1-governance-deny-leaving-organization-attach-success-change-note-20260622.md`.
The next narrow SCP candidate, `deny-root-user-actions.example.json`, is now
recorded as prepared with the target emergency owner, contact, notification,
storage, evidence, scope, and reduction values defined, with the current live
IAM Identity Center principal inventory clarified in
`docs/evidence/domain1-governance-identity-center-current-state-20260625.md`,
and follow-on MFA, SMS reachability, and recovery-code readability evidence
recorded, plus light procedural-validation evidence in
`docs/evidence/domain1-governance-breakglass-procedural-validation-20260703.md`.
The live attachment is recorded in
`docs/evidence/domain1-governance-deny-root-user-actions-attach-success-change-note-20260703.md`.

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
| 2026-07-01 | `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/exercises/sap-c02-exercise-block-003-review.md` | Untimed 25 questions | 24/25 | Hybrid network architecture multi-select discipline | Wrong answer logged; recheck every selected service against a stated requirement |
| 2026-07-01 | `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/exercises/sap-c02-exercise-block-004-review.md` | Untimed 25 questions | 25/25 | None identified | Clean pass; keep no-heading exam-style blocks |
| 2026-07-01 | `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/exercises/sap-c02-exercise-block-005-review.md` | Untimed 25 questions | 25/25 | None identified | Clean pass; no-heading format improved scenario parsing |
| 2026-07-01 | `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/exercises/sap-c02-exercise-block-006-review.md` | Untimed 25 questions | 24/25 | Kinesis Data Streams vs SQS FIFO; block quality caveat | Wrong answer logged; drill event-streaming vs queueing decision patterns; treat the score as useful learning evidence but not fully exam-quality because the block had a flawed single-answer distribution |
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
| 2026-07-01 | Hybrid network architecture: private connectivity, many VPCs, centralized routing, and hybrid DNS | Domain 1 / networking | Added Internet Gateway even though the scenario required private routing and hybrid DNS | Use Direct Connect Gateway + Transit Gateway + Route 53 Resolver inbound/outbound endpoints; include Internet Gateway only when public internet access is explicitly required |
| 2026-07-01 | High-throughput replayable event ingestion with per-customer ordering and multiple consumers | Domain 2 / Domain 3 | Chose SQS FIFO wording with an absolute "unlimited throughput" claim instead of the stream-processing pattern | Use Kinesis Data Streams with customer-ID partition keys and independent consumers/enhanced fan-out; reserve SQS FIFO for ordered queueing and deduplication |

---

## 10. Booking Decision Criteria

### Must be true before booking

| Criterion | Status |
|---|---|
| Two timed practice exams at 80%+ OR one 80%+ and one 75–79% with narrow weak areas | Not met: four additional untimed 25-question blocks now show 96%, 100%, 100%, and 96%, but Block 006 has an answer-distribution quality caveat and full timed practice evidence has not started |
| Domain 1 governance notes complete | Partially met: governance preflight, Organizations governance ADR, org inventory evidence, parent mapping, OU/account-placement decision, first approved live OU creation, approved lakehouse account move, SCP examples, first bounded OU-targeted SCP attempt and rollback evidence, root `SERVICE_CONTROL_POLICY` enablement evidence, first and second successful OU-targeted SCP attachment evidence, permission-set matrix, break-glass procedure, logging/security-service design notes, Security Tooling vs Log Archive split, and a governance live-readiness runbook are documented; further approved implementation changes remain open |
| Networking comparison matrix complete | Not met |
| Migration matrix complete | Not met |
| Lakehouse readiness closure complete and documented | Met: core path, encryption, versioning, lifecycle, bucket tags, Billing Cost Allocation Tag activation, IAM, current end-to-end evidence, and stale Phase 1 reconciliation are complete |
| IAM/Organizations/SCP design complete | Partially met: target Organizations, OU, Identity Center, SCP catalogue, SCP examples, and break-glass procedure are documented; root `SERVICE_CONTROL_POLICY` is enabled, `DenyLeavingOrganization` and `DenyRootUserActions-LakehouseWorkloads` are live for `Lakehouse Workloads OU`, but exception tests, assignment decisions, broader rollback planning, and additional implementation evidence remain open |
| Wrong-answer log reviewed twice | Not met: wrong-answer log is current through local practice block 006, but two separate review cycles are not yet evidenced |
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
| June–July | Python/serverless tutorial hardening + lakehouse readiness closure | Closeout complete: lakehouse code, documentation, diagrams, and evidence now agree with two 20-question practice blocks, wrong-answer logging, and separate tutorial evidence through Lesson 33; remaining pre-governance work is carry-forward review |
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
- [x] Record Python/serverless Lessons 26-33 evidence separately from
  lakehouse implementation evidence; latest local suite passed 217 tests and no AWS
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
- [x] Document CloudTrail/log archive design:
  `docs/planning/domain-1-cloudtrail-log-archive-design-20260621.md`.
- [x] Document AWS Config and GuardDuty design, and the Security Hub
  defer/adopt decision:
  `docs/planning/domain-1-config-guardduty-design-20260621.md`.
- [x] Reconcile the Security/Observability posture with the SAP-C02 governance
  study diagrams by adopting the long-term split between storage-only
  `Security Log Archive` and future `Security Tooling`, including OAM as a
  future Security Tooling concern rather than log archive storage:
  `docs/adr/0005-aws-organizations-governance-design.md`,
  `docs/planning/domain-1-cloudtrail-log-archive-design-20260621.md`,
  `docs/planning/domain-1-config-guardduty-design-20260621.md`, and
  `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/SAP-C02_Security_Observability_Comparison.md`.
- [x] Create a governance live-readiness runbook that turns the accepted design
  into bounded change units, read-only evidence capture, rollback checkpoints,
  and validation steps:
  `docs/runbooks/domain-1-governance-live-readiness-runbook.md`.
- [x] Capture the first read-only Organizations inventory evidence and record
  the root/account/service-access state:
  `docs/evidence/domain1-governance-org-inventory-summary-20260621.md`.
- [x] Record the current-to-target OU/account-placement decision from that
  inventory evidence:
  `docs/planning/domain-1-ou-account-placement-decision-20260621.md`.
- [x] Under explicit approval, create `Lakehouse Workloads OU` and record the
  change boundary, prechange state, rollback, validation, and postchange
  evidence:
  `docs/evidence/domain1-governance-lakehouse-workloads-ou-change-note-20260621.md`.
- [x] Under separate explicit approval, move `lakehouse-workload-account` from root into
  `ou-gbyf-m6ppfmpq` and record the change boundary, propagation nuance,
  rollback, validation, and postchange evidence:
  `docs/evidence/domain1-governance-lakehouse-account-move-change-note-20260622.md`.
- [x] Draft the first bounded OU-targeted SCP change note for
  `Lakehouse Workloads OU`, using `DenyLeavingOrganization` as the narrowest
  first guardrail candidate:
  `docs/evidence/domain1-governance-deny-leaving-organization-change-note-20260622.md`.
- [x] Attempt the first bounded OU-targeted SCP live change and record the
  blocker, error evidence, and rollback cleanup when `AttachPolicy` fails
  because root `r-gbyf` is not yet ready for SCP attachment:
  `docs/evidence/domain1-governance-deny-leaving-organization-change-note-20260622.md`.
- [x] Under separate explicit approval, enable `SERVICE_CONTROL_POLICY` for
  root `r-gbyf`, validate that the root now exposes the enabled policy type,
  and record that prerequisite change:
  `docs/evidence/domain1-governance-enable-scp-root-change-note-20260622.md`.
- [x] Retry the OU-targeted `DenyLeavingOrganization` attach after root
  enablement is validated and record the successful policy attachment:
  `docs/evidence/domain1-governance-deny-leaving-organization-attach-success-change-note-20260622.md`.
- [x] Record the next narrow root-user emergency-only SCP as the follow-on
  candidate for `Lakehouse Workloads OU`, and capture the specific blocker that
  prevents safe live attachment today:
  `docs/evidence/domain1-governance-deny-root-user-actions-change-note-20260622.md`.
- [x] Define the break-glass live values for the next root-user emergency-only
  SCP candidate: emergency owner, notification path, evidence location,
  credential storage/recovery location, current scope, and post-use access
  reduction path.
- [x] Capture the first bounded CloudTrail/log archive baseline in the
  management account and record the current no-trail/no-bucket state:
  `docs/evidence/domain1-governance-cloudtrail-log-archive-change-note-20260622.md`.
- [x] Choose the SAP-C02-preferred CloudTrail ownership path and package the
  security/log archive account implementation boundary:
  `docs/planning/domain-1-security-log-archive-account-implementation-boundary-20260622.md`.
- [x] Draft the next bounded `Security OU` live-change package with current
  prechange evidence, rollback, validation, and blast-radius notes:
  `docs/evidence/domain1-governance-security-ou-change-note-20260622.md`.
- [x] Under separate explicit approval, create `Security OU` as the next narrow
  live change unit for the centralized logging path:
  `docs/evidence/domain1-governance-security-ou-change-note-20260622.md`.
- [x] Under separate explicit approval, create the dedicated
  `Security Log Archive` member account and record the durable
  account-creation boundary:
  `docs/evidence/domain1-governance-security-log-archive-account-change-note-20260624.md`.
- [x] Under separate explicit approval, enable `account.amazonaws.com`
  trusted access and set the alternate contacts for account `955659429518`:
  `docs/evidence/domain1-governance-account-management-trusted-access-and-alternate-contacts-change-note-20260624.md`.
- [x] Under separate explicit approval, move `Security Log Archive`
  (`955659429518`) from root `r-gbyf` into `Security OU`
  (`ou-gbyf-mug20ym0`):
  `docs/evidence/domain1-governance-security-log-archive-account-move-change-note-20260624.md`.
- [x] Capture fresh post-account CloudTrail baseline evidence from the
  management account and `Security Log Archive` account, including the current
  `cloudtrail.amazonaws.com` trusted-access gap and the no-bucket/no-CMK
  storage state:
  `docs/evidence/domain1-governance-cloudtrail-management-sts-prechange-20260624.json`,
  `docs/evidence/domain1-governance-cloudtrail-service-access-prechange-20260624.json`,
  `docs/evidence/domain1-governance-cloudtrail-list-prechange-20260624.json`,
  and the paired security-account prechange files.
- [x] Write the exact dedicated-log-archive S3 bucket policy and KMS key policy
  examples for the CloudTrail target design:
  `docs/policies/s3-cloudtrail-log-archive-bucket-policy.example.json` and
  `docs/policies/kms-cloudtrail-log-archive-key-policy.example.json`.
- [x] Package the next narrow `Security Log Archive` storage-step live change
  for the CloudTrail bucket and KMS key:
  `docs/evidence/domain1-governance-cloudtrail-log-archive-storage-change-note-20260624.md`.
- [x] Under separate explicit approval, create the dedicated CloudTrail
  log-archive bucket and customer-managed KMS key in the `Security Log Archive`
  account, enable versioning, Block Public Access, SSE-KMS default encryption,
  and key rotation, and attach the resolved bucket and key policies:
  `docs/evidence/domain1-governance-cloudtrail-log-archive-storage-change-note-20260624.md`.
- [x] Package the follow-on management-account organization-trail live change,
  including the explicit `cloudtrail.amazonaws.com` trusted-access
  prerequisite:
  `docs/evidence/domain1-governance-cloudtrail-organization-trail-change-note-20260624.md`.
- [x] Under separate explicit approval, enable
  `cloudtrail.amazonaws.com` trusted access, create the multi-Region
  organization trail `organization-management-events` from the management
  account, start logging, and capture the first postchange trail and
  bucket-path evidence:
  `docs/evidence/domain1-governance-cloudtrail-organization-trail-change-note-20260624.md`.
- [x] Capture the first AWS Config baseline evidence across the management,
  lakehouse, and security accounts, including the current trusted-access and
  delegated-administrator gaps:
  `docs/evidence/domain1-governance-config-*-prechange-20260624.json`.
- [x] Write the exact dedicated AWS Config archive bucket policy and KMS key
  policy examples for the security-account storage target:
  `docs/policies/s3-config-log-archive-bucket-policy.example.json` and
  `docs/policies/kms-config-log-archive-key-policy.example.json`.
- [x] Write the exact delegated-admin organization-aggregator trust-policy
  example for the AWS Config security-account role:
  `docs/policies/iam-config-organization-aggregator-role-trust-policy.example.json`.
- [x] Package the next narrow `Security Log Archive` storage-step live change
  for the AWS Config bucket and KMS key:
  `docs/evidence/domain1-governance-config-log-archive-storage-change-note-20260624.md`.
- [x] Under explicit approval, create the dedicated AWS Config archive bucket
  and customer-managed KMS key in the `Security Log Archive` account, enable
  Block Public Access, versioning, and SSE-KMS default encryption, apply the
  resolved bucket and key policies, and confirm that no recorder, delivery
  channel, or aggregator was created as part of the storage-only step:
  `docs/evidence/domain1-governance-config-log-archive-storage-change-note-20260624.md`.
- [x] Under explicit approval, enable `config.amazonaws.com` and
  `config-multiaccountsetup.amazonaws.com` trusted access, register
  `Security Log Archive` (`955659429518`) as delegated administrator for both
  service principals, create IAM role
  `aws-config-organization-aggregator-role`, and create aggregator
  `organization-config-aggregator-eu-west-2` in `eu-west-2`, while confirming
  that no recorder, delivery channel, or rule was created as part of the
  control-plane step:
  `docs/evidence/domain1-governance-config-organization-aggregation-change-note-20260624.md`.
- [x] Under explicit approval, create the management-account AWS Config
  service-linked role, create and start the first customer managed recorder and
  delivery channel in `eu-west-2`, trigger a snapshot delivery into the
  security-account archive bucket, and confirm successful delivery plus
  preserved organization aggregation:
  `docs/evidence/domain1-governance-config-management-recorder-change-note-20260624.md`.
- [x] Under explicit approval, create the lakehouse-account AWS Config
  service-linked role, create and start the next customer managed recorder and
  delivery channel in `eu-west-2`, trigger a snapshot delivery into the
  security-account archive bucket, and confirm successful recorder plus
  delivery-channel status without changing the existing organization
  aggregation boundary:
  `docs/evidence/domain1-governance-config-lakehouse-recorder-change-note-20260625.md`.
- [x] Under explicit approval, create the security-account AWS Config
  service-linked role, create and start the final intended customer managed
  recorder and delivery channel in `eu-west-2`, trigger a snapshot delivery
  into the security-account archive bucket, and confirm successful recorder,
  delivery-channel, bucket-object, and preserved aggregation evidence without
  enabling Config rules:
  `docs/evidence/domain1-governance-config-security-recorder-change-note-20260625.md`.
- [x] Under explicit approval, create the first organization AWS Config managed
  rule from the delegated-admin security account using
  `MULTI_REGION_CLOUD_TRAIL_ENABLED`, capture the sandbox-recorder blocker in
  `974893866311`, narrow the rule by excluding that sandbox account for now,
  resolve the management-account
  `AWSServiceRoleForConfigMultiAccountSetup` blocker, retry the same bounded
  deployment, and record final successful deployment across management,
  lakehouse, and security accounts:
  `docs/evidence/domain1-governance-config-org-cloudtrail-rule-change-note-20260625.md`.
- [x] Re-check `org-multi-region-cloudtrail-enabled` until management account
  `349687196588` no longer reports `UPDATE_IN_PROGRESS`, resolve the
  management-account `AWSServiceRoleForConfigMultiAccountSetup` blocker, and
  refresh the evidence:
  `docs/evidence/domain1-governance-config-org-cloudtrail-rule-change-note-20260625.md`.
- [x] Under explicit approval, extend the central Config archive bucket and
  Config KMS key policies to include sandbox account `974893866311`, create
  the sandbox-account AWS Config service-linked role, recorder, and delivery
  channel in `eu-west-2`, trigger a snapshot delivery into the central archive
  bucket, and confirm successful recorder, delivery-channel, and bucket-object
  evidence without yet changing the current organization rule exclusion:
  `docs/evidence/domain1-governance-config-sandbox-recorder-change-note-20260625.md`.
- [x] Update `org-multi-region-cloudtrail-enabled` so sandbox account
  `974893866311` is no longer excluded, then verify successful organization
  rule deployment across management, lakehouse, security, and sandbox:
  `docs/evidence/domain1-governance-config-org-cloudtrail-rule-change-note-20260625.md`.
- [x] Create the dedicated IAM Identity Center break-glass principal
  `breakglass-principal` / `[redacted-email]` and confirm one
  enrolled MFA device in same-day console evidence:
  `docs/evidence/domain1-governance-identity-center-current-state-20260625.md`.
- [x] Under explicit approval, create the dedicated emergency
  `BreakGlassAdmin` permission set with `PT1H` session duration, attach the
  AWS-managed `AdministratorAccess` policy as the first staged implementation,
  and assign it only to management account `349687196588` for
  `breakglass-principal`:
  `docs/evidence/domain1-governance-breakglass-permission-set-change-note-20260625.md`.
- [x] Under explicit approval, remove `breakglass-principal` from
  `cloud-lab-aws-admins` so the emergency user no longer inherits the routine
  management-account `AdministratorAccess` path and retains only direct
  `BreakGlassAdmin`:
  `docs/evidence/domain1-governance-breakglass-group-membership-cleanup-20260702.md`.
- [x] Preserve read-only evidence that the workload account `464975959576` root
  user has MFA assigned through the `emergency@464975959576` authenticator
  entry:
  `docs/evidence/domain1-governance-root-mfa-readiness-check-20260702.md`.
- [x] Preserve console evidence that `breakglass-principal` has a second IAM
  Identity Center authenticator-app MFA device registered on a separate device:
  `docs/evidence/domain1-governance-breakglass-mfa2-readiness-check-20260703.md`.
- [x] Preserve evidence that the active emergency SMS notification path is
  reachable before restrictive root-user guardrails are attached:
  `docs/evidence/domain1-governance-notification-reachability-check-20260703.md`.
- [x] Preserve evidence that the Google backup codes and Microsoft/Outlook
  recovery code are readable in both private recorded storage locations and
  stored in electronic and paper formats:
  `docs/evidence/domain1-governance-recovery-code-readability-check-20260703.md`.
- [x] Record a light procedural validation of the notification, evidence, and
  access-reduction path before any live root-user emergency-only SCP attachment:
  `docs/evidence/domain1-governance-breakglass-procedural-validation-20260703.md`.
- [x] Under explicit approval, create the customer-managed
  `DenyRootUserActions-LakehouseWorkloads` SCP from the repository example,
  attach it only to `Lakehouse Workloads OU`, and record prechange,
  postchange, and rollback evidence:
  `docs/evidence/domain1-governance-deny-root-user-actions-attach-success-change-note-20260703.md`.
- [ ] After the root-user emergency-only SCP live attachment, create a separate
  `Security Tooling` account in `Security OU`, keep `Security Log Archive`
  storage-only, and migrate delegated-admin functions in order: AWS Config
  first, GuardDuty next, and Security Hub only if later adopted.
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
