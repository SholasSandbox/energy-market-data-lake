# SAP-C02 Readiness Tracker

**Owner:** Shola  
**Created:** 2026-06-12  
**Target exam:** AWS Certified Solutions Architect – Professional, SAP-C02  
**Target attempt window:** Late November to mid-December 2026  
**Booking decision date:** 2026-11-15  
**Weekly capacity assumption:** 10–12 focused hours while not working  
**Controlling principle:** SAP-C02 is the steering architecture. The Energy Data Lakehouse is the practical case study. Everything else must support exam readiness, lakehouse credibility, or job-market positioning.

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

| Week starting | Target hours | Actual hours | Build hours | Study hours | Practice hours | Notes |
|---|---:|---:|---:|---:|---:|---|
| 2026-06-15 | 10–12 |  |  |  |  | Programme reset + lakehouse phase 1 |
| 2026-06-22 | 10–12 |  |  |  |  | Lakehouse MVP |
| 2026-06-29 | 10–12 |  |  |  |  | Lakehouse MVP |
| 2026-07-06 | 10–12 |  |  |  |  | Lakehouse MVP completion |
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
| 2026-10-12 | 10–12 |  |  |  |  | Containers compressed |
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
| Domain 1: Design Solutions for Organizational Complexity | 26% | Not started | Organizations, SCPs, IAM Identity Center, central logging, networking, cost visibility |
| Domain 2: Design for New Solutions | 29% | In progress | Lakehouse MVP, serverless workflow, storage/data choices, resilience design |
| Domain 3: Continuous Improvement for Existing Solutions | 25% | Not started | Cost, performance, reliability, security, observability improvements |
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

## 4. Energy Lakehouse Build Status

### Target architecture

```text
Energy Data Lakehouse
│
├── S3 raw zone
├── S3 curated zone
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

### MVP checklist

| Item | Status | Evidence |
|---|---|---|
| S3 raw bucket created | Not started |  |
| S3 curated bucket created | Not started |  |
| Bucket naming standard defined | Not started |  |
| Versioning decision documented | Not started |  |
| Encryption model defined | Not started |  |
| Glue Data Catalog created | Not started |  |
| Glue ETL job converts raw to Parquet | Not started |  |
| Athena can query curated data | Not started |  |
| IAM role for Glue least privilege | Not started |  |
| IAM role for Athena/query access | Not started |  |
| CloudWatch logging enabled | Not started |  |
| Cost tags applied | Not started |  |
| Architecture diagram created | Not started |  |

### Lakehouse scope boundaries

| Must have before exam | Nice to have | Defer |
|---|---|---|
| S3 raw/curated | Lake Formation | UI/dashboard |
| Glue ETL | EventBridge schedule | AI orchestration |
| Parquet | Step Functions orchestration | Deep REMIT workflow |
| Athena | DynamoDB metadata | Complex API |
| IAM | Basic data quality checks | Multi-region deployment |
| KMS |  |  |
| CloudWatch |  |  |
| CloudTrail/Config design |  |  |
| Cost tags |  |  |

---

## 5. Governance Build Status

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
| Cost allocation tags defined | Not started |  |
| Budget alarms configured | Not started |  |

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
| Lakehouse MVP complete and documented | Not met |
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
| June–July | Lakehouse MVP + serverless core | S3, Glue, Parquet, Athena, IAM basics working/documented |
| August | IAM, Organizations, SCPs, logging, governance | OU/SCP/logging/IAM design complete |
| September | Networking, hybrid connectivity, resilience | TGW/PrivateLink/DX/VPN/DR comparison artifacts complete |
| October | Migration, modernization, containers, cost | Migration/cost/container artifacts complete; first full practice exam |
| November | Practice exams and remediation | Booking decision based on timed scores |
| December | Exam attempt | Attempt only if readiness criteria are met |

---

## 13. Parking Lot

Use this to capture attractive distractions without acting on them.

| Idea | Why attractive | Decision | Revisit date |
|---|---|---|---|
| AI orchestration for lakehouse | Useful later for portfolio story | Defer | After SAP-C02 |
| Deep EKS | Interesting but not critical | Defer | After SAP-C02 |
| UI dashboard | Portfolio polish | Defer | After SAP-C02 |
| Complex REMIT workflow | Domain-relevant but large | Defer | After SAP-C02 |

---

## 14. Acronym Legend

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
