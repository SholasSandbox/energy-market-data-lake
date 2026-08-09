# SAP-C02 Mental Model: The Four Reference Diagrams

**Last revised:** 2026-08-08

*A single-page reference. Every SAP-C02 scenario question lives somewhere inside one of these four diagrams. The point of this document is not to teach the services — the other study guides do that — but to give you four pictures you can hold simultaneously in your head and ask, when reading any scenario, "which piece of which diagram is this?"*

---

## Compact legend

The legend prioritises abbreviations that are easy to confuse across the four
diagrams. The companion study guides contain the fuller service explanations.

### Governance and identity

| Acronym or term | Meaning | Quick context |
|---|---|---|
| **AD** | Active Directory | Enterprise directory that can act as an identity source. |
| **IAM** | Identity and Access Management | AWS service for identities, roles, and permissions. |
| **IdC** | IAM Identity Center | Central workforce access across AWS accounts and applications. |
| **JML** | Joiners, Movers, Leavers | Identity lifecycle for provisioning and deprovisioning access. |
| **OU** | Organizational Unit | Account grouping and policy-attachment boundary in AWS Organizations. |
| **RCP** | Resource Control Policy | Organization guardrail that limits access to supported resources. |
| **SAML** | Security Assertion Markup Language | Federation standard used between an identity provider and AWS. |
| **SCIM** | System for Cross-domain Identity Management | Standard for automated user and group provisioning. |
| **SCP** | Service Control Policy | Organization guardrail that caps permissions for principals. |

### Multi-account networking

| Acronym or term | Meaning | Quick context |
|---|---|---|
| **DC** | Data centre | On-premises site connected to AWS. |
| **DNS** | Domain Name System | Name resolution within AWS and across hybrid networks. |
| **DX** | Direct Connect | Dedicated private network connection into AWS. |
| **DXGW** | Direct Connect Gateway | Global construct connecting DX to virtual private gateways or Transit Gateways. |
| **GWLB** | Gateway Load Balancer | Transparent insertion point for network appliances. |
| **IGW** | Internet Gateway | VPC component that enables internet connectivity. |
| **NAT GW** | Network Address Translation Gateway | Managed outbound internet egress for private subnets. |
| **PHZ** | Private Hosted Zone | Route 53 namespace visible only to associated VPCs. |
| **RAM** | Resource Access Manager | Cross-account resource-sharing service. |
| **TGW** | Transit Gateway | Regional hub for transitive VPC and hybrid routing. |
| **VIF** | Virtual Interface | Logical interface carried over a Direct Connect connection. |
| **VPC** | Virtual Private Cloud | Logically isolated AWS network. |
| **VPN** | Virtual Private Network | Encrypted tunnel used for hybrid or backup connectivity. |

### Multi-region resilience

| Acronym or term | Meaning | Quick context |
|---|---|---|
| **ALB** | Application Load Balancer | Layer-7 load balancer for application traffic. |
| **AMI** | Amazon Machine Image | Launch template image used to recreate compute. |
| **ASG** | Auto Scaling group | Maintains and scales a defined group of compute instances. |
| **AZ** | Availability Zone | Isolated infrastructure location within an AWS Region. |
| **CRR** | Cross-Region Replication | Asynchronous replication of S3 objects between Regions. |
| **DR** | Disaster Recovery | Recovery from site- or Region-scale failure. |
| **RPO** | Recovery Point Objective | Maximum acceptable data-loss window. |
| **RTO** | Recovery Time Objective | Maximum acceptable service-restoration time. |

### Security and observability

| Acronym or term | Meaning | Quick context |
|---|---|---|
| **KMS** | Key Management Service | AWS service for managing encryption keys. |
| **MFA** | Multi-Factor Authentication | Additional authentication factor beyond a password. |
| **OAM** | Observability Access Manager | CloudWatch cross-account observability sharing. |
| **OCSF** | Open Cybersecurity Schema Framework | Normalised security-event schema used by Security Lake. |
| **PII** | Personally Identifiable Information | Sensitive personal data requiring protection. |
| **SIEM** | Security Information and Event Management | Platform for central security-event analysis. |
| **SOC** | Security Operations Center | Team or function that monitors and responds to threats. |
| **TLS** | Transport Layer Security | Encryption for data in transit. |

---

## The four orthogonal questions

The diagrams below answer four different questions about the same architecture. They are deliberately not the same picture redrawn — they are four projections of one system from four different angles.

| Diagram | Axis | Question it answers |
|---|---|---|
| **1. Governance & Identity** | *Authority* — who can do what | How is permission expressed and constrained across many accounts? |
| **2. Multi-Account Networking** | *Space* — how packets travel | How does traffic flow between accounts, Regions, and on-prem? |
| **3. Multi-Region Resilience** | *Time and failure* — how it survives | How does the architecture behave when something fails? |
| **4. Security & Observability** | *Visibility* — how it is observed and audited | How does the system reveal what is happening inside it? |

If you can place every scenario into one of these four frames within five seconds, your exam pace problem largely solves itself.

---

## Diagram 1 — Governance and Identity

*The "who can do what" picture. Reread before any question that mentions accounts, permissions, federation, or compliance boundaries.*

```
┌─────────────────────────────────────────────────────────────┐
│                    AWS Organization                         │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Management Account (billing, Organizations, IdC,    │   │
│  │  SCP/RCP admin; no workload runtime)                 │   │
│  └──────────────────────────────────────────────────────┘   │
│         │                                                   │
│   ┌─────┴─────┬─────────┬─────────┬─────────┐               │
│   ▼           ▼         ▼         ▼         ▼               │
│ Security  Infra    Workloads  Sandbox   Suspended           │
│   OU       OU         OU        OU         OU               │
│              │                                              │
│              ▼   ◄──── SCPs (cap principal actions)         │
│         Prod OU  ◄──── RCPs (resource-side cap; supported   │
│                              services/resources only)       │
│              │                                              │
│         Member accts                                        │
│                                                             │
│  IAM Identity Center (in mgmt or delegated admin account):  │
│   • Identity source: Okta/Entra/AD/built-in                 │
│   • Permission sets pushed as IAM roles into member accts   │
│   • Group assignments to accounts/OUs                       │
│   • Access portal: user → account → permission set          │
│   • Access keys in portal = temporary role credentials      │
│                                                             │
│  Within each account:                                       │
│   • AWSReservedSSO_* roles materialised from permission sets│
│   • IAM roles for workloads (EC2, Lambda, EKS, ECS)         │
│   • Resource-based policies (S3, KMS, etc.)                 │
│   • Permission boundaries for delegated admin               │
└─────────────────────────────────────────────────────────────┘
```

### What to see
- **OUs are the policy attachment surface**, not accounts. If a scenario says "apply to all production accounts, current and future," the answer attaches the policy at the OU.
- **SCPs cap what principals in target accounts can do; RCPs cap who can access your resources.** Direction matters; RCP coverage is limited to supported services and resource types, so verify that coverage before selecting it.
- **SCPs do not affect users or roles in the management account.** Tested constantly.
- **Permissions are granted by IAM policies and permission sets; guardrails narrow them.** SCPs and RCPs set maximums, but they do not grant access by themselves.
- **Permission sets are design artefacts; IAM roles are runtime artefacts.** Identity Center turns an assignment into temporary access through an IAM role in the target account.
- **Identity Center is the front door for humans; IAM roles are the front door for workloads.** Long-lived IAM users for either is almost always the wrong answer.
- **Break-glass is a separate emergency path, not a different kind of IAM user.** A user such as `emergency-admin` is an Identity Center user; `BreakGlassAdmin` is the permission set assigned to the account.
- **Root is last resort, not normal break-glass.** Prefer normal Identity Center access, then a dedicated emergency permission set, then root-user recovery only if the first two paths fail.

### Recognition shapes
- "Across the org, prevent X" → **SCP** at the OU or root.
- "Block external principals from accessing our data" → **RCP**.
- "Federate 25,000 employees from Entra ID across 60 accounts" → **Identity Center + SAML + SCIM**.
- "Access portal shows account + permission set + Access keys" → **Identity Center temporary role credentials**, not permanent IAM user keys.
- "Emergency admin path for Organizations recovery" → **dedicated Identity Center user + short-session break-glass permission set in the management account**.
- "Self-service IAM role creation without escalation" → **permission boundary**.
- "Third-party SaaS access to my account" → **IAM role + trust policy + External ID**.

---

## Diagram 2 — Multi-Account Networking

*The "how packets travel" picture. Reread before any question involving VPCs, connectivity, DNS, or hybrid traffic.*

```
              On-prem DC #1            On-prem DC #2
                  │                         │
              Direct Connect            Direct Connect
                  │                         │   (redundant location)
                  ▼                         ▼
        ┌─────────────────────────────────────────────┐
        │   Direct Connect Gateway (global control    │
        │   plane; associated connectivity is priced) │
        └─────────────────────────────────────────────┘
                            │
                  (Transit VIF / Private VIF)
                            │
        ┌───────────────────┴──────────────────┐
        │      Network Services Account        │
        │  ┌────────────────────────────────┐  │
        │  │  Transit Gateway (hub)         │  │
        │  │  + Route tables (segmentation) │  │
        │  └────────────────────────────────┘  │
        │  ┌────────────────────────────────┐  │
        │  │  Shared Services VPC:          │  │
        │  │  • Route 53 Resolver in/out    │  │
        │  │  • PHZs (or Route 53 Profiles) │  │
        │  │  • Centralised PrivateLink     │  │
        │  │    interface endpoints         │  │
        │  └────────────────────────────────┘  │
        │  ┌────────────────────────────────┐  │
        │  │  Inspection VPC (optional):    │  │
        │  │  • AWS Network Firewall  OR    │  │
        │  │  • GWLB + 3rd-party appliances │  │
        │  └────────────────────────────────┘  │
        │  ┌────────────────────────────────┐  │
        │  │  Egress VPC: NAT GW, IGW       │  │
        │  └────────────────────────────────┘  │
        │                                      │
        │  TGW shared to OUs via AWS RAM       │
        └──────────────────────────────────────┘
              │                  │
       ┌──────┴──┐         ┌─────┴────┐
       ▼         ▼         ▼          ▼
   Prod-VPC  Prod-VPC  NonProd-VPC  Sandbox-VPC
   (acct A)  (acct B)  (acct C)    (acct D)
```

### What to see
- **A centralized Network Services account is a common landing-zone pattern**: shared connectivity is owned centrally and shared *outwards* via AWS RAM. A distributed model can also be valid when isolation, autonomy, or cost requires it.
- **The TGW is regional**; multi-Region means either TGW peering or Cloud WAN.
- **DNS, central endpoints, inspection, and egress are separate VPCs** inside the Network Services account, each doing one job. The exam rewards this separation; mixing roles into one VPC is usually a wrong-answer signal.
- **Direct Connect is the hybrid spine; VPN is the resilience layer**, terminated on the same TGW.
- **PrivateLink is for service exposure, not network connectivity.** If a scenario says "expose one API to many consumers without joining networks," the answer is PrivateLink endpoint service, not TGW.

### Recognition shapes
- "12+ VPCs, transitive routing, hybrid" → **TGW** in Network Services account, shared via RAM.
- "Global, multi-Region, policy-driven WAN" → **Cloud WAN**.
- "Resolve on-prem names from VPCs and vice versa" → **Resolver outbound + inbound endpoints** in Shared Services VPC, rules shared via RAM.
- "Block S3 traffic from leaving AWS backbone" → **S3 Gateway endpoint** (free, same Region).
- "All east-west and egress must be inspected" → **Inspection VPC + Network Firewall + TGW appliance mode**.
- "99.99% hybrid SLA" → **two DX at two locations + VPN backup**, all on the same TGW.

---

## Diagram 3 — Multi-Region Resilience and Disaster Recovery

*The "how it survives" picture. Reread before any question that mentions RTO, RPO, Region failure, business continuity, or "the second Region."*

```
┌──────────────────────────────────────────────────────────────────────┐
│                         GLOBAL SERVICES                              │
│  Route 53 (DNS) ─ IAM ─ Organizations ─ CloudFront ─ Global Accel.   │
│                                                                      │
│  Route 53 health checks + routing policies:                          │
│  failover / weighted / latency / geolocation / multi-value           │
└──────────────────────────────────────────────────────────────────────┘
              │                                            │
              ▼                                            ▼
┌────────────────────────────────┐     ┌────────────────────────────────┐
│   PRIMARY REGION               │     │   SECONDARY (DR) REGION        │
│   e.g. eu-west-1 (Ireland)     │     │   e.g. eu-west-2 (London)      │
│                                │     │                                │
│   ┌──────────────────────┐     │     │     ┌──────────────────────┐   │
│   │  Compute tier        │     │     │     │  Compute tier        │   │
│   │  (ALB + ASG +        │     │     │     │  (sized per DR tier) │   │
│   │   EC2/ECS/Lambda)    │     │     │     │                      │   │
│   └──────────────────────┘     │     │     └──────────────────────┘   │
│                                │     │                                │
│   ┌──────────────────────┐     │     │     ┌──────────────────────┐   │
│   │  Aurora Global DB    │ ── asynchronous replication ───►       │   │
│   │  primary writer      │     │     │     │  read replica        │   │
│   │                      │     │     │     │  (failover path      │   │
│   │                      │     │     │     │   must be tested)    │   │
│   └──────────────────────┘     │     │     └──────────────────────┘   │
│                                │     │                                │
│   ┌──────────────────────┐     │     │     ┌──────────────────────┐   │
│   │  DynamoDB Global     │ ◄── active-active multi-master ──►     │   │
│   │  Table (read+write)  │     │     │     │  (read+write)        │   │
│   └──────────────────────┘     │     │     └──────────────────────┘   │
│                                │     │                                │
│   ┌──────────────────────┐     │     │     ┌──────────────────────┐   │
│   │  S3 source bucket    │── CRR (async) ─────────────►           │   │
│   │                      │     │     │     │  destination bucket  │   │
│   └──────────────────────┘     │     │     └──────────────────────┘   │
│                                │     │                                │
│  AMIs ──── EC2 Image Builder / AMI copy ────► AMIs                    │
│  KMS keys ──── multi-Region keys (same key ID) ────► KMS keys         │
│  Secrets ──── Secrets Manager cross-Region replication ────► Secrets  │
│  CloudFormation StackSets ───── deploy in parallel ──────► same stack │
└────────────────────────────────┘     └────────────────────────────────┘

DR TIER SPECTRUM (choose per workload; cost rises and RTO/RPO improve left → right)

  Backup & Restore  ◄──► Pilot Light  ◄──► Warm Standby  ◄──► Multi-Site
  RTO: hours+            RTO: ~10 min       RTO: minutes       RTO: seconds
  RPO: hours             RPO: minutes       RPO: seconds       RPO: ~zero
  Cost: lowest           Cost: low          Cost: medium       Cost: highest
  Compute in DR:         Compute in DR:     Compute in DR:     Compute in DR:
   none (rebuild)         minimal (data      scaled-down        full capacity
                          replicated, core   running             running
                          off)
```

### What to see
- **Global services are not tied to one workload Region.** Route 53, IAM, Organizations, CloudFront, and Global Accelerator can support failover, but their own dependencies, quotas, and recovery procedures still need consideration. Do not make Regional recovery depend on an untested control-plane action.
- **The DR tier you pick is a cost/recovery trade-off, not a technical decision.** Every scenario tells you the RTO/RPO; that constrains which of the four tiers can possibly be the right answer. Always eliminate tiers that violate the stated RTO/RPO *before* comparing the remaining options.
- **Data replication has its own latency budget.** Aurora Global Database and DynamoDB Global Tables are asynchronous and typically low-latency, while S3 CRR is asynchronous. Actual RPO depends on observed replication lag and the selected failover path. If RPO is zero, asynchronous replication alone cannot satisfy it.
- **Compute follows data.** Without an AMI in the DR Region, an Auto Scaling group, and a launch template, the data being there doesn't help. Pilot Light = data ready, compute off. Warm Standby = both running, scaled down.
- **Failover is a Route 53 decision** in most patterns: health check fails on primary, DNS shifts to secondary. Global Accelerator does the same at the IP layer with faster convergence.
- **KMS multi-Region keys solve a real problem.** A normal KMS key is regional; data encrypted with it cannot be decrypted in another Region. Related multi-Region keys share key material and key ID, but each key remains regional; replicated ciphertext, grants, and application configuration still need to support the recovery design.

### Recognition shapes
- "RTO 1 hour, RPO 1 hour, lowest cost" → **Pilot Light**.
- "RTO 5 minutes, RPO seconds" → **Warm Standby** at minimum, likely **Multi-Site Active/Active**.
- "RTO 24 hours, RPO 4 hours, minimise spend" → **Backup & Restore**.
- "Active in two Regions, both serving writes" → **DynamoDB Global Tables** (or app-level conflict resolution on Aurora Global DB with the new writer-forwarding capabilities).
- "Encrypt with one key, read in two Regions" → **KMS multi-Region keys**.
- "Deploy the same stack identically across N Regions" → **CloudFormation StackSets**.
- "Resilience posture review" → **AWS Resilience Hub** (newer; runs assessments against your stated RTO/RPO).

---

## Diagram 4 — Security and Observability

*The "how the system is observed and audited" picture. Reread before any question involving logging, monitoring, threat detection, compliance evidence, incident investigation, or "the security team needs visibility."*

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          AWS Organization                               │
│                                                                         │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │  MANAGEMENT ACCOUNT                                            │     │
│  │  • CloudTrail Organization Trail (one trail covers all accts)  │     │
│  │  • Registers delegated administrators for security services    │     │
│  │  • Organizations / SCP / Identity Center control plane         │     │
│  │  • Normal admin path + separate break-glass path; no workloads │     │
│  └────────────────────────────────────────────────────────────────┘     │
│                                                                         │
│  ┌──────────────────── SECURITY OU ──────────────────────────────┐      │
│  │                                                               │      │
│  │  ┌──────────────────────────────────────────────────────────┐ │      │
│  │  │  LOG ARCHIVE ACCOUNT          (write-mostly, audit reads)│ │      │
│  │  │  ┌─────────────────────────────────────────────────────┐ │ │      │
│  │  │  │  Immutable S3 log buckets:                          │ │ │      │
│  │  │  │   • CloudTrail org trail destination                │ │ │      │
│  │  │  │   • Config snapshots and history                    │ │ │      │
│  │  │  │   • VPC Flow Logs                                   │ │ │      │
│  │  │  │   • Route 53 Resolver query logs                    │ │ │      │
│  │  │  │   • Network Firewall logs                           │ │ │      │
│  │  │  │   • ELB / CloudFront access logs                    │ │ │      │
│  │  │  │   • Lake Formation / Athena / Redshift audit logs   │ │ │      │
│  │  │  │                                                     │ │ │      │
│  │  │  │  Tamper protections (defence in depth):             │ │ │      │
│  │  │  │   • S3 Object Lock (compliance or governance mode)  │ │ │      │
│  │  │  │   • Customer-managed KMS key                        │ │ │      │
│  │  │  │   • Bucket policy denying delete and policy change  │ │ │      │
│  │  │  │   • SCP at Security OU denying bucket modification  │ │ │      │
│  │  │  └─────────────────────────────────────────────────────┘ │ │      │
│  │  └──────────────────────────────────────────────────────────┘ │      │
│  │                                                               │      │
│  │  ┌──────────────────────────────────────────────────────────┐ │      │
│  │  │  SECURITY TOOLING ACCOUNT (delegated administrator)      │ │      │
│  │  │                                                          │ │      │
│  │  │  Detection / aggregation:                                │ │      │
│  │  │   • Security Hub        — single pane of findings        │ │      │
│  │  │   • GuardDuty           — threat detection (org-wide)    │ │      │
│  │  │   • Amazon Detective    — incident investigation         │ │      │
│  │  │   • Config Aggregator   — resource inventory + rules     │ │      │
│  │  │   • IAM Access Analyzer — external access + unused       │ │      │
│  │  │   • Macie               — S3 PII / sensitive data        │ │      │
│  │  │   • Audit Manager       — compliance evidence collection │ │      │
│  │  │                                                          │ │      │
│  │  │  Query and analytics over logs:                          │ │      │
│  │  │   • Security Lake       — OCSF-format data lake          │ │      │
│  │  │   • CloudTrail Lake     — managed audit lake, SQL        │ │      │
│  │  │                                                          │ │      │
│  │  │  Response:                                               │ │      │
│  │  │   • EventBridge → Lambda / SSM Automation                │ │      │
│  │  │   • Cross-account roles for remediation in workload accts│ │      │
│  │  │   • Forward findings to SIEM / SOC via Kinesis Firehose  │ │      │
│  │  └──────────────────────────────────────────────────────────┘ │      │
│  └───────────────────────────────────────────────────────────────┘      │
│                          ▲       ▲                                      │
│              (logs flow) │       │ (findings flow)                      │
│                          │       │                                      │
│  ┌──────── WORKLOADS / INFRASTRUCTURE / SANDBOX OUs ──────────────┐     │
│  │                                                                │     │
│  │  Organization-wide coverage, with service-specific choices:    │     │
│  │   • CloudTrail / Config / GuardDuty can be centrally extended  │     │
│  │     through Organizations; confirm Region, onboarding, and     │     │
│  │     exclusion behaviour for each service                       │     │
│  │   • VPC Flow Logs, DNS query logs → Log Archive S3             │     │
│  │   • App + OS logs → CloudWatch Logs                            │     │
│  │   • Findings → Security Hub (in Security Tooling)              │     │
│  │                                                                │     │
│  │  Cross-account observability via CloudWatch OAM:               │     │
│  │   • Dedicated Monitoring account = OAM sink                    │     │
│  │   • Source accounts share metrics, logs, traces, App Insights  │     │
│  │   • Source telemetry remains in source accounts; supported     │     │
│  │     data is queried through the OAM link                       │     │
│  └────────────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────────┘
```

### What to see
- **Two security accounts, not one.** Log Archive is *write-mostly* for the org and *read-only* for auditors and forensics. Security Tooling is where humans and automation *work*. Mixing them violates separation of duties and is a frequent exam distractor.
- **Logs flow inward to Log Archive; findings flow inward to Security Tooling.** Two parallel pipelines with different security postures. Don't confuse the flows.
- **Configuration flows outward** from the management account (org trail, delegated admin registrations) and from Security Tooling (Security Hub controls, GuardDuty config, Config rules).
- **The management account stays a control plane.** It owns Organizations, SCP, IAM Identity Center, and delegated-admin registration decisions; it should not host lakehouse runtime workloads or become the normal security-operations workspace.
- **Break-glass is emergency control-plane access.** It should be a dedicated Identity Center user and permission set with MFA, short session duration, clear evidence, and post-use review, not a routine administrator shortcut.
- **Immutability is layered when retention requirements call for it.** Object Lock + customer-managed KMS key + bucket policy + SCP form a strong defence-in-depth target pattern. Object Lock can be enabled on a new or existing general-purpose bucket, but the bucket must use Versioning. Enabling Object Lock permanently makes the bucket capable of retaining locked object versions; choose it where governance or compliance retention requires immutable storage.
- **Delegated administration is the modern AWS pattern.** Management account should not be doing day-to-day security operations; delegate to Security Tooling. Older study material that says "run Security Hub in the management account" is out of date.
- **Useful newer services:**
  - **CloudTrail Lake** — managed audit data lake with SQL queries and configurable retention. Reduces the need to build Athena-on-S3 yourself.
  - **Security Lake** — purpose-built security data lake in OCSF format; the standard answer when a scenario says "feed our SIEM in a normalised schema."
  - **CloudWatch OAM** — cross-account observability without copying logs. The right answer when the question asks for "central dashboards across many accounts without log replication."
  - **IAM Access Analyzer Unused Access** — finds unused IAM roles, users, and permissions for least-privilege tightening.

### Recognition shapes
- "Centralise audit logs across the org with immutable retention" → **CloudTrail org trail → Log Archive S3 with Object Lock**.
- "Single pane of glass for security findings across many accounts" → **Security Hub with delegated admin in Security Tooling**.
- "Need to detach or change an SCP during recovery" → **management-account-capable Identity Center or Organizations delegation path**, not a workload-account-only admin.
- "Detect anomalous IAM activity or compromised credentials" → **GuardDuty** (org-wide, delegated admin).
- "Discover where PII lives in S3" → **Macie**.
- "Continuous compliance evidence for SOC 2 / PCI / HIPAA" → **Audit Manager + Config conformance packs**.
- "Find IAM roles and permissions nobody uses" → **IAM Access Analyzer unused access**.
- "Investigate a security incident across services" → **Amazon Detective**.
- "Feed our SIEM in a standard normalised format" → **Security Lake (OCSF)**.
- "Query 18 months of audit data with SQL" → **CloudTrail Lake**.
- "Central CloudWatch dashboard across 30 accounts without copying logs" → **CloudWatch OAM**.

### Lakehouse-specific mapping (directly relevant to what you're building)

| Lakehouse layer | Security & observability anchors |
|---|---|
| **Bronze (raw ingestion)** | Selective CloudTrail S3 data events for object-level audit; KMS-CMK encryption at rest; bucket policies denying public access and requiring TLS; VPC Flow Logs on ingestion subnets. Add S3 server access logs only when their access-log semantics are specifically required. |
| **Silver (curated / conformed)** | Lake Formation tag-based access control with full audit logging; Glue job runs logged to CloudWatch; Glue Data Catalog access logged via CloudTrail; Macie scans for PII at curation boundary. |
| **Gold (consumption / serving)** | Athena query logs to S3 and CloudWatch; Redshift audit logging enabled; Lake Formation principal access reports; row- and column-level access decisions logged. |
| **Across all layers** | Security Hub aggregating findings if adopted; optional GuardDuty protection plans based on the threat model and cost; IAM Access Analyzer monitoring for buckets shared outside the org; selected Config rules or conformance packs verifying required encryption controls and exceptions. |

**Build-time traps to avoid as you wire this up:**
- Storing CloudTrail logs in the same account that generated them — an account compromise can erase its own audit trail. **Fix:** logs go to Log Archive in the Security OU.
- Treating Object Lock as a universal default — it is a consequential retention control, not a generic encryption or backup setting. **Fix:** where immutable retention is required, enable Object Lock on a versioned new or existing general-purpose bucket, choose governance or compliance mode deliberately, and combine it with tightly controlled access and key management.
- Enabling CloudTrail data events for every S3 bucket — cost explosion that quickly dwarfs the management-event bill. **Fix:** selectively enable data events for sensitive prefixes only (e.g., the gold/serving layer).
- Mixing security findings and security responses in the same account — blast radius if that account is compromised. **Fix:** findings live in Security Tooling; responses execute via cross-account roles or EventBridge into workload accounts.
- Treating Macie as a one-shot scan — it's a continuous capability. **Fix:** schedule recurring jobs on the bronze and silver buckets, and trigger Macie via EventBridge when new prefixes appear.

---

## How the four diagrams interlock

A realistic SAP-C02 scenario typically pulls from all four at once. The skill is *decomposing* the scenario into the four views, answering each independently, then recombining.

**Worked example.** *"A retail bank operates 35 AWS accounts in one Organization. Customer-facing services in eu-west-1 must meet a 1-hour RTO and 15-minute RPO in the event of Region failure. Employees authenticate via Entra ID. On-prem core banking systems must reach AWS over private connectivity. Auditors require all cross-account access to be logged in an immutable archive, with quarterly compliance evidence."*

- **Diagram 1 (governance):** Identity Center federated to Entra ID with permission sets pushed to all 35 accounts; SCPs at the workloads OU restrict to approved Regions and deny disabling of security services.
- **Diagram 2 (networking):** TGW in a Network Services account, shared via RAM; Direct Connect at two DX locations with a transit VIF and DXGW; Route 53 Resolver inbound and outbound endpoints in a Shared Services VPC for hybrid DNS.
- **Diagram 3 (resilience):** Choose Pilot Light or Warm Standby from the 1-hour RTO and operational-readiness requirement; separately design replication to meet the 15-minute RPO. Aurora Global Database for the relational stores; S3 Cross-Region Replication; KMS multi-Region keys; Route 53 failover routing.
- **Diagram 4 (security and observability):** CloudTrail organisation trail to a Log Archive bucket in the Security OU with Object Lock and customer-managed KMS; Config Aggregator and Security Hub in a Security Tooling account as delegated administrators; Audit Manager configured with the bank's compliance frameworks to produce quarterly evidence packs.

Each diagram contributed independent design decisions; together they form the answer. No SAP-C02 question expects you to invent — every right answer is an assembly of these standard pieces in the proportions the scenario demands.

---

## How to use this document during prep

1. **Print it.** Have it physically next to you during practice questions. Force yourself to point at the diagram the question lives in before reading the options.
2. **Re-derive each diagram from memory weekly.** The act of drawing them — not reading them — is what embeds the mental model. Aim to redraw all four on a blank sheet in under fifteen minutes by exam week.
3. **Annotate each diagram with one piece from your Lakehouse portfolio.** Where does governance live? Where is the network ingress? What's the DR posture for the curated zone? Where do Lake Formation audit logs land? This is how the portfolio becomes interview-defensible — you can speak to every layer of all four diagrams using your own artifact as the evidence.
4. **When a practice question feels confusing, stop and identify which diagram you're in.** Most confusion comes from trying to answer a Diagram 4 question with Diagram 1 thinking, or treating a Diagram 3 problem as if it were Diagram 2.

---

## Trade-offs worth recording for each diagram

Following the practice you've adopted across your study guides, the rejected alternatives for each diagram are worth capturing explicitly:

- **Diagram 1:** Why not flat IAM users with cross-account roles? — Doesn't scale beyond a handful of accounts; no central JML automation; no group-based assignment; no SCIM provisioning.
- **Diagram 2:** Why not VPC peering and per-account VPNs? — Pairwise count explodes; no transitivity; operational sprawl on the on-prem side; no central inspection point.
- **Diagram 3:** Why not multi-AZ alone? — Multi-AZ protects against AZ failure, not Region failure. Regulatory and business-continuity requirements increasingly demand cross-Region resilience that multi-AZ cannot provide, and Region-wide service disruptions, while rare, have happened.
- **Diagram 4:** Why not CloudTrail in each account locally? — No tamper resistance, no central view, breaks separation of duties (an account admin could disable their own audit trail). Why not let the management account run Security Hub and GuardDuty? — AWS's modern guidance is delegation to a dedicated Security Tooling account; the management account should be minimally used. Why not CloudWatch Logs as the primary archive? — Cost at scale, retention limits, harder to apply tamper protections than S3 plus Object Lock.
