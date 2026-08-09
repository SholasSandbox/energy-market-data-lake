# SAP-C02 Study Guide: Domain 3 — Continuous Improvement for Existing Solutions

**Last revised:** 2026-08-08

## Series Context & Domain Coverage

**Domain 3 = 25% of scored content.** This guide covers all five task statements:

| Task | Statement | Weight in this guide |
|---|---|---|
| 3.1 | Determine a strategy to improve overall operational excellence | Standard |
| 3.2 | Determine a strategy to improve security | Standard |
| 3.3 | Determine a strategy to improve performance | **Elevated** (Paper 01 miss: Q16) |
| 3.4 | Determine a strategy to improve reliability | Standard |
| 3.5 | Identify opportunities for cost optimization | **Elevated** (Paper 01 miss: Q19) |

Observability (Paper 01 miss: Q17) spans 3.1 and 3.3 and gets its own discriminator section.

**Why Domain 3 is different from Domains 1, 2, and 4 — and why it caught you.** Domains 1 and 2 reward *design* thinking: pick the right architecture from a blank page. Domain 3 rewards *remediation* thinking: an existing workload is broken, slow, insecure, or expensive, and you must fix it **without rebuilding it**. The single most important mental shift:

> **In Domain 3, the migration/rewrite option is almost always a distractor.** "Migrate to DynamoDB", "rewrite as serverless", "move to Aurora" — these solve the problem by replacing the workload, which violates the implied constraint of *continuous improvement* and usually an explicit qualifier ("minimal code changes", "minimal redesign"). The correct answer is nearly always a managed component slotted *into* the existing architecture.

This guide's structure: concept deep-dives per task → the three discriminator tables that decide most Domain 3 questions → distractor taxonomy → recognition shapes → 10 practice questions with full rationale → rejected alternatives → acronym legend.

---

# Task 3.1 — Improve Operational Excellence

## 3.1.1 The telemetry service boundary problem

The Q17 miss was a **control-plane vs data-plane conflation**, and it is one of the most reused traps on the exam. Commit this boundary to memory:

| Service | Records | Answers the question | Is NEVER the answer for |
|---|---|---|---|
| **CloudTrail** | AWS API calls (control plane; data events optionally for S3/Lambda) | "Who called `DeleteTrail` / `PutObject`, from where, when?" | Tracing application requests through services |
| **AWS X-Ray / ADOT** | Application request traces with propagated context | "Where in the request path is latency/error occurring?" | Auditing who changed AWS resources |
| **AWS Config** | Resource configuration state over time + compliance | "What did this security group look like last Tuesday? Is it compliant now?" | Real-time request flow or API attribution |
| **VPC Flow Logs** | IP-level network flow metadata (no payload) | "What talked to what, on which ports, accepted or rejected?" | Application-layer or identity-level questions |
| **CloudWatch Logs/Metrics** | Application and service telemetry | "What is the error rate / what did the app log?" | Configuration history or API audit |

**The test to apply:** if the question says *"trace a user request as it flows across services"* → X-Ray/ADOT, full stop. CloudTrail Lake appearing in the options is bait — it queries *audit events*, and audit events do not carry trace context. Conversely, *"determine who modified the resource"* → CloudTrail; X-Ray in the options is the mirror-image bait.

## 3.1.2 Cross-account observability (the "single pane of glass" pattern)

For "N accounts, one view, least custom development":

- **CloudWatch cross-account observability**: designate a central **monitoring account**, link source accounts (natively or via Organizations for automatic enrolment of new accounts). Metrics, logs, *and traces* become queryable from the monitoring account. This is the modern default answer.
- **X-Ray traces flow into the same monitoring-account view** — so "distributed tracing + single pane across accounts" = instrument with X-Ray/ADOT **+** cross-account observability. Two features, one answer option.
- Legacy patterns that appear as distractors: metric streams to a central account via Firehose, log subscription filters to a central Kinesis stream, self-managed Grafana/Prometheus/Jaeger on EC2. All *work*; all lose to the managed feature on "least development/overhead".
- **Amazon Managed Grafana / Managed Service for Prometheus** are correct only when the question names Prometheus/Grafana ecosystems explicitly (usually EKS-heavy scenarios).

## 3.1.3 Operations automation ladder

Exam scenarios describe manual toil; the answer assigns the right Systems Manager (SSM) capability or event-driven pattern:

| Toil described | Answer |
|---|---|
| "Patch hundreds of instances on a schedule with defined maintenance windows" | **SSM Patch Manager** + maintenance windows |
| "Run the same command/script across a fleet without SSH" | **SSM Run Command** |
| "Codified multi-step operational procedure (restart, snapshot, notify) with approval steps" | **SSM Automation runbooks** |
| "Keep instances in a defined state / enforce agent installed" | **SSM State Manager** |
| "Interactive shell access without bastions, SSH keys, or open port 22" | **SSM Session Manager** (also a Task 3.2 answer) |
| "React to a specific AWS event with remediation" | **EventBridge rule → Lambda/SSM Automation** |
| "Detect non-compliant resource configuration and fix it automatically" | **AWS Config rule + auto-remediation (SSM Automation)** |

**Recognition shape:** "no SSH", "no bastion", "no open inbound ports" → Session Manager. It beats hardened bastions and EC2 Instance Connect whenever auditability (session logging to S3/CloudWatch) is mentioned.

Boundary that must remain explicit: Session Manager can log commands and output
for normal interactive shell sessions. It also supports port forwarding and SSH
tunnelling, but it cannot log the command/content stream inside those encrypted
tunnels. A question requiring command logs plus the ability to forward ports is
therefore describing two available modes, not a simultaneously logged tunnel.

## 3.1.4 Deployment and change improvement

Existing workload, risky manual deployments → the improvement is a pipeline plus a safe deployment strategy (canary/linear with alarm-triggered rollback — CodeDeploy for EC2/ECS/Lambda). Covered in depth in Domain 2 study; in Domain 3 form, the qualifier is usually *"reduce deployment risk without changing the platform"* — so the answer adds CodeDeploy to the existing compute, rather than moving to a new compute service.

---

# Task 3.2 — Improve Security

## 3.2.1 The credential remediation ladder

Paper 01 Q18 (which you got right — this consolidates it):

1. **Credentials for AWS-to-AWS access should not exist.** EC2 → instance profile role; Lambda → execution role; ECS → task role; on-premises servers → **IAM Roles Anywhere** (X.509-based temporary credentials — increasingly tested as the answer to "long-lived keys on on-prem servers").
2. **Credentials that must exist** (third-party APIs, database passwords) → **Secrets Manager** when rotation, cross-account access, or RDS/Redshift native integration is required.
3. **Parameter Store (SecureString)** when it's configuration or a secret with *no rotation requirement* and cost sensitivity (standard tier is free).

**The Secrets Manager vs Parameter Store discriminator:** the word **"rotation"** (or "automatically rotate") in the stem selects Secrets Manager. No rotation requirement + "lowest cost" selects Parameter Store SecureString. A `String` (not SecureString) parameter for a secret is always wrong — plaintext at rest.

## 3.2.2 The detection service discriminator table

The second-highest-yield table in Domain 3. These five are deliberately confusable:

| Service | Detects | Data sources | Stem trigger words |
|---|---|---|---|
| **GuardDuty** | Threats and anomalous behaviour (compromised credentials, crypto-mining, C2 traffic, malware) | CloudTrail, VPC Flow Logs, DNS logs, EKS audit, EBS malware scan | "threat detection", "compromised", "unusual activity", "malicious" |
| **Inspector** | Software vulnerabilities and unintended network exposure | EC2 (SSM agent), ECR images, Lambda | "CVEs", "vulnerability scanning", "unpatched packages", "container image scanning" |
| **Macie** | Sensitive data in **S3** (PII, credentials, financial data) | S3 object content | "discover PII", "sensitive data in buckets", "data classification" |
| **Detective** | Investigation/root-cause of findings after the fact | Graphs built from GuardDuty/CloudTrail/VPC Flow | "investigate the finding", "root cause of the security incident", "visualize related activity" |
| **Security Hub** | Nothing itself — aggregates findings + runs compliance standards checks | Findings from the above + Config | "single view of findings", "CIS/AWS Foundational benchmark", "aggregate across accounts" |

**Composition pattern:** GuardDuty *finds* it, Detective *investigates* it, Security Hub *aggregates* it, EventBridge + Lambda/SSM *remediates* it. Questions often ask for two of these in a Select TWO.

**The preventative-vs-detective trap:** if the stem says access/behaviour "must be **blocked**" or "**prevented**", every detection service above is a distractor — the answer is a policy control (SCP, RCP, endpoint policy, bucket policy, permission boundary — Domain 1 territory deliberately cross-wired into Domain 3 stems). If the stem says "detect", "alert", "identify" — now the table applies.

## 3.2.3 Least privilege and exposure reduction on existing workloads

- **IAM Access Analyzer**: identifies resources shared outside your zone of trust (external access findings) and generates least-privilege policies *from CloudTrail activity* ("right-size this over-permissive role based on what it actually uses" → Access Analyzer policy generation).
- **IMDSv2 enforcement** (`HttpTokens=required`): the answer to SSRF-style credential theft from EC2 metadata. Stem trigger: "application vulnerability allowed retrieval of instance credentials".
- **Encrypt existing unencrypted resources**: unencrypted EBS/RDS cannot be encrypted in place — snapshot → copy-with-encryption → restore/recreate. The distractor claims a checkbox enables encryption in place; for existing volumes/instances it does not.

---

# Task 3.3 — Improve Performance

## 3.3.1 The database performance decision tree (contains the Q16 fix)

An existing relational database is struggling. The exam expects you to identify *which* struggle, because each maps to exactly one managed remediation:

| Symptom in the stem | Remediation | Why the alternatives lose |
|---|---|---|
| "Too many connections", connection churn, **Lambda/serverless clients**, connection storms after failover | **RDS Proxy** | Raising `max_connections`/instance class treats symptom at cost; throttling the app (reserved concurrency) protects the DB by *breaking the app* |
| Read-heavy, reads can tolerate slight staleness, "offload reporting/read traffic" | **Read replicas** (or Aurora replicas) | ElastiCache requires code changes for cache logic; replicas need only a reader endpoint |
| Repeated identical reads, sub-millisecond latency demanded, session data, leaderboards | **ElastiCache** (Redis/Valkey) | Replicas still cost a query round-trip; caching is the only path below ~1 ms |
| DynamoDB + microsecond-to-millisecond reads of hot items, read-intensive | **DAX** | ElastiCache in front of DynamoDB is a code-heavy re-implementation of DAX |
| Failover time itself is the complaint (Multi-AZ failover too slow) | **Aurora** (failover typically completes within 30 seconds when a replica can be promoted) or **RDS Proxy** (preserves and redirects application connections) | Bigger instances don't change failover mechanics; Aurora still needs at least one appropriately placed replica for the fast promotion path |
| Unpredictable/spiky load, "scale database capacity automatically" | **Aurora Serverless v2** | Manual instance resizing has downtime and lag |

**The banked rule from Q16:** *Lambda + relational database + connection errors → RDS Proxy.* It is a connection-string change (satisfies "minimal code changes"), it pools/multiplexes server-side, and it preserves client connections through failover. Reserved concurrency as a "fix" is the **throttle trap** — the option that stops the errors by capping the workload's ability to do its job. Watch for the same trap shape elsewhere: SQS `maxReceiveCount=1`, ASG max=1, API Gateway account-level throttle set low.

## 3.3.2 The caching layer stack

Performance questions often hide "which layer should cache this?":

- **CloudFront** — edge caching of HTTP(S) responses, static or dynamic; the answer when users are *global* and latency is geographic.
- **API Gateway caching** — regional response cache per stage; correct only when the audience is regional (a global audience + edge caching requirement defeats it — Paper 01 Q10).
- **ElastiCache** — application-tier caching of query results/sessions; requires code (cache-aside), so loses "minimal code changes" contests but wins raw-latency ones.
- **DAX** — DynamoDB-specific, API-compatible (minimal code change), read-through/write-through.
- **RDS Proxy is not a cache** — a distractor sometimes implies it accelerates reads; it pools connections, nothing more.

## 3.3.3 Compute and storage performance remediation

- **Compute rightsizing evidence** → **AWS Compute Optimizer** (ML-based recommendations for EC2, ASG, EBS, Lambda memory). "How do we know which instances are over/under-provisioned?" → Compute Optimizer, not Trusted Advisor (coarser) and not CloudWatch alone (data, not recommendations).
- **EBS**: gp2 → **gp3** is simultaneously a performance *and* cost answer (independent IOPS/throughput provisioning, ~20% cheaper). io2 only when >16K IOPS or durability language appears.
- **S3 request performance**: prefix-level parallelism scales automatically (3,500 PUT / 5,500 GET per prefix per second) — the "add random prefixes" answer is outdated; **multipart upload + byte-range fetches** for large objects; **Transfer Acceleration** only for long-haul client uploads to a bucket (it does not increase your link bandwidth — Paper 01 Q21).
- **Lambda performance**: cold starts → **provisioned concurrency** (SnapStart for Java); CPU-bound → raise memory (CPU scales with it). Distractor: "increase timeout" — timeouts change failure behaviour, never speed.
- **Tightly coupled grid/HPC latency**: launch the instances together in a
  **cluster placement group**. Adding nodes or using EC2 Fleet does not control
  physical proximity.
- **ENA versus EFA**: ENA is enhanced conventional IP networking and is already
  used by Nitro instances. EFA adds an OS-bypass path for compatible
  MPI/NCCL/NIXL/Libfabric workloads on supported instances; do not select it as
  a generic fix for an ordinary TCP timeout.

---

# Task 3.4 — Improve Reliability

## 3.4.1 The single-point-of-failure elimination pattern

Paper 01 Q20 (which you got right) is the archetype. Each fragile component has a *configuration-level* HA counterpart — the exam wants the mapping, not a redesign:

| Fragile component | HA remediation (no redesign) |
|---|---|
| Single EC2 instance | **ASG min/desired 1 across multiple AZs** behind an ELB (self-healing + AZ mobility). EC2 auto recovery is the weaker distractor: same host constraints, no AZ escape |
| Single-AZ RDS | **Multi-AZ deployment** (synchronous standby, automatic failover). Read replica is the distractor — asynchronous, manual promotion, wrong tool for HA |
| Single-node ElastiCache | **Replication group with Multi-AZ automatic failover** — replicas must be in *different* AZs |
| Single NAT gateway | **One NAT gateway per AZ**, with per-AZ route tables |
| EFS/S3 | Already multi-AZ (except One Zone classes — which is the trap when they appear under "business-critical" data) |

**Multi-AZ vs read replica is a deliberate confusion pair**: Multi-AZ = availability (synchronous, automatic failover, standby not readable on classic RDS); read replica = read scaling / cross-region (asynchronous, promotion is manual). Stems mixing "improve availability" with a read-replica option are testing exactly this line. (Multi-AZ *DB cluster* deployments blur it — two *readable* standbys — but only select that when the stem demands both HA and read scaling.)

## 3.4.2 Recovery and failure-isolation improvements

- **AWS Backup** — centralized, policy-based backup across services and accounts (backup plans + vaults, cross-region and cross-account copies, vault lock for immutability). The answer to "inconsistent, per-team backup scripts" is an AWS Backup org-level policy, not better scripts.
- **Route 53 ARC zonal shift / zonal autoshift** — shift traffic away from an impaired AZ for ALB/NLB without changing the architecture; the modern answer to "reduce blast radius of a single-AZ impairment on an existing load-balanced app".
- **Health check depth**: ELB health checks should verify the *application* (a real dependency-touching endpoint), not just TCP 80 — "instances marked healthy while the app fails" → fix the health check target/path.
- **Static stability**: pre-provision capacity so failover requires *no* control-plane action (e.g., run each AZ at N+1). Appears in stems as "must not depend on launching new capacity during a failure".

---

# Task 3.5 — Identify Cost Optimization Opportunities

## 3.5.1 The commitment flexibility ladder (Paper 01 Q15 consolidation)

Discount depth and flexibility are inversely related; the stem tells you which end you're allowed:

| Instrument | Flexibility | Relative discount | Select it when the stem says |
|---|---|---|---|
| **Compute Savings Plans** | Any instance family, size, region, OS; includes Fargate & Lambda | Good | "may change instance families/regions", "moving some workloads to containers" |
| **EC2 Instance Savings Plans** | Locked to instance family + region | Better | "standardized on family X in region Y for the term" |
| **Standard RIs** | Family/region locked; capacity reservation possible (zonal) | Best | "guaranteed capacity" language, stable multi-year usage |
| **Convertible RIs** | Exchangeable across families | Between | Rarely correct post-Savings Plans; appears mostly as a distractor |
| **Spot** | None — reclaimable at 2 min notice | Up to ~90% | "fault-tolerant", "stateless", "can be interrupted", "batch/CI" |
| **On-Demand** | Total | None | "unpredictable", "short-lived", "unknown" |

**The deepest-discount trap:** an option offering the biggest percentage (EC2 Instance SP, Standard RI, Spot-for-everything) attached to a workload whose description forbids the lock-in. Read the workload's *certainty*, then pick the instrument — never the reverse. Spot's presence also requires the stem to concede interruptibility; "business-critical, always-on" + Spot = distractor regardless of savings.

## 3.5.2 The S3 storage class decision procedure (contains the Q19 fix)

Two rules decide these questions:

**Rule 1 — access-pattern knowledge picks the warm class:**
- Access pattern **unknown, shifting, or unpredictable** → **Intelligent-Tiering**. This is near-mechanical: the phrase selects the class, because IT has *no retrieval fees* and moves objects automatically — you cannot be wrong about a pattern you didn't predict.
- Access pattern **known infrequent** (predictable, e.g. "accessed once a quarter") → **Standard-IA** (or One Zone-IA *only* if the stem concedes the data is re-creatable/non-critical — One Zone under critical data is always the trap).
- **Frequently accessed** → **Standard**. IA classes on hot data lose money twice: per-GB retrieval fees on every access + 30-day minimum storage duration. Small hot datasets (your Q19 thumbnails) stay in Standard even though it "looks" unoptimized.

**Rule 2 — retrieval-time tolerance picks the Glacier tier:**

| Regulator/business says retrieval within… | Tier | Notes |
|---|---|---|
| Milliseconds | **Glacier Instant Retrieval** | ~68% cheaper than Standard-IA storage, but retrieval fees; minimum 90-day duration |
| Minutes–hours (expedited/standard/bulk) | **Glacier Flexible Retrieval** | 90-day minimum |
| **≤ 12 hours** (standard; bulk ~48h) and cheapest possible | **Glacier Deep Archive** | 180-day minimum; lowest storage cost in S3 |

The Q19 miss decomposed: paying Instant Retrieval prices for a 12-hour tolerance buys speed nobody asked for (over-provisioned durability/latency is a *cost* defect on this exam), and Standard-IA on unknown patterns risks retrieval-fee bleed. **Match the tier to the stated tolerance exactly — no better, no worse.**

Supporting mechanics worth one read: lifecycle policies can transition *and* expire (expire incomplete multipart uploads — a free-money answer that appears in "reduce S3 cost, choose TWO" questions); **Storage Lens** for org-wide S3 usage visibility; minimum storage durations (30d IA / 90d GIR & GFR / 180d GDA) make short-lived objects in cold classes *more* expensive — the trap for temp/staging data.

## 3.5.3 Architecture-level cost leaks (high-frequency Select TWO material)

- **NAT gateway data processing** for traffic to S3/DynamoDB → **gateway VPC endpoints** (free, no NAT processing charge). One of the most repeated cost answers on the exam: "large S3 transfer costs from private subnets" → gateway endpoint, not a bigger NAT.
- **Interface endpoints vs NAT** for other AWS services: endpoints win on security posture; on pure cost it depends on volume — the stem will signal which lens applies.
- **Cross-AZ data transfer** in chatty architectures → AZ-affinity (e.g., disable/enable cross-zone LB deliberately, co-locate consumers), or accept it as an HA cost.
- **CloudFront in front of origin** reduces origin egress (internet-out from S3/EC2 is billed; CloudFront-to-internet rates + caching usually net cheaper for cacheable content).
- **gp2 → gp3**, **Graviton** where the runtime allows, **Lambda memory rightsizing via Compute Optimizer / Power Tuning** — the "safe" modernization cost answers.
- **Idle resource detection** → **Trusted Advisor** (idle/underutilized checks) and **Cost Explorer rightsizing recommendations**; **Cost Anomaly Detection** for "alert us when spend deviates" (detection, not optimization — don't confuse the two roles in Select TWO options).

---

# The Domain 3 Distractor Taxonomy

Name the trap and the option eliminates itself. Every Domain 3 distractor in Paper 01 falls into one of these six:

1. **The Rebuild Trap** — solves the problem by replacing the workload ("migrate to DynamoDB" for a connection-pool problem). Eliminated by any "minimal changes / existing application" language. *(Q16 option C)*
2. **The Throttle Trap** — "fixes" an overload symptom by capping the workload below its required capacity (reserved concurrency = 10). The database stops hurting because the application stopped working. *(Q16 option D — your miss)*
3. **The Control-Plane Conflation** — offers an audit/config service (CloudTrail, Config) for a data-plane question (request tracing, app latency), or vice versa. *(Q17 option D — your miss)*
4. **The Wrong-Temperature Trap** — a storage class or commitment instrument mismatched to the stated access pattern or retrieval tolerance: IA on hot/unknown data, Instant Retrieval against a 12-hour SLA, Deep Archive under live data. *(Q19 option B — your miss)*
5. **The Manual/Heroic Trap** — runbooks, self-managed EC2 stacks (BIND, Jaeger, cron), "document the procedure", "rely on the team to…". Eliminated by "least operational overhead / minimal recurring effort". *(Q18 option E)*
6. **The Detective-for-Preventative Swap** — a monitoring/detection service (Macie, GuardDuty, Config) offered where the stem demands *blocking* — or a preventative control offered where the stem asks only to *identify*. *(Paper 01 Q6 option D)*

---

# Recognition Shapes — Domain 3 Quick Index

| Stem signal | Answer |
|---|---|
| Lambda + RDS + "too many connections" / failover storms | RDS Proxy |
| "Trace a request across services/accounts" | X-Ray/ADOT (+ CloudWatch cross-account observability for the single pane) |
| "Who changed this resource / made this API call" | CloudTrail |
| "What did the configuration look like / is it compliant" | AWS Config |
| "Unknown/changing access patterns" (S3) | Intelligent-Tiering |
| Retrieval within 12 hours, cheapest | Glacier Deep Archive |
| "Automatically rotate" a secret | Secrets Manager |
| Secret/config, no rotation, lowest cost | Parameter Store SecureString |
| "May change instance family/region" + steady usage | Compute Savings Plans |
| "Fault-tolerant / interruptible" batch | Spot |
| "No SSH / no bastion / audited shell access" | SSM Session Manager |
| "Detect threats / compromised credentials" | GuardDuty |
| "Find PII in S3" | Macie |
| "Scan for CVEs / vulnerable packages / images" | Inspector |
| "Investigate the root cause of a finding" | Detective |
| "Aggregate findings, benchmark compliance" | Security Hub |
| Single instance/node/AZ + "recover automatically" | ASG multi-AZ / Multi-AZ RDS / Redis replication group with Multi-AZ |
| "Reduce NAT/S3 transfer costs from private subnets" | Gateway VPC endpoint |
| "Which instances are over-provisioned" | Compute Optimizer |
| "Alert on unusual spend" | Cost Anomaly Detection |
| "Shift traffic away from an impaired AZ" | Route 53 ARC zonal shift |

---
# Practice Questions (Domain 3 Drill — 10 Questions)

Attempt under exam conditions (~29 minutes) before reading the rationale. Questions 1, 4, and 8 deliberately re-test the Paper 01 miss patterns in new clothing.

### Question 1

A reporting application runs on Amazon ECS and connects to an Amazon Aurora PostgreSQL cluster. During month-end processing, task count scales from 10 to 400, and the database begins rejecting connections; after a writer failover, thousands of stale connections cause a 10-minute recovery delay. The team cannot modify the application beyond configuration. Which solution resolves both issues?

**A.** Scale the Aurora writer to the largest available instance class and raise `max_connections`.
**B.** Deploy RDS Proxy in front of the cluster and update the application's connection string to the proxy endpoint.
**C.** Set the ECS service's maximum task count to 50 to keep connections within database limits.
**D.** Migrate the reporting workload to Amazon Redshift Serverless.

### Question 2

A company's security team must be able to (1) detect when EC2 instances begin communicating with known cryptocurrency-mining domains, and (2) investigate the sequence of API activity and network behaviour that preceded any such finding. Which combination meets these requirements? **(Select TWO.)**

**A.** Enable Amazon GuardDuty in all accounts via delegated administrator.
**B.** Enable Amazon Inspector continuous scanning on all EC2 instances.
**C.** Enable Amazon Detective and use its behaviour graphs to investigate findings.
**D.** Enable Amazon Macie automated discovery.
**E.** Create AWS Config rules for cryptocurrency-related configuration changes.

### Question 3

An operations team manually patches 300 EC2 instances monthly by connecting over SSH through a bastion host. Security wants SSH and the bastion eliminated, all interactive access session-logged, and patching automated within approved weekly windows. Which combination meets these requirements? **(Select TWO.)**

**A.** Use SSM Session Manager for interactive access, with session logging to CloudWatch Logs, and remove inbound SSH rules and the bastion.
**B.** Use SSM Patch Manager with patch baselines and maintenance windows to automate patching.
**C.** Replace the bastion with EC2 Instance Connect Endpoint and retain SSH key distribution.
**D.** Schedule a cron job on each instance to run `yum update` weekly.
**E.** Use AWS Config to detect missing patches and email the operations team.

### Question 4

A SaaS application stores three datasets in Amazon S3: (1) 80 TB of customer-uploaded documents, some accessed daily and some untouched for years, with no reliable way to predict which; (2) 300 TB of transaction records that must be kept for 10 years and retrieved within 48 hours for legal discovery, expected at most once every few years; (3) 2 TB of ML feature files read hundreds of times per day by training jobs. Which configuration minimizes cost without violating requirements?

**A.** (1) Intelligent-Tiering; (2) Glacier Deep Archive; (3) S3 Standard.
**B.** (1) Standard-IA; (2) Glacier Flexible Retrieval; (3) Intelligent-Tiering.
**C.** (1) Intelligent-Tiering; (2) Glacier Instant Retrieval; (3) S3 Standard.
**D.** (1) Standard; (2) Glacier Deep Archive; (3) Standard-IA.

### Question 5

A microservices platform on EKS across 8 accounts suffers intermittent latency. Engineers can see per-service CPU metrics but cannot determine which downstream call in a request path is slow, and each account's telemetry is isolated. The company wants the fastest path to cross-account request-path visibility. Which solution meets these requirements?

**A.** Enable VPC Flow Logs in all accounts and centralize them in a Security Lake for latency analysis.
**B.** Instrument services with AWS Distro for OpenTelemetry sending traces to X-Ray, designate a central CloudWatch monitoring account, and link the 8 source accounts.
**C.** Enable CloudTrail Lake across the organization and query API latency by service.
**D.** Deploy Prometheus and Jaeger on a central EKS cluster and configure remote-write from all accounts.

### Question 6

A company runs a stateless image-processing farm (interruptible, checkpointed) and a customer-facing web tier with steady baseline usage that will migrate from x86 EC2 to Fargate within the commitment term. Finance wants maximum savings consistent with these plans. Which purchasing combination is MOST cost-effective?

**A.** EC2 Instance Savings Plans for both workloads.
**B.** Compute Savings Plans sized to the web tier baseline, and Spot capacity for the image-processing farm.
**C.** Standard Reserved Instances for the web tier and On-Demand for the farm.
**D.** Spot Instances for both workloads with capacity rebalancing.

### Question 7

An application in private subnets transfers roughly 60 TB per month to and from Amazon S3 in the same region, currently routed through a NAT gateway. The monthly bill shows significant NAT data-processing charges. Which change eliminates these charges with the LEAST disruption?

**A.** Replace the NAT gateway with a NAT instance on a large EC2 instance.
**B.** Create a gateway VPC endpoint for S3 and add it to the private subnets' route tables.
**C.** Move the application to public subnets with an internet gateway.
**D.** Enable S3 Transfer Acceleration to reduce the transfer volume.

### Question 8

After a security incident, auditors ask three questions: (a) which IAM principal disabled the CloudTrail trail; (b) what the security group's rules were at the time of the incident; and (c) which internal IP addresses the compromised instance communicated with. Which services answer (a), (b), and (c) respectively?

**A.** CloudTrail; AWS Config; VPC Flow Logs.
**B.** AWS Config; CloudTrail; X-Ray.
**C.** CloudTrail; VPC Flow Logs; GuardDuty.
**D.** X-Ray; AWS Config; VPC Flow Logs.

### Question 9

A business-critical API runs on EC2 behind an ALB across three AZs, with an Aurora cluster (writer + two readers spread across the AZs). During a recent single-AZ impairment, the ALB continued routing some traffic into the degraded AZ for several minutes, breaching the API's availability target. The company wants the ability to rapidly move traffic away from an impaired AZ without changing the application or its architecture. Which solution meets this requirement?

**A.** Configure Amazon Route 53 Application Recovery Controller zonal shift for the ALB, allowing traffic to be shifted away from an impaired AZ on demand or automatically.
**B.** Convert the ALB to a Network Load Balancer for faster failover.
**C.** Add a CloudWatch alarm that triggers an ASG instance refresh when AZ latency rises.
**D.** Deploy the entire stack in a second region and fail over with Route 53 when an AZ is impaired.

### Question 10

A DevOps team stores database passwords, third-party API keys requiring 30-day rotation, and ~200 non-sensitive application configuration values. They want AWS-native storage that minimizes cost while meeting the rotation requirement. Which combination meets these requirements? **(Select TWO.)**

**A.** Store the database passwords and rotating API keys in AWS Secrets Manager with automatic rotation configured.
**B.** Store the non-sensitive configuration values in Systems Manager Parameter Store standard-tier parameters.
**C.** Store all values in Secrets Manager to standardize on one service.
**D.** Store the API keys in Parameter Store SecureString with an EventBridge-scheduled Lambda to rotate them.
**E.** Store the configuration values in a DynamoDB table encrypted with a customer managed key.

---

# Answer Key

### Q1 — B
The dual symptom — connection exhaustion at scale-out *and* stale-connection storms after failover — is the RDS Proxy signature; both are solved by server-side pooling and proxy-managed failover, via a connection-string change only. **A** is the pay-more-treat-symptom option (churn and failover storms remain). **C** is the **Throttle Trap**: capping tasks at 50 makes month-end processing miss its window. **D** is the **Rebuild Trap** — a platform migration for a pooling problem, and Redshift is an analytics engine, not a drop-in Postgres target.

### Q2 — A and C
Requirement (1) "detect… communicating with known mining domains" is GuardDuty's crypto-mining finding class verbatim (DNS/flow-log analysis). Requirement (2) "investigate the preceding activity" is Detective's behaviour graph. **B** — Inspector finds CVEs, not active threats. **D** — Macie is S3 data classification. **E** — Config tracks resource configuration, not network behaviour.

### Q3 — A and B
Session Manager removes SSH/bastion and provides the demanded session logging; Patch Manager with maintenance windows automates patching in approved windows. **C** retains SSH keys (explicitly to be eliminated). **D** is the **Manual/Heroic Trap** in cron form — unmanaged, unlogged, no window governance. **E** is the **Detective-for-Preventative Swap** — detection plus email is not automation.

### Q4 — A
(1) "no reliable way to predict" → Intelligent-Tiering, mechanically. (2) 48-hour tolerance, retrieval every few years → Deep Archive (bulk retrieval ~48h, standard ~12h — both inside tolerance at the lowest storage price). (3) hot, small → Standard. **B** — Standard-IA bleeds retrieval fees on the unpredictable dataset; Flexible Retrieval pays for speed the 48-hour SLA doesn't need. **C** — Instant Retrieval is the **Wrong-Temperature Trap** for a 48-hour tolerance (~4–5× Deep Archive storage). **D** — IA on files read hundreds of times daily loses money on every read.

### Q5 — B
Request-path latency across services requires propagated trace context (ADOT/X-Ray); cross-account single pane is CloudWatch cross-account observability. Together they are one option. **A** — flow logs are IP metadata; no request causality. **C** — the **Control-Plane Conflation**: CloudTrail measures AWS API audit events, not inter-service request latency. **D** works and loses: self-managed stack vs "fastest path".

### Q6 — B
The web tier will change *compute platform* (EC2 → Fargate) inside the term — only Compute Savings Plans span EC2 and Fargate, so any instance-locked instrument strands the discount. The farm concedes interruption → Spot. **A** — EC2 Instance SPs don't cover Fargate; the **deepest-discount trap**. **C** — Standard RIs strand on the Fargate move; On-Demand for interruptible batch leaves ~90% on the table. **D** — Spot under the customer-facing steady tier violates its availability character.

### Q7 — B
Gateway endpoints for S3 are free, in-region, and remove the NAT data path entirely; a route-table change is the least-disruption bar. **A** swaps managed NAT for self-managed NAT — costs remain, toil increases. **C** is a security regression to fix a billing line. **D** — Transfer Acceleration is for long-haul client uploads and *adds* fees; same-region VPC→S3 traffic gains nothing.

### Q8 — A
(a) "Which principal called `StopLogging`" is CloudTrail by definition. (b) point-in-time resource configuration is AWS Config's timeline. (c) IP-level communication records are VPC Flow Logs. Every other option misassigns at least one — note **C**'s subtlety: GuardDuty would *alert* on suspicious flows but is not the queryable record of which IPs communicated; Flow Logs are the evidence, GuardDuty is the detector.

### Q9 — A
"Move traffic away from an impaired AZ, no architecture change" is the zonal shift feature description — ARC zonal shift (and zonal autoshift for automatic response) acts on the existing ALB. **B** — changing load balancer type doesn't create AZ-evacuation capability and NLB/ALB serve different layers. **C** — instance refresh replaces instances; it does not stop the LB routing into the impaired AZ. **D** — a full second region is the **Rebuild Trap** scaled up: multi-region DR to solve a single-AZ traffic-steering problem.

### Q10 — A and B
Rotation requirement → Secrets Manager (A). Non-sensitive config at 200 values → Parameter Store standard tier, free (B). **C** — Secrets Manager charges per secret per month; putting 200 non-sensitive values there fails "minimize cost". **D** — hand-rolling rotation with EventBridge+Lambda rebuilds Secrets Manager's native feature (Manual/Heroic Trap in serverless clothing). **E** — DynamoDB for config is custom infrastructure where a purpose-built free tier exists.

---

# Rejected Alternatives (Guide-Level)

Decisions embedded in this guide, and why the alternatives lost — recorded per series discipline:

1. **Organizing by service catalogue (rejected) vs by task statement + discriminator tables (chosen).** Domain 3 questions are symptom-first, not service-first; a service catalogue reproduces documentation you already have, while symptom→remediation tables reproduce the exam's own decision structure. The three Paper 01 misses were all failures to map a symptom to a service boundary, not failures of service knowledge.

2. **Covering all five tasks equally (rejected) vs elevating 3.3 and 3.5 (chosen).** Your Paper 01 evidence localizes the gap to performance remediation and storage/commitment economics; equal coverage would spend your revision hours re-reading what 7/7 and 5/5 domain scores already demonstrate. Observability got a dedicated discriminator section for the same evidentiary reason.

3. **A second full mixed paper now (rejected) vs deep-dive then targeted drill (chosen).** A mixed paper re-samples strong domains at 75% of its length — low signal density per hour. The 10-question drill above is the verification instrument; a second full mixed paper belongs *after* it, as the confirmation that Domain 3 no longer drags the compensatory score.

4. **Memorizing storage prices (rejected) vs decision rules (chosen).** Prices drift; the exam tests *relative* economics (retrieval fees, minimum durations, tolerance matching). The two S3 rules and the commitment ladder are stable against pricing changes; figures quoted (e.g. ~4–5× GIR vs GDA) are order-of-magnitude anchors, not values to recite.

---

# Acronym Legend

| Acronym | Expansion |
|---|---|
| ADOT | AWS Distro for OpenTelemetry |
| ARC | (Route 53) Application Recovery Controller |
| ASG | Auto Scaling group |
| CVE | Common Vulnerabilities and Exposures |
| DAX | DynamoDB Accelerator |
| ELB / ALB / NLB | Elastic / Application / Network Load Balancer |
| GDA / GFR / GIR | Glacier Deep Archive / Flexible Retrieval / Instant Retrieval |
| IA | Infrequent Access |
| IMDSv2 | Instance Metadata Service version 2 |
| IT | (S3) Intelligent-Tiering |
| PII | Personally identifiable information |
| RI | Reserved Instance |
| SP | Savings Plan |
| SSM | AWS Systems Manager |
| SSRF | Server-side request forgery |
