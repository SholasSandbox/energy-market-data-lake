<!-- markdownlint-disable MD013 -->

# AWS Skill Builder SAP-C02 Answer-Difference Audit

**Last revised:** 2026-08-09

**Local-only answer-bearing source:** `aws-skill-builder-sap-c02-assessment-full-answer-set-20260809.xlsx` (intentionally Git-ignored)
**Scope:** every question where the learner selection differs from the exported AWS answer

## Result

The workbook contains 75 questions and exactly 30 selection-versus-key differences.
The extraction reconciles with the assessment totals of 45 correct and 30
incorrect.

The independent audit produces three classifications:

| Classification | Count | Questions |
|---|---:|---|
| AWS key stands | 28 | 5, 8, 11–13, 19–21, 23, 25–26, 41, 43, 45–48, 50–51, 53–54, 58–59, 64, 67, 72–73, 75 |
| Exported key is dated; learner answer is correct under current AWS behaviour | 1 | 7 |
| Exported key is dated; learner answer remains wrong, but a different option is now best | 1 | 10 |

This means the workbook score remains useful as historical assessment evidence,
but Questions 7 and 10 must not be memorized from the export literally.

## Control check: Question 74

Question 74 is not in the mismatch set: the learner selected `A,E`, which is
also the exported AWS key. That selection is sound. Use MGN to rehost the
non-modernizable Windows server, and use SCT plus DMS for the heterogeneous Db2
to managed-relational-database migration. My earlier `A,D` recommendation was
wrong because it treated rehosting the Db2 VM as preferable despite the explicit
managed-service requirement.

## Detailed mismatch audit

### Q5 — Grid-computing network timeouts

- **Learner:** `A` — recreate the instances in an EC2 Fleet.
- **AWS key:** `D` — recreate them in a cluster placement group.
- **Why AWS marks D:** adding or fleet-launching ordinary instances does not fix
  east-west latency. A cluster placement group places instances close together
  within one Availability Zone and is the least-change latency intervention
  when a reusable AMI already exists.
- **Audit verdict:** **Key stands.** EFA is also an HPC technology, but the stem
  asks for the least operational effort and already supplies an AMI-based
  relaunch path.
- **Takeaway:** distinguish a **capacity problem** from a **node-to-node latency
  problem**. Fleet size solves the former; cluster placement solves the latter.
- **AWS reference:** <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/placement-groups.html>

### Q7 — Five-year S3 write-once retention

- **Learner:** `C` — enable Versioning and Object Lock on the existing bucket,
  then use five-year compliance-mode retention.
- **Exported AWS key:** `B` — create a new Object Lock bucket and move the data.
- **Why the export marks B:** its rationale says Object Lock could be enabled
  only when a bucket was created. Compliance mode is correctly chosen because
  even the root user cannot shorten or bypass the retention period.
- **Audit verdict:** **The key is dated; C is now valid.** AWS has supported
  enabling Object Lock on an existing bucket since November 2023. Enabling a
  default retention rule affects new object versions; existing versions require
  explicit retention, often through S3 Batch Operations.
- **Takeaway:** memorize the durable rule—**compliance mode for absolute WORM**—
  but use current AWS behaviour: Object Lock can be enabled on an existing
  bucket.
- **AWS reference:** <https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-configure.html>

### Q8 — Mixed and unknown S3 access patterns

- **Learner:** `C` — lifecycle all objects to S3 Glacier Flexible Retrieval after
  180 days.
- **AWS key:** `B` — use S3 Intelligent-Tiering and activate Archive Access at
  180 days.
- **Why AWS marks B:** access is mixed and cannot be identified reliably during
  the first six months. Intelligent-Tiering adapts per object; Archive Access
  standard retrieval normally completes in 3–5 hours, inside the six-hour limit.
- **Audit verdict:** **Key stands.** A blanket lifecycle rule cannot optimize the
  objects that become cold before day 180 and treats all objects identically.
- **Takeaway:** **unknown or object-specific access pattern** is the strongest
  Intelligent-Tiering cue; verify that the selected archive tier meets the
  restore-time ceiling.
- **AWS reference:** <https://docs.aws.amazon.com/AmazonS3/latest/userguide/intelligent-tiering-overview.html>

### Q10 — Scheduled availability of Level 3-protected keys

- **Learner:** `B` — use KMS and schedule key-policy removal and restoration.
- **Exported AWS key:** `C` — recreate a Multi-AZ CloudHSM cluster from backup
  during business hours.
- **Why the export marks C:** it treats FIPS 140-2 Level 3 as a CloudHSM-only
  requirement and uses cluster deletion/recreation to stop hourly HSM charges.
- **Audit verdict:** **Both the learner answer and exported key should be
  rejected under current service capabilities.** Current KMS HSMs are FIPS
  140-3 Security Level 3 validated. Of the offered answers, `D`—scheduled
  `DisableKey` and `EnableKey`—is now the managed, highly available and
  cost-effective control. Editing a key policy changes authorization, not the
  cryptographic state of the key.
- **Takeaway:** separate **who may call KMS** from **whether the KMS key is
  enabled**. Use policy for authorization; use key state for scheduled
  cryptographic unavailability.
- **AWS reference:** <https://docs.aws.amazon.com/kms/latest/developerguide/data-protection.html>

### Q11 — Tightly coupled HPC networking

- **Learner:** `D,E,F` — PV AMI, single AZ and EFA.
- **AWS key:** `B,E,F` — disable hyperthreading, use one AZ and EFA-capable
  instances.
- **Why AWS marks B,E,F:** the workload needs low-latency node communication and
  high network throughput. EFA and single-AZ placement directly support that;
  PV virtualization is obsolete and not the performance requirement. Many HPC
  workloads benefit from one thread per physical core.
- **Audit verdict:** **Key stands, with nuance.** Hyperthreading should be
  benchmarked for a real workload, but it is the defensible exam choice over a
  PV AMI.
- **Takeaway:** for tightly coupled HPC, think **single AZ + placement-aware
  compute + EFA**, then consider disabling simultaneous multithreading when the
  application benefits.
- **AWS reference:** <https://docs.aws.amazon.com/wellarchitected/latest/high-performance-computing-lens/compute.html>

### Q12 — Cross-AZ shared files for two application copies

- **Learner:** `D` — independent EBS volumes synchronized by DataSync.
- **AWS key:** `C` — Regional EFS mounted by both EC2 instances.
- **Why AWS marks C:** both servers must read and update one coherent file
  namespace. Regional EFS supplies concurrent NFS access and multi-AZ data
  availability. Periodic copying between EBS volumes does not provide shared
  filesystem semantics or safe concurrent updates.
- **Audit verdict:** **Key stands.** EBS Multi-Attach is also not a cross-AZ
  shared-filesystem solution.
- **Takeaway:** **multiple hosts concurrently update the same files** points to
  EFS or FSx, not replication between block volumes.
- **AWS reference:** <https://docs.aws.amazon.com/efs/latest/ug/how-it-works.html>

### Q13 — Cost model for a website and restartable video analysis

- **Learner:** `B` — reserved baseline and On-Demand burst, but memory-optimized
  R instances for both tiers.
- **AWS key:** `C` — retain suitable T instances for the website; cover its
  baseline with Reserved Instances and burst with On-Demand; run queued,
  restartable video analysis on diversified compute-optimized Spot capacity.
- **Why AWS marks C:** the cost instruments and instance families match the
  workload shapes without an unnecessary platform migration.
- **Audit verdict:** **Key stands.** The products in the question are dated, but
  the decision rule remains valid.
- **Takeaway:** buy commitment for the **steady baseline**, keep unpredictable
  frontend bursts **On-Demand**, and use **Spot for interruptible queued work**;
  choose instance family from the workload bottleneck.
- **AWS reference:** <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-spot-instances.html>

### Q19 — Reusing common SCP restrictions

- **Learner:** `A` — remove Recruiting accounts from the organization, rebuild
  the OU, and invite the accounts back.
- **AWS key:** `D` — create Recruiting as a child OU under HR and move the
  accounts internally.
- **Why AWS marks D:** the child OU inherits HR's common SCPs and retains its own
  additional restrictions. Accounts can be moved between OUs without leaving
  the organization.
- **Audit verdict:** **Key stands.** Removing accounts creates needless billing,
  governance and re-invitation risk.
- **Takeaway:** encode **common controls on the parent OU** and **additional
  restrictions on the child OU**; move accounts within the organization.
- **AWS reference:** <https://docs.aws.amazon.com/organizations/latest/userguide/move_account_to_ou.html>

### Q20 — AWS Batch with a required custom AMI

- **Learner:** `C` — managed Fargate capacity.
- **AWS key:** `D` — managed EC2 compute environment using Spot and the custom
  AMI.
- **Why AWS marks D:** Fargate does not accept an EC2 custom AMI. AWS Batch can
  manage an EC2 compute environment that uses an AMI override; restartable,
  monthly work is a strong Spot candidate.
- **Audit verdict:** **Key stands.** A custom AMI keeps some guest-OS lifecycle
  responsibility, but Batch still manages fleet scaling and scheduling.
- **Takeaway:** **custom AMI is an EC2 boundary**. “No server management” cannot
  override a technical requirement that Fargate cannot satisfy.
- **AWS reference:** <https://docs.aws.amazon.com/batch/latest/userguide/managed_compute_environments.html>

### Q21 — Managed intelligent contact centre

- **Learner:** `C,D,F` — Connect, Lex and Alexa for Business.
- **AWS key:** `B,C,D` — Lambda, Connect and Lex.
- **Why AWS marks B,C,D:** Connect supplies calls and contact flows, Lex performs
  speech recognition and intent handling, and Lambda queries or updates the
  company's business applications.
- **Audit verdict:** **Key stands.** Alexa for Business is not the integration
  layer for the call-centre workflow.
- **Takeaway:** map requirements component by component: **Connect = contact
  centre, Lex = conversational intent, Lambda = backend integration**.
- **AWS reference:** <https://docs.aws.amazon.com/connect/latest/userguide/contactflow.html>

### Q23 — DynamoDB cost reduction with repeated hot-key reads

- **Learner:** `A` — DAX, auto scaling and Savings Plans.
- **AWS key:** `C` — DAX plus provisioned capacity and DynamoDB auto scaling.
- **Why AWS marks C:** DAX absorbs repeated reads for the limited hot-key set,
  while provisioned capacity can lower cost for a recurring workload. Savings
  Plans do not purchase DynamoDB table throughput.
- **Audit verdict:** **Key stands.** For a sharp known midnight load, scheduled
  capacity can complement auto scaling in a real design, but no offered option
  says that.
- **Takeaway:** DAX solves **DynamoDB read latency/hot-read load**; capacity mode
  solves **table pricing and throughput**. Do not apply EC2 Savings Plans to
  DynamoDB.
- **AWS reference:** <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DAX.html>

### Q25 — Rotating a shared RDS credential

- **Learner:** `C` — Secrets Manager alternating-users rotation.
- **AWS key:** `A` — Secrets Manager single-user rotation.
- **Why AWS marks A:** moving hardcoded credentials to Secrets Manager fixes the
  failure and centralizes retrieval. Single-user rotation is the least-change
  strategy when the stem does not demand uninterrupted availability during the
  rotation window.
- **Audit verdict:** **Key stands.** Alternating users is a valid higher-
  availability pattern, but it adds setup and privilege requirements.
- **Takeaway:** default to **single-user rotation for least effort**; choose
  alternating users only when the stem explicitly values rotation availability
  over simplicity.
- **AWS reference:** <https://docs.aws.amazon.com/secretsmanager/latest/userguide/rotation-strategy.html>

### Q26 — Auto Scaling when CPU does not track demand

- **Learner:** `C` — scheduled scaling.
- **AWS key:** `B` — dynamic scaling on ALB request count per target.
- **Why AWS marks B:** peaks are unexpected and CPU has shown no correlation.
  `RequestCountPerTarget` measures the load each target must absorb and can drive
  target tracking dynamically.
- **Audit verdict:** **Key stands.** Predictive or scheduled scaling depends on
  recurring, forecastable demand, which the stem explicitly denies.
- **Takeaway:** select a scaling metric that tracks the **actual bottleneck**;
  do not default to CPU or schedules when the evidence contradicts them.
- **AWS reference:** <https://docs.aws.amazon.com/autoscaling/ec2/userguide/as-scaling-target-tracking.html>

### Q41 — WorkSpaces with on-premises AD credentials

- **Learner:** `D` — deploy AD Connector on premises.
- **AWS key:** `B` — deploy AD Connector in the WorkSpaces VPC and proxy to the
  on-premises directory over Direct Connect.
- **Why AWS marks B:** AD Connector is an AWS Directory Service proxy. It does
  not store or cache user credentials; authentication remains with the existing
  AD, enabling SSO to domain resources.
- **Audit verdict:** **Key stands.** The connector endpoints belong in AWS
  subnets, not in the data centre.
- **Takeaway:** **AD Connector lives in AWS but identity truth remains on
  premises**; ensure resilient DX/VPN reachability and required AD ports.
- **AWS reference:** <https://docs.aws.amazon.com/workspaces/latest/adminguide/manage-workspaces-directory.html>

### Q43 — Durable Redis-compatible database migration

- **Learner:** `B` — MemoryDB, but use MGN to migrate both application and
  database.
- **AWS key:** `C` — MemoryDB Multi-AZ, MGN for the application servers, and an
  exported Redis snapshot in S3 to seed MemoryDB.
- **Why AWS marks C:** MGN rehosts servers; it is not a logical Redis-to-managed-
  database migration mechanism. MemoryDB can restore supported external `.rdb`
  snapshots from S3 and provides durable Redis-compatible storage.
- **Audit verdict:** **Key stands.** Confirm snapshot compatibility and the
  single-database restriction during implementation.
- **Takeaway:** separate **server migration** from **data-engine migration**;
  use MGN for hosts and the target database's native import path for data.
- **AWS reference:** <https://docs.aws.amazon.com/memorydb/latest/devguide/snapshots-restoring.html>

### Q45 — Standardizing and enforcing organization tags

- **Learner:** `C` — Config required-tags rules plus automated remediation.
- **AWS key:** `A` — tag policies, Resource Groups compliance review, service-
  specific correction, and SCP guardrails for supported create operations.
- **Why AWS marks A:** tag policies define standardized keys and values;
  compliance views find existing deviations. A Config rule is detective and
  remediation is not the same as preventing future creation.
- **Audit verdict:** **Key stands, with a current-service nuance.** Tag-policy
  enforcement can reject noncompliant supplied values for supported resource
  types, but basic enforcement does not make an omitted tag appear. SCPs remain
  useful where request-tag condition keys are supported.
- **Takeaway:** **tag policy = vocabulary/compliance; SCP = preventive
  guardrail; Config = detective/remediation**. Check per-service tag-on-create
  support.
- **AWS reference:** <https://docs.aws.amazon.com/organizations/latest/userguide/orgs_manage_policies_tag-policies-best-practices.html>

### Q46 — Consolidating security findings

- **Learner:** `B` — Audit Manager.
- **AWS key:** `A` — Security Hub integrated with Organizations and a delegated
  administrator/member model.
- **Why AWS marks A:** Security Hub aggregates findings from GuardDuty, Macie,
  IAM Access Analyzer and other products. Audit Manager collects evidence for
  assessments rather than serving as the findings console.
- **Audit verdict:** **Key stands as the least-operations service choice, but the
  option is incomplete for a current multi-Region rollout.** Configure a home
  Region and linked Regions through central configuration/cross-Region
  aggregation.
- **Takeaway:** **Security Hub = consolidated findings and posture; Audit
  Manager = audit evidence and assessment reports**.
- **AWS reference:** <https://docs.aws.amazon.com/securityhub/latest/userguide/start-central-configuration.html>

### Q47 — Fault-tolerant compute and cache

- **Learner:** `B,D` — ASG plus two fixed extra EC2 instances.
- **AWS key:** `B,E` — ASG across AZs plus Redis OSS with Multi-AZ automatic
  failover.
- **Why AWS marks B,E:** the ASG provides native elasticity and replacement for
  the application tier. Redis replication groups provide the requested
  automatic cache failover; Memcached and fixed extra nodes do not.
- **Audit verdict:** **Key stands.** A cache can be disposable, but the question
  explicitly requires fault tolerance and native automated failover.
- **Takeaway:** when the stem says **automatic cache failover**, choose
  Redis/Valkey replication, not Memcached.
- **AWS reference:** <https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/Replication.Redis-RedisCluster.html>

### Q48 — Removing a single-AZ egress dependency

- **Learner:** `A` — add a second internet gateway.
- **AWS key:** `B` — one NAT gateway per AZ with each private subnet routed to
  its local NAT gateway.
- **Why AWS marks B:** an internet gateway is already a horizontally scaled,
  Regional VPC component. One NAT gateway remains an AZ-scoped dependency for
  both private subnets.
- **Audit verdict:** **Key stands.** The design also avoids unnecessary cross-AZ
  traffic and charges during normal operation.
- **Takeaway:** **IGW is Regional; NAT gateway is AZ-scoped**. Multi-AZ private
  egress normally uses a NAT gateway in each AZ.
- **AWS reference:** <https://docs.aws.amazon.com/vpc/latest/userguide/vpc-nat-gateway.html>

### Q50 — Low-volume private cross-Region S3 access

- **Learner:** `C` — replicate the complete dataset to a new regional bucket.
- **AWS key:** `A` — use an S3 interface endpoint in the bucket Region and reach
  its private IPs across inter-Region VPC peering.
- **Why AWS marks A:** the remote access volume is low and the existing dataset
  is large. An interface endpoint supports access from a VPC in another Region
  over peering or Transit Gateway without duplicating the dataset.
- **Audit verdict:** **Key stands.** Use the endpoint-specific S3 DNS name and
  account for endpoint processing plus inter-Region data-transfer charges.
- **Takeaway:** gateway endpoints are VPC/Region-local; **S3 interface endpoints
  extend private access across peering, TGW and hybrid links**. Compare their
  usage cost with replication volume.
- **AWS reference:** <https://docs.aws.amazon.com/AmazonS3/latest/userguide/privatelink-interface-endpoints.html>

### Q51 — PostgreSQL DR with a managed 30-second RPO

- **Learner:** `D` — RDS Multi-AZ standby in another Region.
- **AWS key:** `B` — Aurora PostgreSQL Global Database with primary and secondary
  Regions and managed RPO.
- **Why AWS marks B:** RDS Multi-AZ standby placement is within one Region.
  Aurora Global Database supplies the managed cross-Region topology and RPO
  control with minimal application change from PostgreSQL.
- **Audit verdict:** **Key stands.** A plain cross-Region RDS read replica does
  not offer the stated managed-RPO setting.
- **Takeaway:** **Multi-AZ is Regional HA; global database/read replica is
  cross-Region DR**. Match the mechanism to the stated RPO control.
- **AWS reference:** <https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-global-database-disaster-recovery.html>

### Q53 — Automated pilot-light failover

- **Learner:** `D` — latency routing with Lambda-driven database promotion and
  scale-out.
- **AWS key:** `C` — Route 53 health check and failover routing plus automation
  to promote the replica and activate the backup ASG.
- **Why AWS marks C:** the passive Region has zero compute capacity and requires
  recovery actions before serving traffic. Failover routing models primary and
  secondary roles; latency routing models active endpoints.
- **Audit verdict:** **Key stands.** The design must coordinate data promotion,
  compute activation and traffic cutover; health detection alone is not enough.
- **Takeaway:** **pilot light = detect + promote data + scale compute + switch
  routing**. Use failover semantics unless both Regions are already active.
- **AWS reference:** <https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/dns-failover.html>

### Q54 — Automating a runbook-defined deployment

- **Learner:** `C` — CloudFormation for infrastructure but retain manual runbook
  steps.
- **AWS key:** `B` — CloudFormation for infrastructure plus EC2 user data for
  repeatable bootstrap.
- **Why AWS marks B:** it automates both resource creation and installation/
  configuration while keeping the environment declarative and changeable.
- **Audit verdict:** **Key stands.** For a larger real system, cfn-init, Systems
  Manager or image pipelines can replace oversized user-data scripts, but the
  exam distinction is automation versus documentation.
- **Takeaway:** **a runbook is not automation**. Put infrastructure and
  repeatable bootstrap/configuration into executable deployment artifacts.
- **AWS reference:** <https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-init.html>

### Q58 — Aurora Global Database local write forwarding

- **Learner:** `C` — read from the secondary endpoint and write directly to the
  primary endpoint.
- **AWS key:** `B` — use the secondary cluster endpoint for reads and forwarded
  writes; test DR with managed planned failover.
- **Why AWS marks B:** local write forwarding avoids a separate cross-Region
  write endpoint in the application and can provide read-after-write consistency
  for the session. Planned failover is the controlled DR-test mechanism.
- **Audit verdict:** **Key stands.** Explicitly choose the write-forwarding
  consistency mode in a real design; stronger consistency can increase latency.
- **Takeaway:** **write forwarding simplifies the application path**;
  consistency mode controls how long secondary reads wait for replicated writes.
- **AWS reference:** <https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-global-database-write-forwarding-apg.html>

### Q59 — Organization-wide SSH packet enforcement

- **Learner:** `B` — Firewall Manager common security-group policy that only
  identifies noncompliant resources.
- **AWS key:** `A` — Firewall Manager Network Firewall policy with an ordered
  stateless allow for trusted CIDRs and a broader deny/default path.
- **Why AWS marks A:** the requirement is centrally enforced packet filtering
  that workload accounts cannot weaken. The offered security-group policy is
  detective only because it says to identify noncompliance rather than remediate
  it.
- **Audit verdict:** **Key stands within the offered designs.** Network Firewall
  still requires centralized routing through firewall endpoints; a stateless
  rule with a lower numeric priority is evaluated first.
- **Takeaway:** distinguish **audit-only policy** from **enforced traffic path**.
  In Network Firewall, lower numeric stateless priority runs first.
- **AWS reference:** <https://docs.aws.amazon.com/network-firewall/latest/developerguide/stateless-rule-groups-standard.html>

### Q64 — FIFO migration and worker scaling

- **Learner:** `A` — change the existing Standard queue to FIFO and scale on raw
  visible-message count.
- **AWS key:** `C` — create a new FIFO queue, update the application, and use a
  backlog-per-instance target-tracking metric.
- **Why AWS marks C:** SQS queue type cannot be converted in place. A FIFO queue
  plus deduplication addresses duplicates, while backlog per worker accounts for
  changing fleet size and processing capacity.
- **Audit verdict:** **Key stands.** FIFO deduplication does not remove the need
  for application idempotency in all retry and side-effect scenarios.
- **Takeaway:** **new FIFO queue + producer/consumer migration**; scale workers
  on **backlog per instance**, not total queue depth alone.
- **AWS reference:** <https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-moving.html>

### Q67 — Idle nodes after scaling Memcached

- **Learner:** `A` — replace Memcached with sharded Redis.
- **AWS key:** `C` — discover the new Memcached node endpoints and configure the
  client to use them.
- **Why AWS marks C:** adding nodes changes available endpoints but does not make
  an unaware client distribute keys to them. Updating client discovery is much
  smaller than migrating cache engines.
- **Audit verdict:** **Key stands, though the durable implementation rule is
  Auto Discovery.** A supported client should use the configuration endpoint so
  future node changes are learned automatically.
- **Takeaway:** **Memcached scaling is client-side distribution**. Nodes can be
  healthy and idle when the client still knows only the old endpoint set.
- **AWS reference:** <https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/AutoDiscovery.Benefits.html>

### Q72 — Prefix-scoped S3 access at scale

- **Learner:** `C` — treat S3 folders as resources with attached policies.
- **AWS key:** `D` — create prefix-scoped S3 access points with resource policies.
- **Why AWS marks D:** S3 folders are key-prefix UI concepts and cannot hold IAM
  resource policies. Access points provide separate aliases and policies for
  distinct access patterns against one bucket.
- **Audit verdict:** **Key stands.** The bucket policy must also permit or
  delegate the intended access-point model.
- **Takeaway:** **prefixes organize keys; access points organize access**. Use
  access points when many teams need different views of one bucket.
- **AWS reference:** <https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-points.html>

### Q73 — Fast shutdown and restart with EC2 hibernation

- **Learner:** `B` — replace the root volume of existing instances and then
  enable hibernation.
- **AWS key:** `C` — create an encrypted AMI, relaunch supported instances with
  hibernation enabled, then hibernate them between events.
- **Why AWS marks C:** hibernation must be configured at launch and requires an
  encrypted, sufficiently large EBS root volume. It cannot be turned on for an
  existing instance.
- **Audit verdict:** **Key stands.** Current EC2 can also perform single-step
  encryption while launching from an unencrypted AMI, but C is the only offered
  answer that relaunches correctly with hibernation enabled.
- **Takeaway:** **hibernation is a launch-time contract**, not a setting that can
  be added to a stopped instance.
- **AWS reference:** <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/hibernating-prerequisites.html>

### Q75 — Geographic website variants with user exceptions

- **Learner:** `A` — path behaviours plus Lambda@Edge redirects.
- **AWS key:** `B` — one distribution with Lambda@Edge dynamically choosing the
  origin from request attributes.
- **Why AWS marks B:** origin-request logic can combine viewer location with a
  cookie, header or other per-user exception and select either S3 origin without
  exposing separate distribution logic to the client.
- **Audit verdict:** **Key stands.** Static cache behaviours route by path, not by
  arbitrary location-plus-user exception logic.
- **Takeaway:** use **cache behaviours for static path rules** and
  **Lambda@Edge origin-request logic for dynamic origin selection**.
- **AWS reference:** <https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/lambda-event-structure.html>

## What to carry into Mock 006

Do not attempt to memorize 30 isolated answers. Carry these seven decision
rules:

1. Identify the actual bottleneck before selecting a scaling mechanism.
2. Treat explicit workload constraints—custom AMI, shared filesystem, native
   failover, private cross-Region access—as service-boundary filters.
3. Separate service roles: findings versus audit evidence, tag vocabulary versus
   prevention, contact handling versus intent versus integration.
4. Prefer managed topology changes over leaving/rejoining organizations or
   maintaining manual runbooks.
5. Distinguish Regional HA from cross-Region DR and coordinate data, compute and
   routing during failover.
6. For multi-response questions, map every stated requirement to one selected
   component and reject options that satisfy only part of the requirement.
7. Challenge old practice keys against current AWS documentation when the
   service capability has changed; do not challenge a key merely because another
   design could also work outside the offered constraints.

The next evidence step remains Full Mock 006. This audit is remediation and
source-quality control, not an additional booking-gate mock.
