# Transformation Log

Updated: 2026-08-09

## 2026-08-09 Skill Builder calibration continuation

- Replaced the partial browser/PDF evidence with the complete exported
  assessment workbook: attempt 2 passed at 775 against a 750 threshold, with
  45 correct, 30 incorrect and no skipped questions. The workbook is external
  calibration evidence, not a timed booking-gate mock.
- Corrected earlier independent interpretations for mandatory tag presence,
  Organizations-integrated Security Hub, organization-wide SSH enforcement,
  Secrets Manager rotation selection and Db2-to-RDS migration. The durable
  rules now distinguish tag policy from SCP, Network Firewall policy from
  security-group audit, single-user from alternating-users rotation, and DMS
  replication from SCT/schema conversion.
- Preserved current-service corrections where the exported assessment key is
  dated: Object Lock can now be enabled on an existing bucket, and current KMS
  HSMs are FIPS Level 3 validated. These items are not being memorized from the
  older key literally.

- Added the WorkSpaces plus AD Connector proxy-authentication pattern and made
  the AWS-versus-on-premises placement boundary explicit.
- Added SCP attachment-scope inheritance, Organizations tag-policy enforcement
  limits, and Security Hub multi-account/multi-Region aggregation rules.
- Distinguished MemoryDB as a durable Redis-compatible database from
  ElastiCache Redis OSS and Memcached cache roles, including snapshot import
  and native Multi-AZ failover selection.
- Added forecasted AWS Budgets actions, one-NAT-per-AZ resilience, public-apex
  latency alias routing, and Aurora PostgreSQL Global Database managed-RPO
  mechanics.
- Split second-Region S3 decisions by economics: CRR plus a local gateway
  endpoint for repeated/high-volume reads versus a billed interface endpoint
  over inter-Region connectivity for low-volume access where full data
  duplication is wasteful.
- Used the supplied questions only as calibration evidence; the canonical notes
  retain reusable scenario rules rather than copied questions.
- Added the two-sided cross-account `AssumeRole` authorization model and exact
  trust-principal scoping.
- Added coordinated pilot-light Regional recovery: health detection, Route 53
  failover, compute activation and RDS read-replica promotion, including the
  incomplete-design traps when either traffic movement or recovery actions are
  missing.
- Completed the supplied 75-question Skill Builder calibration with reusable
  rules for CloudFormation bootstrap automation, Lightsail, Transfer Family to
  Glue events, ECS Fargate replatforming, Aurora write forwarding/switchover,
  Firewall Manager policy selection, multi-account OU design and centralized
  EventBridge processing.
- Added Organizations/AWS Backup policy inheritance, SQS FIFO migration and
  backlog-per-instance scaling, S3 File Gateway and access points, Memcached
  Auto Discovery, Transit Gateway consolidation, NACL reverse-path reasoning,
  EC2 hibernation, Db2 rehost-versus-replatform selection and Lambda@Edge
  origin selection.

## 2026-08-08 exam-depth calibration

- Calibrated the pack against the current official SAP-C02 domain guide and
  in-scope service list plus representative AWS Skill Builder questions.
- Added composed CloudFront origin-protection and device-specific edge-content
  patterns.
- Added Session Manager shell, port-forwarding and logging boundaries.
- Added cluster placement group, ENA and EFA selection rules.
- Added bounded replatforming, S3 bulk-encryption, Macie, App Runner, Elastic
  Beanstalk, Amazon MQ, AppSync, Firewall Manager, CloudHSM, Artifact and Audit
  Manager discriminators.
- Added a recognition-depth chapter for less-common in-scope services.
- Added current Object Lock mode/retroactivity rules, Intelligent-Tiering
  archive restore-time selection, multi-account SCP recovery, current KMS
  Level 3 scheduling, and complete HPC bundle reasoning from further
  representative Skill Builder calibration.
- Added reusable rules for Regional EFS versus same-AZ EBS Multi-Attach,
  organization-zone Access Analyzer, Cognito developer-authenticated guest
  transitions, managed-EC2 Batch with custom AMIs, Spot purchase placement,
  Auto Scaling signal selection, Transit Gateway route-domain isolation, DAX
  cost patterns, Amazon Connect composition, and database-secret rotation.
- Added reusable rules for ALB request-count target tracking, durable HTTP-to-SQS
  acceptance, current AWS Transform MGN naming and transfer-time comparison,
  organization conformance packs, CloudTrail integrity validation, local-Region
  S3 replication, Parameter Store AMI indirection, Managed Microsoft AD MFA,
  KMS deletion alerts, Service Catalog launch constraints, exact OAC policy
  principals, and ARC readiness-versus-routing responsibilities.
- Removed the retired QLDB cue and the discontinued CloudWatch Evidently cue.
- Kept emerging responsible-AI material at recognition depth because the
  current guide labels it unscored pretest content.

## 2026-08-07 consolidation

- Moved the canonical numbered chapters into the repository revision-note
  library under `docs/exam-prep/revision-notes/core/`.
- Removed the synchronized combined reading copy because it duplicated the
  numbered chapters and created a competing maintenance surface.
- Preserved this log and `source-manifest.json` with the canonical chapters for
  provenance.

## 2026-07-28 assessment-led corrections

- Added the ARC routing-control DNS-cache boundary.
- Added the gp2 versus gp3 versus io2 Block Express decision boundary.
- Corrected DynamoDB MREC and MRSC strong-read semantics.
- Made Direct Connect longest-prefix precedence explicit.
- Corrected Region-restriction SCP global-service exception reasoning.
- Distinguished Discovery Agent physical-host telemetry from the current Agentless Collector network module.
- Synchronized the combined reading copy with the updated numbered chapters.
- Expanded Application Discovery Service, DataSync, and Migration Hub decision boundaries, including the VMware vCenter module versus WinRM/SNMP network-module distinction.
- Re-synchronized Chapters 10 and 13 in the combined reading copy.

## User feedback addressed

The first generated SAP-C02 notes were too sparse. They focused on exam domains but did not include enough detail on the services that appear inside exam scenarios, especially:

- Kinesis
- ALB/NLB
- Route 53
- Fargate/ECS
- event-driven services
- storage/data movement
- networking and hybrid access
- security/governance
- migration and DR

## Changes made in v2

- Rebuilt the pack around service-scenario decision making.
- Added a deep Kinesis chapter for missed practice questions.
- Added ALB/NLB/Route 53/CloudFront/Global Accelerator coverage.
- Added ECS/Fargate/EKS operational and exam traps.
- Added serverless and event-driven architecture coverage.
- Added storage, database, networking, security, migration, DR, and cost chapters.
- Added decision matrices and flashcards.
- Removed duplicate SAA-C03 flashcard repetition and study-plan filler.
- Preserved useful SAA-C03 foundations but elevated the depth and trade-off framing to SAP-C02.

## Source archive inspected

Uploaded source archive: `saa-c03-notes.tar.gz`

Notable source files used conceptually:

- `saa-c03-data-movement-revision.md`
- `saa-c03-service-picker-route53-ga-alb-nlb-cloudfront.md`
- `saa-c03-Docker-Kubernetes-EKS-Fargate-ECS_Revision_Notes.md`
- `saa-c03-lambda-containers-kinesis-security-revision.md`
- `saa-c03-streaming-queues-orchestration-flashcards.md`
- `saa-c03-hpc-networking-global-revision.md`
- `saa-c03-iam-identity-revision.md`
- `saa-c03-dr-caching-revision.md`
- `saa-c03-kms-secrets-hsm-revision.md`
- `saa-c03-cross-account-services-revision.md`

## Remaining limitation

These are revision notes, not a full textbook. For final readiness, pair them with:

- official AWS exam guide
- AWS whitepapers and Well-Architected material
- hands-on labs
- timed SAP-C02 practice blocks
- post-practice error log
