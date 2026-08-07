# Transformation Log

Updated: 2026-08-07

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
