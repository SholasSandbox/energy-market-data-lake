# SAP-C02 Revision Notes v2

**Last revised:** 2026-08-08

Purpose: SAP-C02 revision notes rebuilt from the uploaded SAA-C03 notes, with the missing service-scenario depth added. These notes are optimized for **AWS Certified Solutions Architect - Professional (SAP-C02)** style questions, where the answer usually depends on architecture trade-offs rather than service definitions.

Use this pack as a service-selection and scenario-recognition reference, not as a substitute for official AWS documentation or hands-on practice.

The numbered chapter files are the canonical editable notes. The former
`sap-c02-revision-pack-v2.md` combined reading copy was removed on 2026-08-07
because it duplicated the chapter files and created a second maintenance
surface. Use the parent [revision-note library](../README.md) to choose between
the broad core pack and newer targeted lessons.


## What changed from v1

The earlier version was too sparse because it compressed the source notes into the four SAP-C02 domains. That loses the practical exam skill: identifying which AWS service belongs in the scenario.

This v2 pack adds service-focused revision chapters for:

- **Kinesis Data Streams / Amazon Data Firehose / Managed Service for Apache Flink**
- **Application Load Balancer (ALB), Network Load Balancer (NLB), Gateway Load Balancer (GWLB), Route 53, CloudFront, Global Accelerator**
- **Amazon ECS, AWS Fargate, Amazon EKS, AWS Batch, Amazon ECR**
- **Lambda, SQS, SNS, EventBridge, Step Functions**
- **S3, EFS, FSx, Storage Gateway, DataSync, Snow Family**
- **RDS, Aurora, DynamoDB, ElastiCache, Redshift, OpenSearch**
- **Organizations, SCPs, IAM, KMS, CloudTrail, Config, Security Hub, GuardDuty, WAF, Shield**
- **DMS, SCT, MGN, Migration Hub, 7 Rs**
- **DR, multi-region, multi-account, hybrid connectivity, observability, cost optimization**

## Recommended revision sequence

| Step | File | Why |
|---:|---|---|
| 1 | `00-exam-scope-and-study-map.md` | Know the exam domains and how service knowledge maps to them. |
| 2 | `01-service-scenario-index.md` | Build quick recognition of service trigger phrases. |
| 3 | `02-kinesis-streaming-analytics.md` | High priority because you missed a Kinesis stream question. |
| 4 | `03-load-balancing-dns-edge.md` | Frequent SAP-C02 scenario area: ALB/NLB/Route 53/GA/CloudFront. |
| 5 | `04-containers-ecs-fargate-eks.md` | Common modern architecture pattern. |
| 6 | `05-serverless-event-driven.md` | Lambda/SQS/SNS/EventBridge/Step Functions decision making. |
| 7 | Remaining domain/service files | Fill gaps and reinforce exam traps. |
| 8 | `17-less-common-service-boundaries.md` | Build recognition and elimination depth for less-frequent services in the current official scope. |

## How to revise with this pack

For each service, learn four things:

1. **Trigger phrases**: what wording points to the service.
2. **Disqualifiers**: wording that makes the service the wrong answer.
3. **Failure modes**: throttling, ordering loss, retry storms, stale DNS, hot partitions, bad health checks.
4. **Architecture pairings**: services rarely appear alone in SAP-C02.

## Source basis

- Uploaded SAA-C03 notes archive: `saa-c03-notes.tar.gz`
- Official AWS SAP-C02 exam guide and AWS documentation listed in `source-manifest.json`

## Warning

Do not memorize these notes as isolated facts. SAP-C02 questions often include two technically valid answers. The correct answer is usually the one that best satisfies **all** constraints: security, reliability, operational complexity, cost, migration risk, performance, and governance.
