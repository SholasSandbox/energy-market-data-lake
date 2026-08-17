# 00 - SAP-C02 Exam Scope and Study Map

**Last revised:** 2026-08-08

## Official domain model

SAP-C02 is organized around four scored content domains:

| Domain | Weight | Architecture focus |
|---|---:|---|
| Domain 1: Design Solutions for Organizational Complexity | 26% | Multi-account, networking, identity, centralized governance, hybrid access |
| Domain 2: Design for New Solutions | 29% | New workload architecture, security controls, reliability, performance, cost |
| Domain 3: Continuous Improvement for Existing Solutions | 25% | Refactoring, resilience improvement, observability, performance and cost tuning |
| Domain 4: Accelerate Workload Migration and Modernization | 20% | Migration planning, 7 Rs, database migration, application modernization |

## What SAP-C02 actually tests

The exam is less about “what is service X?” and more about:

- Choosing between similar services under constraints.
- Recognizing hidden non-functional requirements.
- Avoiding over-engineering.
- Designing blast-radius control across accounts, networks, and regions.
- Combining services into a complete, operable architecture.

## Scenario-depth calibration

AWS Skill Builder-style questions frequently provide three or more individually
reasonable controls. The winning answer is the combination that satisfies
**every** stated constraint at the correct enforcement point.

For each option, test four things:

1. **Direction:** which component initiates the action and which component
   enforces it?
2. **Mode:** does the named service mode support the required logging,
   consistency, protocol, target type, or failover behaviour?
3. **Boundary:** does the control protect the viewer edge, workload origin,
   identity, account, network, or resource that the question actually names?
4. **Operating qualifier:** does the answer meet “least operational effort”,
   “no code changes”, “no public ingress”, “retain existing protocol”, or a
   similar decisive phrase?

Example composed pattern:

```text
CloudFront-only application access
  -> private S3 origin: OAC + bucket policy
  -> internet-facing ALB origin: CloudFront secret header
     + origin-side validation
  -> internal ALB origin: consider a CloudFront VPC origin
```

Knowing OAC, WAF, CloudFront, and ALB separately is insufficient. The exam can
test where each control belongs and which direct-access path remains open.

## Current exam-guide boundary

The current exam guide retains four scored domains and also lists emerging
responsible-AI topics as possible **unscored pretest** content. Keep only a
recognition-level model before the SAP-C02 attempt:

- Bedrock Guardrails: content filtering and policy controls;
- AgentCore Identity: access controls for agentic applications; and
- Step Functions or an equivalent workflow: explicit human approval for
  consequential AI actions.

Do not let this emerging-topic note displace scored-domain revision.

## Revision model

Use this mental chain for every scenario:

```text
Business requirement
  -> non-functional constraints
  -> workload pattern
  -> AWS service family
  -> specific service
  -> operating model
  -> security and failure controls
```

## Decision dimensions

| Dimension | What to ask | Example |
|---|---|---|
| Security | Who can access what, from where, and under what policy? | SCP + IAM role + KMS key policy + VPC endpoint policy |
| Reliability | What fails and how does traffic recover? | ALB health checks, Route 53 failover, multi-AZ RDS |
| Scalability | What is the scaling unit? | Kinesis shard, ECS task, Lambda concurrency, DynamoDB partition |
| Performance | Where is latency introduced? | DNS TTL, CloudFront cache miss, NAT Gateway bottleneck, DB read replica lag |
| Cost | Which resource scales with requests, throughput, storage, or time? | NAT processing, Kinesis shards, provisioned DynamoDB, Fargate vCPU/memory |
| Operations | Who patches, scales, backs up, and observes it? | Fargate vs EC2, Aurora Serverless vs provisioned, managed MSK vs self-managed Kafka |
| Migration risk | How much code/data/schema/network change is required? | DMS homogeneous migration vs SCT heterogeneous conversion |

## Service-to-domain mapping

| Service family | Domain 1 | Domain 2 | Domain 3 | Domain 4 |
|---|---:|---:|---:|---:|
| Organizations, SCP, IAM Identity Center, Control Tower | High | Medium | High | Medium |
| VPC, Transit Gateway, Direct Connect, VPN, PrivateLink | High | High | High | High |
| Route 53, CloudFront, Global Accelerator, ALB/NLB | Medium | High | High | Medium |
| ECS, Fargate, EKS, Lambda, Batch | Medium | High | High | High |
| SQS, SNS, EventBridge, Step Functions, Kinesis | Medium | High | High | Medium |
| S3, EFS, FSx, Storage Gateway, DataSync, Snow | Medium | High | High | High |
| RDS, Aurora, DynamoDB, ElastiCache, Redshift | Medium | High | High | High |
| KMS, Secrets Manager, CloudTrail, Config, GuardDuty, WAF | High | High | High | Medium |
| DMS, SCT, MGN, Migration Hub | Medium | Medium | Medium | High |

## High-yield SAP-C02 service clusters

### Streaming and event processing

```text
Real-time ordered stream -> Kinesis Data Streams
Near-real-time delivery to S3/OpenSearch/Redshift -> Firehose
Stream SQL/Flink analytics -> Managed Service for Apache Flink
Work queue / retry buffer -> SQS
Broadcast fanout -> SNS
SaaS/AWS/app event routing -> EventBridge
Workflow orchestration -> Step Functions
```

### Edge, routing, and load balancing

```text
DNS decision -> Route 53
HTTP cache / edge delivery / WAF at edge -> CloudFront
Global static anycast IP + regional endpoint acceleration -> Global Accelerator
HTTP/HTTPS host/path routing -> ALB
TCP/UDP/TLS low-latency static IP load balancing -> NLB
Transparent appliance insertion -> Gateway Load Balancer
```

### Containers and compute

```text
Short event handler -> Lambda
Long-running container service with minimal infrastructure ops -> ECS on Fargate
Kubernetes required -> EKS
Batch scheduling -> AWS Batch
Full host control / GPU / daemon agent -> EC2, ECS on EC2, or EKS managed nodes
```

## Trap pattern

Many wrong SAP-C02 answers fail because they solve only one requirement.

Example:

> Need active-passive regional failover for a public web app.

A partial answer says “use ALB across multiple AZs.” That solves regional AZ resilience but not regional failover. A stronger answer includes **Route 53 failover or Global Accelerator**, health checks, replicated data, DNS/endpoint behavior, and application state strategy.

## Source references

- Current SAP-C02 exam guide: <https://docs.aws.amazon.com/aws-certification/latest/solutions-architect-professional-02/solutions-architect-professional-02.html>
- Current in-scope services: <https://docs.aws.amazon.com/aws-certification/latest/solutions-architect-professional-02/sap-02-in-scope-services.html>
- See `source-manifest.json` for additional official AWS documentation URLs.
