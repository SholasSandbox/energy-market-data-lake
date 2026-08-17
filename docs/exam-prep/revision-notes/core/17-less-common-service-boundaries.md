# 17 - Less-Common Services and Cross-Domain Boundaries

**Last revised:** 2026-08-08

## Purpose

The current SAP-C02 in-scope list is broader than the services that dominate
most mock examinations. This chapter supplies **recognition and elimination
depth** for less-common services. It is not a request to memorize every API,
quota, or implementation procedure.

Use this chapter when a question contains an unusually specific workload noun:
GraphQL, ActiveMQ, industrial telemetry, virtual desktop, third-party dataset,
HSM interface, SaaS connector, speech transcription, or application streaming.

## Application integration and frontend

| Scenario signal | Service | Eliminate |
|---|---|---|
| Existing ActiveMQ Classic or RabbitMQ application; retain JMS/AMQP/MQTT/OpenWire/STOMP compatibility | Amazon MQ | SQS/SNS/EventBridge require AWS-native APIs and semantics |
| Managed GraphQL schema, resolvers, subscriptions, or real-time client updates | AWS AppSync | API Gateway is the general REST/HTTP/WebSocket service but does not supply GraphQL resolver semantics |
| Transfer supported SaaS records between applications and AWS services such as S3 | Amazon AppFlow | DataSync moves file/object storage; DMS moves database data |
| Transactional or bulk application email | Amazon SES | SNS email subscriptions are notifications, not a full email-sending platform |
| Build/host a web or mobile frontend with integrated AWS backend workflows | AWS Amplify | Do not select merely because the workload has a browser UI |
| Customer application registration, authentication and tokens | Amazon Cognito | IAM Identity Center is for workforce access to AWS accounts/apps |
| Targeted customer engagement campaigns/channels where the exam explicitly names the capability | Amazon Pinpoint | SES is email delivery; SNS is notification fanout |
| Test mobile applications on hosted real devices or web applications across browser environments | AWS Device Farm | It tests client applications; it does not host the production frontend |

## Analytics and externally supplied data

| Scenario signal | Service | Eliminate |
|---|---|---|
| Find, license, subscribe to, grant, or consume third-party data products | AWS Data Exchange | Glue transforms data; Data Exchange supplies entitlements and delivery |
| Hadoop/Spark ecosystem and framework/cluster control | Amazon EMR | Glue is the simpler serverless ETL/catalog path when cluster control is unnecessary |
| Managed dashboards and business intelligence | Amazon QuickSight | Athena/Redshift provide query/storage layers, not the final BI experience |
| Fine-grained governed table/column/row access to a data lake across accounts | Lake Formation | IAM/S3/KMS still enforce adjacent identity, storage and encryption boundaries |
| Managed Kafka compatibility | Amazon MSK | Kinesis Data Streams is the AWS-native stream; Amazon MQ is a traditional message broker |
| Create or access a managed blockchain network where blockchain is an explicit business requirement | Amazon Managed Blockchain | Do not introduce blockchain merely to provide an immutable application audit log; S3 Object Lock or a database audit design solves a different requirement |

## Compute location and end-user computing

| Scenario signal | Service | Eliminate |
|---|---|---|
| Source code or image directly to a managed, scalable public web service | App Runner | ECS/Beanstalk expose more architecture choices and operations |
| Managed application platform with customer-visible EC2 environment and deployment configuration | Elastic Beanstalk | App Runner is the narrower managed web-service abstraction |
| Simple small website/app with bundled, predictable infrastructure | Lightsail | Not the normal answer for complex enterprise multi-account architecture |
| AWS infrastructure/services physically installed on premises for residency or local latency | AWS Outposts | Local Zones remain AWS-operated metro infrastructure; Snow handles transfer/edge cases |
| Mobile/5G workload must execute at a telecommunications edge | AWS Wavelength | Select only when carrier-edge latency is explicit |
| Persistent managed virtual desktops | Amazon WorkSpaces | WorkSpaces Applications/AppStream streams applications rather than providing the same persistent desktop model |
| Stream centrally managed desktop applications to users | Amazon WorkSpaces Applications (AppStream 2.0) | Do not choose a full desktop when the requirement is application streaming |
| Run ECS tasks on registered on-premises or edge servers while retaining the ECS control plane | Amazon ECS Anywhere | Outposts supplies AWS infrastructure on premises; ECS Anywhere extends ECS management to external instances |
| Operate an AWS-supported Kubernetes distribution on customer-managed on-premises infrastructure | Amazon EKS Anywhere | EKS in an AWS Region is the managed AWS-hosted control-plane service |
| Use the open-source Kubernetes distribution that Amazon EKS is based on | Amazon EKS Distro | A distribution is not itself a managed cluster or control plane |

## Security, compliance and organization-scale operations

| Scenario signal | Service | Eliminate |
|---|---|---|
| Download AWS compliance reports/certifications or manage AWS agreements | AWS Artifact | Audit Manager collects evidence about customer controls |
| Continually collect and organize assessment evidence for the customer environment | AWS Audit Manager | It does not certify legal/regulatory compliance |
| Centrally deploy WAF, Shield Advanced, Network Firewall, DNS Firewall, SG or NACL policies across an organization | AWS Firewall Manager | The underlying protections enforce traffic; Firewall Manager manages policy at scale |
| Single-tenant HSM cluster or application requires PKCS #11/JCE/CNG interfaces | AWS CloudHSM | KMS is the usual managed key service integrated with AWS resources |
| Microsoft AD-compatible directory for domain joins, trusts or directory-dependent applications | AWS Directory Service | IAM Identity Center is workforce access orchestration, not a managed Microsoft AD replacement |
| Curated self-service infrastructure products with organizational governance | AWS Service Catalog | CloudFormation defines stacks; Service Catalog governs approved products/portfolios |
| Centrally enforced Config-rule bundle with account exclusions | AWS Config organization conformance pack | StackSets distribute general stacks but do not provide the same Config governance contract |
| Track and optimize owned software licenses across AWS/on premises | AWS License Manager | Cost Explorer tracks spend, not license-entitlement rules |
| Record workload reviews against Well-Architected pillars and improvement items | AWS Well-Architected Tool | Trusted Advisor and Compute Optimizer provide checks/recommendations from different evidence |
| Standardized infrastructure/application templates for platform teams | AWS Proton | Do not select merely because a single stack needs CloudFormation |
| Account- and resource-specific AWS service events, planned changes or impairments | AWS Health Dashboard | CloudWatch monitors workload telemetry; the public Service Health Dashboard is not account-specific |
| View and request increases for AWS service quotas | Service Quotas | Trusted Advisor can flag limits, but Service Quotas owns quota values and supported increase requests |
| Managed visualization and dashboards for metrics, logs and traces | Amazon Managed Grafana | Grafana visualizes; it is not the Prometheus-compatible metrics store |
| Managed, scalable Prometheus-compatible metric ingestion, storage and queries | Amazon Managed Service for Prometheus | Pair it with Grafana when a dashboard is required |

## Developer and deployment tools

| Need | Service |
|---|---|
| Private package repository for supported package formats | CodeArtifact |
| Managed build/test/package execution | CodeBuild |
| Orchestrate delivery stages, approvals and integrations | CodePipeline |
| Deploy to EC2, ECS or Lambda with supported rollout strategies | CodeDeploy |
| Code review/profiling recommendations where explicitly named | CodeGuru |
| Distributed request tracing and service-map analysis | X-Ray |

```text
source
  -> CodePipeline
       -> CodeBuild
       -> approval/test stages
       -> CodeDeploy or CloudFormation
```

Do not confuse the orchestrator, builder, deployer and infrastructure engine.

### AMI indirection and approved self-service

Use the same Systems Manager Parameter Store name in each target account and
Region when one CloudFormation template must resolve the local custom AMI ID.
Updating the parameter decouples AMI publication from template editing. A
mapping still embeds every Region's AMI ID in the template, and a normal stack
in one account cannot provision resources in unrelated accounts without a
cross-account mechanism such as StackSets.

Service Catalog is the developer self-service control when teams may launch
only approved development/test configurations. CloudFormation defines each
product; portfolios and launch constraints determine who may launch it and
which provisioned-product role supplies the deployment permissions. Merely
placing unrestricted templates in S3 does not constrain developers to the
approved configurations.

## IoT recognition table

| Workload noun | Service |
|---|---|
| Secure device connectivity, MQTT broker, rules routing | AWS IoT Core |
| Fleet registration, grouping, jobs and remote device management | AWS IoT Device Management |
| IoT configuration audits, behaviour detection and security posture | AWS IoT Device Defender |
| Detect significant device/equipment state patterns and trigger actions | AWS IoT Events |
| Run local messaging, components and processing on edge devices | AWS IoT Greengrass |
| Industrial equipment data collection, modelling and monitoring | AWS IoT SiteWise |
| Simple configured devices invoke cloud actions such as Lambda with one click | AWS IoT 1-Click; retain recognition only because it is a legacy entry in the current guide |
| Visually connect devices and services into IoT workflows | AWS IoT Things Graph; retain recognition only because it is a legacy entry in the current guide |

IoT Core is the connectivity foundation; the other services solve fleet,
security, event-state, edge-runtime or industrial-data requirements. Do not
select an IoT service merely because a producer emits telemetry—Kinesis may be
the downstream streaming layer.

## Media recognition table

| Workload | Service |
|---|---|
| Ingest, store and consume time-indexed video streams from cameras/devices | Kinesis Video Streams |
| Asynchronously convert media files between formats using the legacy service named in the current exam guide | Elastic Transcoder |

Use these as explicit-noun recognition cues. Do not infer that a legacy-listed
service is the preferred design for a new production system when a current AWS
service outside the exam list would normally supersede it.

## Purpose-built AI/ML recognition table

These are architecture service-selection cues, not model-development depth.

| Workload | Service |
|---|---|
| Entity/sentiment/key-phrase analysis of text | Amazon Comprehend |
| Extract printed text, handwriting, forms or tables from documents | Amazon Textract |
| Speech to text | Amazon Transcribe |
| Text translation | Amazon Translate |
| Text to speech | Amazon Polly |
| Image/video labels, faces or moderation | Amazon Rekognition |
| Conversational voice/text interface | Amazon Lex |
| Cloud contact center, inbound calls and contact flows | Amazon Connect |
| Personalized recommendations | Amazon Personalize |
| Enterprise semantic/search experience across connected content | Amazon Kendra |
| Build, train, tune and deploy custom ML models | Amazon SageMaker AI |
| Managed fraud-risk prediction where the service is explicitly in scope | Amazon Fraud Detector |

The decisive distinction is usually **purpose-built API versus custom-model
platform**. Select SageMaker when the organization must build/train/manage the
model rather than call a managed domain API.

## Composed-pattern check

Before choosing an answer, reconstruct the complete path:

```text
identity
  -> network/edge entry
  -> compute or integration layer
  -> data store
  -> encryption
  -> telemetry/audit
  -> failure and recovery path
```

A three-service answer is often correct because each service owns a different
boundary. Reject combinations that duplicate one boundary while leaving another
requirement unaddressed.

## High-value traps

| Trap | Correction |
|---|---|
| “Any queue means SQS” | Existing broker protocols and minimal rewrite can select Amazon MQ. |
| “Lex is the whole call center” | Connect supplies telephony/contact flows, Lex supplies conversational intent, and Lambda integrates business systems. |
| “Any API means API Gateway” | GraphQL schema/resolver/subscription requirements can select AppSync. |
| “Any data transfer means DataSync” | SaaS records select AppFlow; DB rows select DMS; third-party data entitlements select Data Exchange. |
| “Any key control means CloudHSM” | KMS is the managed integrated default; CloudHSM requires an explicit HSM/application-interface requirement. |
| “Any remote desktop/app means WorkSpaces” | Distinguish a persistent virtual desktop from streamed applications. |
| “The in-scope list implies equal depth or frequency” | Use recognition depth for long-tail services and deep decision models for recurring/high-weight patterns. |
| “A service appears in the guide, so it must be the current production default” | The guide is non-exhaustive and can retain legacy entries; learn the tested boundary and re-check current AWS guidance for live design. |

## Official references

- SAP-C02 exam guide: <https://docs.aws.amazon.com/aws-certification/latest/solutions-architect-professional-02/solutions-architect-professional-02.html>
- SAP-C02 in-scope services: <https://docs.aws.amazon.com/aws-certification/latest/solutions-architect-professional-02/sap-02-in-scope-services.html>
- Amazon MQ: <https://docs.aws.amazon.com/amazon-mq/latest/developer-guide/welcome.html>
- AppSync real-time subscriptions: <https://docs.aws.amazon.com/appsync/latest/devguide/aws-appsync-real-time-data.html>
- Firewall Manager: <https://docs.aws.amazon.com/waf/latest/developerguide/fms-chapter.html>
- Audit Manager and Artifact boundary: <https://docs.aws.amazon.com/audit-manager/latest/userguide/what-is.html>
- AppFlow: <https://docs.aws.amazon.com/appflow/latest/userguide/flow-tutorial.html>
- Data Exchange: <https://docs.aws.amazon.com/data-exchange/latest/userguide/what-is.html>
