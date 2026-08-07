# 12 - Cost, Performance, Observability, and Continuous Improvement

Domain 3 often asks how to improve an existing architecture without violating requirements.

## Cost optimization levers

| Area | Options |
|---|---|
| EC2 | right-size, Graviton, Spot, Reserved Instances, Savings Plans, Auto Scaling |
| Containers | Fargate vs EC2 capacity providers, Fargate Spot for interruptible tasks |
| Lambda | memory tuning, provisioned concurrency only where needed, reduce duration |
| S3 | lifecycle, Intelligent-Tiering, storage class choice, reduce request/data transfer |
| NAT | VPC endpoints, reduce cross-AZ/cross-region traffic |
| DynamoDB | on-demand vs provisioned, auto scaling, table design, DAX only if justified |
| RDS/Aurora | right-size, read replicas, Serverless v2 where fit, storage/performance tuning |
| Data analytics | Parquet/ORC, partitioning, compression, reduce data scanned |
| CloudFront | cache more at edge, reduce origin load/data transfer |
| Kinesis | provisioned vs on-demand, shard count, EFO only when needed |

## Performance tuning

| Symptom | Likely area |
|---|---|
| ALB 5xx | target health, app errors, deployment issue |
| High target response time | app/database/backend latency |
| Lambda throttles | concurrency limit/reserved concurrency |
| Lambda cold start | package size, VPC/networking, provisioned concurrency |
| SQS age of oldest message rising | consumers under-scaled or failing |
| Kinesis iterator age rising | stream consumer lag |
| DynamoDB throttling | hot partition or insufficient capacity |
| RDS high CPU/connections | query/index/connection pooling/read scaling issue |
| NAT high cost | private subnet traffic to AWS services/internet |
| Athena high cost | too much data scanned |

## Observability services

| Service | Use |
|---|---|
| CloudWatch Metrics | service/resource metrics |
| CloudWatch Logs | log collection/search |
| CloudWatch Alarms | threshold/composite alarms |
| CloudWatch Synthetics | canaries |
| CloudWatch Evidently | feature experimentation |
| X-Ray | distributed tracing |
| CloudTrail | API audit |
| AWS Config | resource config history/compliance |
| VPC Flow Logs | network flow visibility |
| ELB access logs | request-level load balancer visibility |
| S3 server access logs / CloudTrail data events | S3 access audit |
| Cost Explorer | cost trend analysis |
| Compute Optimizer | rightsizing recommendations |
| Trusted Advisor | account checks |
| AWS Health | service/account health events |

## Improvement workflow

```text
Measure
  -> identify bottleneck/cost driver
  -> change the correct layer
  -> validate against SLO/SLA/security
  -> automate guardrails
  -> monitor regression
```

## Common improvement scenarios

### High NAT Gateway cost

Likely fix:

- Add S3 Gateway endpoint.
- Add DynamoDB Gateway endpoint if relevant.
- Add interface endpoints for ECR, CloudWatch Logs, Secrets Manager, SSM, KMS.
- Keep large data paths regional and private.
- Review cross-AZ routing.

### Lambda throttling

Likely fixes:

- Increase account concurrency limit if justified.
- Set reserved concurrency for critical functions.
- Reduce upstream burst using SQS.
- Use provisioned concurrency for latency-sensitive cold-start issues.
- Move long-running workers to ECS/Fargate.

### Kinesis consumer lag

Likely fixes:

- Increase shard count or switch capacity mode where appropriate.
- Fix hot partition key.
- Use enhanced fan-out for read contention.
- Tune Lambda batch/parallelization.
- Scale KCL/ECS consumers.
- Reduce downstream latency.

### RDS connection exhaustion

Likely fixes:

- RDS Proxy.
- App connection pooling.
- Reduce Lambda burst directly to DB.
- Read replicas for read load.
- Query/index optimization.

### S3 analytics slow/expensive

Likely fixes:

- Convert CSV/JSON to Parquet/ORC.
- Partition by common filter columns.
- Compress.
- Use Glue Data Catalog.
- Avoid small-file explosion.
- Consider Redshift for warehouse workloads.

## Operational excellence

High-yield controls:

- Infrastructure as Code (CloudFormation/CDK/Terraform).
- Automated deployment with rollback.
- Centralized logging.
- Runbooks and playbooks.
- Game days/DR tests.
- Least privilege CI/CD roles.
- Tagging and cost allocation.
- Config rules and SCP guardrails.
- Backup policies and restore testing.

## Exam traps

| Trap | Correction |
|---|---|
| “Add more EC2 instances for every latency issue” | Find bottleneck first; DB/cache/network may be limiting. |
| “Use provisioned concurrency to reduce Lambda cost” | It reduces cold starts, but can increase cost. |
| “Add read replicas for write bottleneck” | Read replicas reduce reads, not writes. |
| “Use DAX for every DynamoDB table” | Use only for read-heavy eventually consistent cacheable workloads. |
| “Use S3 Glacier for frequently accessed data” | Retrieval latency/cost can violate requirements. |
| “Use Spot for critical non-interruptible tasks” | Use Spot only when interruption is acceptable. |
