# 12 - Cost, Performance, Observability, and Continuous Improvement

**Last revised:** 2026-08-09

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

### AWS Budgets forecast plus automated action

When the requirement is advance warning and the lowest risk of overspending,
use a **forecasted** cost threshold rather than waiting for actual spend to
cross it. An AWS Budgets action can then stop named EC2 instances automatically
after the configured approval model, whereas notification alone depends on a
human responding in time.

Check the surrounding architecture: stopping an instance in an Auto Scaling
group may cause the group to replace it. If the goal is to cap an autoscaled
workload, also control desired capacity or the permissions that launch further
capacity; an instance-stop action alone is not a durable fleet cap.

### Stable baseline, variable peak and queued compute

```text
steady required web baseline -> Reserved Instances or Savings Plans
unpredictable web peak       -> On-Demand capacity
interruptible queued work    -> diversified Spot capacity
compute-intensive processing -> compute-optimized instance families
memory-intensive processing  -> memory-optimized instance families
```

Do not place the required website baseline on Spot solely to reduce cost. A
durable SQS backlog makes independent, restartable workers a much better Spot
candidate. Select the instance family from the bottleneck: a video-analysis
worker described as compute-intensive points to C-family, not R-family.

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

### EC2 network timeouts in tightly coupled workloads

Do not treat ENA, EFA, placement groups and EC2 Fleet as interchangeable.

| Requirement or symptom | First remediation | Why nearby answers lose |
|---|---|---|
| Tightly coupled grid/HPC nodes have inter-instance latency or timeout problems | Launch compatible instances together in a cluster placement group | Adding instances or using EC2 Fleet changes capacity acquisition, not proximity |
| General IP networking needs enhanced bandwidth, PPS and lower virtualization overhead | ENA on a supported instance/AMI; all Nitro instances use ENA | ENA does not control physical placement |
| MPI, NCCL, NIXL or Libfabric workload needs OS-bypass, low-latency high-throughput communication | EFA on supported instances, normally combined with a cluster placement group | EFA requires compatible instances, drivers/libraries and application communication model; it is not a transparent TCP timeout switch |
| Large distributed system needs correlated-hardware failure isolation | Partition placement group | Cluster placement optimizes communication rather than partitioning failure domains |
| Small number of critical instances must use distinct underlying hardware | Spread placement group | Spread has a small-per-AZ scale boundary and is not the HPC proximity choice |

Skill Builder-style clue chain:

```text
grid computing + network timeouts + adding nodes failed + AMI exists
  -> relaunch the nodes together in a cluster placement group
```

Choose EFA only when the stem supplies the HPC/ML communication-stack cue or
explicitly requires OS-bypass/RDMA-style capabilities.

For a tightly coupled HPC bundle, reconstruct all layers rather than treating
one feature as sufficient:

```text
supported Linux instance and EFA-aware AMI/libraries
  + EFA-capable instance type
  + one subnet / one Availability Zone
  + cluster placement group when offered
  + optional one thread per core when the workload benefits from disabled SMT
```

Disabling simultaneous multithreading (hyper-threading) can improve some HPC
workloads, but it is a CPU-performance tuning choice—not the mechanism that
raises network throughput. If a choose-three question offers EFA, single-AZ,
and disabled SMT but omits cluster placement, those three can be the intended
bundle by elimination. In a direct architecture question, cluster placement
remains the normal proximity control. Partition placement, burstable instances,
and paravirtual AMIs are not substitutes.

### Auto Scaling policy and metric selection

| Clue | Better answer |
|---|---|
| Breach magnitude varies sharply and choices are only simple versus step | Step scaling; define different adjustments for different breach ranges |
| One fixed adjustment followed by a cooldown | Simple scaling; slower to respond to successive large changes |
| Requests have roughly equal work | `RequestCountPerTarget` can track demand |
| A few requests can consume far more compute than others | `CPUUtilization` better reflects the actual constrained resource |

Target tracking is the normal AWS recommendation when it is offered. If it is
not offered and load can jump 2–10 times, step scaling responds more
proportionally than simple scaling. Always select the metric that correlates
with the bottleneck; request count is misleading when per-request cost varies.

For an ALB application with unpredictable peaks and no correlation between the
peaks and instance CPU, target tracking on `ALBRequestCountPerTarget` is the
stronger signal. Scheduled scaling requires known times, while predictive
scaling requires a recurring pattern that can be forecast. Configure the ASG
minimum/desired capacity to preserve the required multi-AZ baseline; Auto
Scaling then replaces an unhealthy or stopped instance.

For queue workers, raw queue depth alone does not vary inversely with ASG
capacity. Use backlog per instance—visible messages divided by in-service
workers—as a target-tracking custom metric when the answer requires a
proportional worker-scaling signal.

An existing SQS Standard queue cannot be converted in place to FIFO. Create a
new `.fifo` queue, configure deduplication/message-group behavior, and update
producers and consumers. FIFO deduplication reduces duplicate deliveries within
its deduplication scope, but consumers should still be idempotent.

Current Auto Scaling can derive backlog per instance with CloudWatch metric
math, avoiding a separately published custom metric. If an exam option instead
offers a custom backlog-per-instance metric, it remains superior to scaling on
raw `ApproximateNumberOfMessagesVisible` because it is proportional to worker
capacity and the acceptable processing delay.

### EC2 hibernation is a launch-time contract

Hibernation preserves RAM to the encrypted EBS root volume, allowing a much
faster resume than a fresh boot. It must be enabled **when the instance is
launched** and cannot be retrofitted onto an existing stopped instance. The
instance type, AMI, root volume type/size and Region must support it, and the
root volume must be encrypted and large enough for RAM.

For existing unencrypted instances, create or copy an encrypted AMI/snapshot
and launch replacement instances with hibernation enabled. Merely attaching an
encrypted volume to the old instances does not make them hibernation-capable.

### CloudFormation plus bootstrap automation

Use CloudFormation for the VPC, EC2 and RDS lifecycle and EC2 user data or a
managed configuration service for repeatable initial software installation.
This converts a manual runbook into versioned infrastructure and configuration
that can be updated through controlled stack changes. A custom SDK script can
work, but it recreates dependency ordering, rollback, drift and update logic
that CloudFormation already manages.

### Deployment and operations tool boundary

| Requirement | Service/pattern |
|---|---|
| Build/test/package source | CodeBuild |
| Orchestrate source, build, test, approval and deployment stages | CodePipeline |
| Blue/green, canary, linear or in-place deployment to supported compute | CodeDeploy |
| Provision repeatable infrastructure and detect stack drift | CloudFormation |
| Govern approved self-service infrastructure products | Service Catalog |
| Execute a command across managed nodes | Systems Manager Run Command |
| Interactive managed-node access | Systems Manager Session Manager |
| Multi-step operational automation with approvals/branches | Systems Manager Automation runbooks |

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
| “Add more grid-compute nodes to fix inter-node latency” | Change the placement/network architecture; a cluster placement group is the first proximity control. |
| “Configure EFA for any network timeout” | EFA is application-stack-specific; do not assume an ordinary TCP workload can use it transparently. |
| “Disable hyper-threading to increase network bandwidth” | SMT tuning may help HPC compute performance; EFA, supported instances and placement provide the network path. |

## Additional references

- EC2 placement groups: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/placement-groups.html
- Enhanced networking with ENA: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/enhanced-networking.html
- Elastic Fabric Adapter: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa.html
- EFA with MPI: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa-start.html
- EC2 CPU options and SMT: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-optimize-cpu.html
