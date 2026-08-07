# 04 - Containers: ECS, Fargate, EKS, ECR, and Batch

SAP-C02 often tests container decisions through operational constraints: Kubernetes requirement, host control, cost, scaling, deployment model, private networking, and security role separation.

## Core choices

| Requirement | Service choice |
|---|---|
| Simple AWS-native container orchestration | ECS |
| Run containers without managing EC2 hosts | Fargate |
| Kubernetes API/ecosystem required | EKS |
| Batch scheduling and managed job queues | AWS Batch |
| Container image registry | ECR |
| Host-level control, custom agents, GPU, privileged workloads | ECS/EKS on EC2 or plain EC2 |

## ECS

### What it is

Amazon Elastic Container Service (ECS) is an AWS-native container orchestrator.

### Building blocks

| Component | Meaning |
|---|---|
| Cluster | Logical grouping of capacity and services |
| Task definition | Blueprint: containers, image, CPU/memory, ports, env vars, roles |
| Task | Running copy of a task definition |
| Service | Maintains desired number of tasks and integrates with load balancers |
| Capacity provider | Defines compute capacity strategy: Fargate, Fargate Spot, EC2 ASG |
| Execution role | Allows ECS agent/Fargate to pull image, write logs, fetch secrets |
| Task role | IAM role used by application code inside the container |

### ECS exam triggers

Choose ECS when:

- app is already containerized
- no Kubernetes requirement exists
- AWS-native orchestration is acceptable
- simpler operations than EKS are preferred
- service needs ALB/NLB integration
- workers need to run from SQS/EventBridge
- containers need longer runtime than Lambda

## Fargate

### What it is

Fargate runs containers without requiring you to manage EC2 instances. With ECS on Fargate, you define CPU/memory, networking, IAM policies, and containers; AWS manages the underlying compute.

### Choose Fargate when

- you do not want to manage instances
- workloads are containerized services or tasks
- scaling is by task count
- workload does not need host-level access
- task-level isolation is useful
- operational simplicity matters more than lowest possible unit cost

### Avoid Fargate when

- workload needs privileged containers or host-level agents
- workload needs GPUs or specific EC2 instance families
- very large steady-state fleet would be cheaper with EC2 capacity optimization
- Kubernetes DaemonSets or node-level customization are required
- application requires unsupported task definition parameters

### Fargate networking

Fargate tasks use `awsvpc` networking. Each task receives an elastic network interface in a subnet.

Implications:

- security groups apply directly to tasks
- private subnet tasks need outbound access via NAT Gateway or VPC endpoints
- ECS tasks pulling from ECR in private subnets usually need ECR API, ECR Docker, S3, and CloudWatch Logs path access
- ALB target groups commonly use IP target type for Fargate

### ECS/Fargate pattern

```text
Route 53
  -> CloudFront/WAF
  -> ALB
  -> ECS Service on Fargate in private subnets
  -> RDS/Aurora/DynamoDB
  -> CloudWatch Logs
```

### Queue worker pattern

```text
SQS queue
  -> ECS service workers on Fargate
  -> DynamoDB/RDS/S3
  -> DLQ for poison messages
```

Use this when Lambda is too short-lived or unsuitable for heavy dependencies.

## EKS

### What it is

Amazon Elastic Kubernetes Service (EKS) is managed Kubernetes. AWS manages the Kubernetes control plane; you manage or select worker compute.

### Choose EKS when

- Kubernetes is explicitly required
- teams use Kubernetes manifests, Helm, operators, CRDs
- portability across Kubernetes environments matters
- platform engineering standardizes on Kubernetes
- workloads require Kubernetes-native networking/policy/controllers

### Avoid EKS when

- the scenario only says “containers” and does not require Kubernetes
- lowest operational complexity is required
- small team lacks Kubernetes operational skills
- ECS/Fargate satisfies all workload needs

### EKS compute options

| Option | Use when |
|---|---|
| Managed node groups | General Kubernetes workloads with EC2 node management handled partly by AWS |
| Self-managed nodes | Maximum node customization |
| Fargate profiles | Pod-level serverless compute for supported workloads |
| Karpenter | Flexible node provisioning and consolidation patterns |

### EKS traps

- EKS is not automatically serverless; pods need compute.
- EKS Fargate is not a universal replacement for EC2 nodes.
- DaemonSet-heavy workloads usually need EC2 nodes.
- Kubernetes adds operational surface area: cluster add-ons, ingress controller, IAM roles for service accounts, network policies, upgrades.

## AWS Batch

### What it is

AWS Batch schedules and runs batch computing jobs on managed compute environments. It can use EC2 or Fargate-backed compute.

### Choose Batch when

- workload is job-oriented, not service-oriented
- jobs need queues, priorities, dependencies, retries
- large batch processing needs managed scheduling
- scientific/HPC/ETL/media jobs need scalable compute

### Batch vs ECS RunTask vs Lambda

| Requirement | Better fit |
|---|---|
| One-off or scheduled container task with simple trigger | ECS RunTask |
| Complex job queues, dependencies, priorities | AWS Batch |
| Short event-driven function | Lambda |
| Long-running web service | ECS Service/EKS Deployment |

## ECR

### What it is

Elastic Container Registry stores container images.

### Exam points

- ECS/EKS/Fargate pull images from ECR.
- Private subnet workloads need network path to ECR and S3 layer storage.
- Use image scanning and lifecycle policies.
- Use immutable tags or digest pinning for production deployment safety.
- Separate build role from runtime task role.

## Deployment patterns

| Pattern | Services | Use when |
|---|---|---|
| Rolling deployment | ECS service / EKS deployment | Basic zero-downtime within service |
| Blue/green | CodeDeploy + ECS/ALB, weighted target groups | Safer release with rollback |
| Canary | ALB weighted target groups, Route 53 weighted, CodeDeploy Lambda/ECS | Gradual exposure |
| Immutable | New environment, then shift traffic | Maximum rollback clarity |
| Sidecar | ECS/EKS task/pod with helper container | Proxy/logging/service mesh patterns |

## Security

| Control | ECS/Fargate implication |
|---|---|
| Task role | App permissions to AWS APIs |
| Execution role | Pull image, write logs, get secrets at startup |
| Secrets Manager/SSM Parameter Store | Avoid plaintext secrets in env vars |
| Security groups | Task-level ingress/egress for Fargate |
| Private subnets | Keep tasks off public internet |
| VPC endpoints | Reduce NAT dependency and keep AWS API traffic private |
| ECR scan/lifecycle | Image vulnerability and storage hygiene |

## Observability

Minimum telemetry:

- Container logs to CloudWatch Logs.
- ECS service events.
- ALB target health and 5xx metrics.
- CPU/memory utilization.
- Task restart count.
- Deployment failures.
- Queue depth if workers process SQS.
- X-Ray/OpenTelemetry for distributed tracing where useful.

## Exam traps

| Trap | Correction |
|---|---|
| “Containers mean EKS” | ECS is simpler unless Kubernetes is required. |
| “Fargate means no VPC design” | Fargate tasks still need subnets, security groups, routing, endpoints/NAT. |
| “Execution role and task role are the same” | Execution role is platform operations; task role is application permissions. |
| “Lambda is always cheaper for background work” | Long-running/heavy workers may be better on Fargate/ECS/Batch. |
| “EKS Fargate supports all Kubernetes patterns” | Some node-level patterns need EC2 nodes. |
| “Private subnet task can always pull ECR image” | It needs NAT or VPC endpoints and permissions. |
| “Desired count equals available capacity” | On EC2-backed ECS/EKS, node capacity still matters. |

## Source references

- ECS/Fargate architecture: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/AWS_Fargate.html
- ECS capacity providers for Fargate: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/fargate-capacity-providers.html
