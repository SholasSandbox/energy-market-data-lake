# SAP-C02 Timed Mixed Diagnostic Review - 2026-07-21

<!-- markdownlint-disable MD013 MD060 -->

## Evidence Role

**Document role:** answer-bearing post-submission review. The frozen learner
answers remain in the
[question-and-submission artifact](sap-c02-mixed-diagnostic-30q-20260720.md).
This review was created only after explicit submission.

## Result

| Metric | Result |
|---|---:|
| Questions | 30 |
| Correct | 29 |
| Incorrect | 1 |
| Score | 96.7% |
| Single-choice | 17/18 |
| Multiple-response | 12/12 |
| Time used | 67 of 72 minutes |
| Time remaining | 5 minutes |

This is strong timed mixed-domain evidence, including a clean multiple-response
result. It is not a full 75-question simulation and does not independently
satisfy the booking gate.

## Answer Reconciliation

| Question | Submitted | Key | Result | Domain |
|---:|:---:|:---:|:---:|---|
| 1 | AB | AB | Correct | 1 |
| 2 | D | B | Incorrect | 2 |
| 3 | A | A | Correct | 3 |
| 4 | C | C | Correct | 4 |
| 5 | AB | AB | Correct | 1 |
| 6 | C | C | Correct | 2 |
| 7 | B | B | Correct | 3 |
| 8 | AC | AC | Correct | 4 |
| 9 | D | D | Correct | 1 |
| 10 | AC | AC | Correct | 2 |
| 11 | C | C | Correct | 3 |
| 12 | AB | AB | Correct | 4 |
| 13 | D | D | Correct | 1 |
| 14 | AC | AC | Correct | 2 |
| 15 | B | B | Correct | 3 |
| 16 | C | C | Correct | 4 |
| 17 | ACE | ACE | Correct | 1 |
| 18 | BD | BD | Correct | 2 |
| 19 | C | C | Correct | 3 |
| 20 | B | B | Correct | 4 |
| 21 | AD | AD | Correct | 1 |
| 22 | C | C | Correct | 2 |
| 23 | AC | AC | Correct | 3 |
| 24 | D | D | Correct | 4 |
| 25 | ABE | ABE | Correct | 1 |
| 26 | B | B | Correct | 2 |
| 27 | C | C | Correct | 3 |
| 28 | A | A | Correct | 2 |
| 29 | AD | AD | Correct | 1 |
| 30 | B | B | Correct | 2 |

## Domain Signal

| Domain | Questions | Result | Boundary |
|---|---:|---:|---|
| Domain 1: Design Solutions for Organizational Complexity | 8 | 8/8 | Clean within this diagnostic only |
| Domain 2: Design for New Solutions | 9 | 8/9 | One deployment-strategy miss |
| Domain 3: Continuous Improvement for Existing Solutions | 7 | 7/7 | Clean within this diagnostic only |
| Domain 4: Accelerate Workload Migration and Modernization | 6 | 6/6 | Clean within this diagnostic only |

The distribution approximates the official SAP-C02 weighting. These are small
per-domain samples, so the results identify a remediation target rather than
proving complete domain mastery.

## Miss Review: Question 2

### Winning requirement

The new ECS revision must be tested beside the current revision, traffic must
shift through the load balancer, and failed health or business metrics must
cause automatic traffic rollback.

### Correct answer

**B. Use an AWS CodeDeploy blue/green deployment with test and production
listeners plus automatic rollback alarms.**

An ECS blue/green deployment keeps the original task set available while the
replacement task set is validated. CodeDeploy can control load-balancer traffic
shifting and reroute traffic to the original task set when deployment failure
or configured alarm conditions trigger rollback.

### Why submitted answer D loses

AWS Systems Manager Patch Manager automates operating-system and supported
application patching on managed nodes. It is not the service that creates an
ECS replacement task set, shifts Application Load Balancer traffic between
task sets, evaluates the application rollout, or restores production traffic
to the previous ECS revision.

### Durable decision rule

> For an ECS application release requiring side-by-side validation, controlled
> load-balancer traffic shifting, and automatic rollback, choose a blue/green
> deployment strategy. Patch Manager solves managed-node patch compliance, not
> container application revision deployment.

### Spaced retest prompt

An ECS service must expose a new task definition to a test listener, shift
production traffic only after validation, and restore the original task set if
an alarm enters `ALARM`. Which deployment service or strategy owns this
workflow, and why does Patch Manager not satisfy it?

## Compact Rationale Key

| Question | Decision rule |
|---:|---|
| 1 | IAM Identity Center grants centrally managed workforce access; an SCP supplies the organization-leaving guardrail but grants no permissions. |
| 2 | Blue/green deployment supplies side-by-side ECS revisions, controlled ALB traffic shifting, and rollback. |
| 3 | AWS Config evaluates the configuration continuously; Systems Manager Automation supplies remediation. |
| 4 | AWS Application Migration Service is the rehost mechanism for continuously replicated supported servers. |
| 5 | An organization trail centralizes events; a separately governed Object Lock design protects retained log objects. |
| 6 | A complete, functional, reduced-capacity recovery environment is warm standby. |
| 7 | Secrets Manager provides managed storage and rotation; the task role supplies retrieval authorization. |
| 8 | Snowball Edge handles the offline bulk copy; DataSync transfers the final file deltas. |
| 9 | Resolver inbound/outbound endpoints and rules solve hybrid DNS; Direct Connect and Transit Gateway supply transport. |
| 10 | Kinesis partition keys preserve per-key ordering, while retention and independent consumers support replay and fan-out. |
| 11 | Rightsize first, cover the stable baseline with commitment pricing, and use Spot only for interruption-tolerant bursts. |
| 12 | AWS SCT converts heterogeneous schemas and code; AWS DMS full load plus CDC moves and synchronizes data. |
| 13 | Transit Gateway route tables provide transitive segmentation; a Direct Connect gateway integrates the private hybrid path. |
| 14 | AWS WAF filters application-layer attacks; Shield Advanced provides the stated enhanced DDoS protection and response support. |
| 15 | Aurora Replicas and the reader endpoint offload read traffic from the writer. |
| 16 | Replacing a purchased application with a SaaS product is repurchase. |
| 17 | Cost allocation tags, Cost Categories, and Cost Anomaly Detection address attribution, grouping, and unusual-spend alerts. |
| 18 | Global Accelerator supplies static anycast ingress; DynamoDB global tables supply multi-Region active-active key-value data. |
| 19 | Standard queues can redeliver; idempotency, visibility management, and a DLQ control duplicate effects and poison messages. |
| 20 | DataSync is designed for online file transfer with metadata handling, incremental copies, and verification. |
| 21 | Cross-account KMS use requires permission in the key-policy boundary and permission for the calling role. |
| 22 | Gateway endpoints are the low-additional-cost S3/DynamoDB path; unsupported public IPv4 destinations still need reviewed egress. |
| 23 | Drift detection finds unmanaged changes; change sets expose planned infrastructure changes before execution. |
| 24 | API Gateway, Lambda, and SQS provide managed request handling, elastic compute, and asynchronous buffering. |
| 25 | Separately administered copies, immutable retention, and restore testing address compromise, deletion, and recoverability. |
| 26 | Service quotas are independent scaling constraints that must be monitored and raised before forecast demand arrives. |
| 27 | Organization CloudTrail preserves API identity; EventBridge routes matching events into a controlled remediation workflow. |
| 28 | S3 File Gateway exposes file protocols with a managed local cache backed by S3 objects. |
| 29 | AWS RAM and Organizations enable VPC subnet sharing while the owner retains network controls. |
| 30 | CloudFront caches static content; Global Accelerator supplies fixed anycast ingress for healthy regional TCP endpoints. |

## Tracker Outcome

The timed 30-question diagnostic is complete at 29/30 in 67 minutes. One new
wrong-answer theme is recorded: ECS blue/green deployment and rollback versus
Systems Manager Patch Manager. The next scheduled tracker item remains the
fresh question-only Resilience/DR retest on 2026-07-27, followed by migration
foundations.

## Official AWS References

- [SAP-C02 exam guide and domain weighting](https://docs.aws.amazon.com/aws-certification/latest/solutions-architect-professional-02/solutions-architect-professional-02.html)
- [CodeDeploy blue/green deployments for Amazon ECS](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/deployment-type-bluegreen.html)
- [Amazon ECS deployments with CodeDeploy and rollback](https://docs.aws.amazon.com/codedeploy/latest/userguide/deployment-steps-ecs.html)
- [AWS Systems Manager Patch Manager](https://docs.aws.amazon.com/systems-manager/latest/userguide/patch-manager.html)
