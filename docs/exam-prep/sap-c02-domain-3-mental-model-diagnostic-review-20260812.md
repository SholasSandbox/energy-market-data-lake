# SAP-C02 Domain 3 Mental-Model Diagnostic Review - 2026-08-12

<!-- markdownlint-disable MD013 MD060 -->

## Result

| Measure | Result |
|---|---:|
| Overall | **25/25 (100%)** |
| Domain 3 questions | **20/20 (100%)** |
| Cross-domain controls | **5/5 (100%)** |
| Single-response questions | **16/16 (100%)** |
| Multiple-response questions | **9/9 exact match (100%)** |
| Time | **47 of 60 minutes; 13 minutes remaining** |
| Uncertain responses | **None recorded** |

The complete submission was frozen before marking. The learner reports a timed
closed-book attempt; that condition is learner-attested rather than
independently observable. This is strong focused diagnostic evidence, not a
full 75-question simulation and not booking-gate evidence.

## Domain 3 task coverage

The 20 Domain 3 questions were balanced across the five current Domain 3 task
areas.

| Domain 3 task area | Questions | Result |
|---|---|---:|
| 3.1 Determine a strategy to improve overall operational excellence | 1, 8, 14, 20 | **4/4** |
| 3.2 Determine a strategy to improve security | 2, 9, 15, 21 | **4/4** |
| 3.3 Determine a strategy to improve performance | 3, 10, 16, 22 | **4/4** |
| 3.4 Determine a strategy to improve reliability | 5, 11, 17, 24 | **4/4** |
| 3.5 Identify opportunities for cost optimizations | 6, 12, 19, 25 | **4/4** |

The five cross-domain controls were Questions 4, 7, 13, 18, and 23. All five
were correct, so the clean Domain 3 result did not depend on simply treating
every scenario as an existing-solution improvement question.

## Exact-match marking

| Q | Submitted | Key | Result |
|---:|---|---|---|
| 1 | ACE | ACE | Correct |
| 2 | B | B | Correct |
| 3 | C | C | Correct |
| 4 | BDF | BDF | Correct |
| 5 | D | D | Correct |
| 6 | B | B | Correct |
| 7 | A | A | Correct |
| 8 | CE | CE | Correct |
| 9 | C | C | Correct |
| 10 | BD | BD | Correct |
| 11 | D | D | Correct |
| 12 | B | B | Correct |
| 13 | C | C | Correct |
| 14 | AC | AC | Correct |
| 15 | BE | BE | Correct |
| 16 | D | D | Correct |
| 17 | AD | AD | Correct |
| 18 | A | A | Correct |
| 19 | C | C | Correct |
| 20 | D | D | Correct |
| 21 | BDF | BDF | Correct |
| 22 | B | B | Correct |
| 23 | A | A | Correct |
| 24 | A | A | Correct |
| 25 | CE | CE | Correct |

## Mental-model review

1. CodeDeploy blue/green for ECS needs the deployment group, test and
   production listeners, and CloudWatch-alarm-driven rollback as a complete
   mechanism.
2. Secrets Manager single-user rotation is the least-operational-effort fit
   when the scenario does not require an alternating-user strategy.
3. RDS Proxy absorbs connection churn and makes application recovery across a
   database failover less disruptive.
4. IAM Identity Center centralizes workforce access, SCPs establish
   organization guardrails, and an organization trail centralizes audit logs.
5. NAT gateways are zonal: use one per active Availability Zone and route each
   private subnet to its local NAT gateway; the internet gateway is regional.
6. S3 Intelligent-Tiering fits unknown or changing access patterns; S3 Glacier
   Deep Archive fits long retention with slow retrieval.
7. Application Migration Service handles server rehosting; Schema Conversion
   Tool plus Database Migration Service handles heterogeneous database change
   and data movement.
8. AWS Distro for OpenTelemetry with X-Ray provides distributed traces, while
   CloudWatch cross-account observability supports centralized operations.
9. AWS Config detects resource noncompliance and Systems Manager Automation can
   remediate it.
10. Cluster placement groups and Elastic Fabric Adapter address tightly coupled
    HPC latency and throughput requirements.
11. Service Quotas monitoring and proactive increase requests remove scaling
    ceilings before traffic reaches them.
12. Compute Savings Plans cover a flexible steady baseline; Spot Instances fit
    interruption-tolerant excess capacity.
13. Warm standby keeps a scaled-down functional environment, replicated data,
    tested failover, and traffic-routing readiness.
14. Fault Injection Service experiments need CloudWatch stop conditions so the
    blast radius stays bounded.
15. Systems Manager Session Manager removes inbound administration paths, while
    Patch Manager standardizes patch compliance.
16. DAX requires both a DAX cluster and application use of the DAX client;
    strongly consistent reads bypass the cache.
17. A dead-letter queue isolates poison messages, while idempotent processing
    protects against duplicate delivery.
18. Transit Gateway or peering provides network reachability, Direct Connect
    supplies the hybrid path, and Route 53 Resolver endpoints provide hybrid
    DNS resolution.
19. Cost-allocation tags plus a Cost and Usage Report in S3 queried by Athena
    provide attributable, detailed cost analysis.
20. Systems Manager State Manager associations enforce a desired configuration
    repeatedly and can be scaled across accounts.
21. Vault Lock compliance mode, isolated backup copies, and restore testing
    address immutable recovery rather than backup creation alone.
22. Global Accelerator supplies static anycast IP addresses, TCP/UDP routing,
    and health-based endpoint selection without DNS-cache delay.
23. Kinesis Data Streams uses partition keys for ordered shards and retention
    for replayable event history.
24. RDS Multi-AZ provides managed synchronous standby and automatic failover for
    a regional database availability improvement.
25. S3 and DynamoDB use gateway endpoints for private VPC access; Secrets
    Manager uses an interface endpoint with suitable security groups and private
    DNS.

## Interpretation and next action

No demonstrated Domain 3 knowledge gap needs a wrong-answer-log entry or an
immediate focused retest. The result supports all five Domain 3 mental models,
including exact-match selection discipline, under the bounded conditions of
this diagnostic.

Do not treat one focused 25-question result as proof of full-exam transfer.
Preserve the tracker sequence: **complete Full Mock 006 next**, then use Mocks
007 and 008 to test whether the Domain 3 models and prior service-boundary
remediation remain stable in independent mixed-domain conditions.
