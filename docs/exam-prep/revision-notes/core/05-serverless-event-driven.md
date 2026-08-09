# 05 - Serverless and Event-Driven Architecture

**Last revised:** 2026-08-09

This chapter covers Lambda, SQS, SNS, EventBridge, Step Functions, API Gateway, and common failure patterns.

## Service selection

| Requirement | Service |
|---|---|
| Short stateless event handler | Lambda |
| Durable queue and backpressure | SQS |
| Pub/sub broadcast | SNS |
| Event routing and filtering | EventBridge |
| Stateful workflow/orchestration | Step Functions |
| Public/private API front door | API Gateway |
| Long-running containerized async worker | ECS/Fargate |
| Stream processing | Kinesis / Managed Service for Apache Flink |
| Existing ActiveMQ/RabbitMQ/protocol-compatible broker workload | Amazon MQ |
| Managed GraphQL API and real-time subscriptions | AWS AppSync |

## Lambda

### Choose Lambda when

- execution is short-lived and event-driven
- scaling per request/event is useful
- operational overhead should be minimal
- integrations trigger functions directly
- workload fits Lambda runtime, memory, package, and duration constraints

### Avoid Lambda when

- long-running job exceeds limits
- sustained compute is more cost-effective on containers
- host-level customization is required
- workload needs persistent local process state
- cold starts violate latency requirements and cannot be mitigated

### Lambda concurrency

Key concepts:

- **Reserved concurrency**: caps and guarantees concurrency for a function.
- **Provisioned concurrency**: pre-initializes execution environments for lower cold-start latency.
- **Account concurrency**: regional concurrency pool.
- **Event source mapping scaling**: queue/stream sources scale according to their source semantics.

### Lambda traps

- Retrying async Lambda events can create duplicate processing.
- Functions must be idempotent.
- VPC-enabled Lambda can access private resources but needs networking design.
- SQS/Lambda requires visibility timeout aligned with function timeout.
- Stream sources can retry failed batches and increase iterator age.

## SQS

### What it is

SQS is a durable managed queue. It decouples producers and consumers.

### Standard queue

Use when:

- high throughput is required
- duplicate/out-of-order messages can be handled
- idempotency is implemented
- exact order is not mandatory

### FIFO queue

Use when:

- order matters
- message groups can partition ordered work
- deduplication is required
- lower throughput constraints are acceptable or high-throughput FIFO mode fits

### DLQ

Use a dead-letter queue for poison messages that repeatedly fail processing.

Controls:

- redrive policy
- `maxReceiveCount`
- DLQ retention longer than source queue retention
- CloudWatch alarm on visible messages in DLQ
- manual or controlled redrive after fixing root cause

### Visibility timeout

Visibility timeout must exceed the expected processing time. If it is too short, another worker can process the same message before the first worker finishes.

### SQS traps

| Trap | Correction |
|---|---|
| “SQS guarantees exactly-once for standard queues” | Standard queues are at-least-once and best-effort order. |
| “FIFO gives global parallelism automatically” | Parallelism is by message group. |
| “DLQ fixes poison messages” | DLQ isolates failures; you still need root-cause fix and replay plan. |
| “Queue depth alone proves latency” | Use age of oldest message and consumer metrics. |
| “Visibility timeout is retry delay” | It controls message invisibility after receive. |

## SNS

### What it is

SNS is pub/sub notification and fanout.

### Choose SNS when

- one event must go to many subscribers
- SMS/email/mobile push is needed
- fanout to multiple SQS queues is needed
- message filtering at subscription level helps reduce consumer work

### Pattern

```text
OrderCreated topic
  -> SQS queue for billing
  -> SQS queue for shipping
  -> Lambda for notification
  -> HTTPS endpoint for partner integration
```

### Trap

SNS does not provide the same buffering semantics to all endpoint types. For durable fanout to application workers, use **SNS -> SQS**.

## EventBridge

### What it is

EventBridge routes events from AWS services, custom apps, and SaaS sources to targets using rules and event patterns.

### Choose EventBridge when

- event bus decoupling is required
- SaaS/AWS/app events need routing
- schema discovery or archive/replay is useful
- scheduled events are needed
- cross-account event routing is needed

### Avoid EventBridge when

- strict ordering is required
- high-throughput stream analytics is required
- a simple work queue is enough
- a multi-step workflow is the core requirement

## Amazon MQ

Choose Amazon MQ when an existing application depends on Apache ActiveMQ
Classic or RabbitMQ semantics, clients, or protocols such as JMS, AMQP, MQTT,
OpenWire, or STOMP and the migration should minimize application change.

Do not select it merely because the stem says “messages”. For a new AWS-native
workload, first compare SQS, SNS and EventBridge because they avoid broker
topology, sizing and protocol management.

```text
legacy/protocol contract must remain -> Amazon MQ
durable cloud-native work queue      -> SQS
cloud-native pub/sub fanout          -> SNS
event routing/filtering              -> EventBridge
```

Migration bundle rule: preserve the existing ActiveMQ contract with Amazon MQ
when least change is required; moving to SQS is a refactor because clients and
delivery semantics change. Rehost executable PHP/worker tiers on compute rather
than placing server-side code in an S3 static website. If the existing data is
JSON/document-oriented and the offered managed targets are DynamoDB or a
relational redesign, DynamoDB is normally the lower-model-change target—but the
processing application must still be modified and its access patterns checked.

### Fast durable HTTP acceptance with asynchronous workers

```text
unchanged HTTP client
  -> API Gateway direct AWS service integration
  -> SQS durable queue
  -> parallel worker fleet
  -> durable database
```

Use this pattern when clients need a quick response but processing takes much
longer. API Gateway can acknowledge the durable SQS handoff without holding the
HTTP connection open. Scale EC2/ECS workers from backlog per worker. A
synchronous Lambda proxy that performs 90-second work does not meet a client
timeout under 10 seconds, even though the work is below Lambda's maximum
duration.

### Centralize events, not duplicate handlers

For the same operational event emitted by workloads in many accounts:

```text
member-account EventBridge rules
  -> central event bus with a permitting resource policy
  -> one central Lambda handler
```

Use this when a central team owns the target data and cleanup logic. For
Auto Scaling termination events, the event contains the instance identifier,
which can map to an S3 object prefix. Lambda is lower overhead than maintaining
EC2 or ECS workers for short, sporadic cleanup that must complete within
minutes. Separate Lambda functions in every member account duplicate code,
roles, deployment and monitoring.

For short Java order processing where servers must not be managed, SQS plus
Lambda is a valid capture/processing layer. Preserve an explicitly required
Oracle operating model with Multi-AZ RDS for Oracle rather than converting the
database engine without a modernization requirement.

## AWS AppSync

Choose AppSync when the application needs a managed GraphQL API, resolver-based
access to data sources, fine-grained API authorization, or GraphQL
subscriptions for live updates. AppSync manages the WebSocket connections for
subscriptions.

Do not choose AppSync only because an application needs any WebSocket. API
Gateway also provides WebSocket APIs; GraphQL schema/resolver/subscription
semantics are the stronger AppSync cues.

## Step Functions

### What it is

Step Functions coordinates workflows using state machines.

### Choose Step Functions when

- workflow has multiple steps
- retries/catches/branching/timeouts are required
- human approval/manual step is required
- long-running orchestration is needed
- Lambda chaining would become fragile

### Standard vs Express

| Type | Use when |
|---|---|
| Standard | Long-running, durable, auditable workflows |
| Express | High-volume short workflows where cost/throughput matters |

### Trap

Step Functions orchestrates work; it does not replace Kinesis for streaming or SQS for queue buffering.

## API Gateway

### Choose API Gateway when

- API authentication/authorization/throttling is required
- request/response transformations are useful
- usage plans/API keys are needed
- Lambda/private integration backend is used
- WebSocket API is needed

### API Gateway vs ALB

| Requirement | Better fit |
|---|---|
| REST API management, throttling, authorizers | API Gateway |
| HTTP routing to container microservices | ALB |
| Web app behind HTTP load balancer | ALB |
| Lambda API with auth and rate limiting | API Gateway |
| Cheapest simple HTTP entry to ECS service | Often ALB, depending traffic/profile |

## Event-driven architecture patterns

### Async web request offload

```text
Client -> API Gateway/ALB -> Lambda/ECS API
  -> SQS
  -> worker Lambda/ECS
  -> DB/S3
  -> DLQ
```

### Fanout with isolated consumers

```text
Producer -> SNS topic
  -> SQS queue A -> Consumer A
  -> SQS queue B -> Consumer B
  -> SQS queue C -> Consumer C
```

### Workflow

```text
EventBridge rule
  -> Step Functions
      -> Lambda validate
      -> ECS task process
      -> DynamoDB update
      -> SNS notify
```

### Streaming

```text
Producer -> Kinesis Data Streams
  -> Lambda/Flink/ECS consumers
  -> S3 archive
```

## Source references

- Lambda event source mappings: https://docs.aws.amazon.com/lambda/latest/dg/invocation-eventsourcemapping.html
- SQS queue types: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-types.html
- SQS dead-letter queues: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- Amazon MQ overview: https://docs.aws.amazon.com/amazon-mq/latest/developer-guide/welcome.html
- AppSync subscriptions: https://docs.aws.amazon.com/appsync/latest/devguide/aws-appsync-real-time-data.html
