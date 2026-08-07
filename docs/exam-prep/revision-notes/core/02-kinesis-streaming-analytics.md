# 02 - Kinesis, Streaming, and Real-Time Analytics

This is a priority chapter because a Kinesis stream question was missed in practice. SAP-C02 often tests whether you can distinguish **streaming**, **queuing**, **fanout**, **delivery**, and **analytics**.

## Core mental model

| Need | Service |
|---|---|
| Replayable real-time ordered event stream with custom consumers | **Kinesis Data Streams (KDS)** |
| Managed delivery of streaming records to destinations such as S3/Redshift/OpenSearch/HTTP endpoints | **Kinesis Data Firehose** |
| Stateful stream processing, SQL/windowing/Flink applications | **Amazon Managed Service for Apache Flink**; older notes may say **Kinesis Data Analytics** |
| Kafka API/ecosystem compatibility | **Amazon MSK** |
| Work queue with retries/backpressure | **Amazon SQS** |
| Publish/subscribe fanout | **Amazon SNS** |
| Event routing between AWS services, SaaS, and applications | **Amazon EventBridge** |

## Kinesis Data Streams

### What it is

**Kinesis Data Streams** is a managed service for collecting and processing streams of records in real time. Producers write records to a stream. Consumers read records from shards.

### Exam trigger phrases

Choose **Kinesis Data Streams** when the scenario says:

- real-time data ingestion
- clickstream, IoT telemetry, market data, application logs
- multiple independent consumers
- ordered processing by key
- replay/reprocess events
- custom stream processing
- sub-second or seconds-level processing
- Lambda or EC2 consumers processing stream records
- scaling by shards or partition keys

### Architecture pattern

```text
Producers
  -> Kinesis Data Streams
      -> Consumer A: Lambda updates DynamoDB
      -> Consumer B: Firehose archives to S3
      -> Consumer C: Flink app computes windows/aggregates
      -> Consumer D: EC2/ECS app runs custom processing
```

### Key terms

| Term | Meaning | Exam implication |
|---|---|---|
| Stream | Named sequence of records | The durable event stream |
| Record | Data blob + partition key + sequence number | The processing unit |
| Partition key | Hash key used to place records into shards | Controls ordering and shard distribution |
| Shard | Capacity and ordering unit | Scale, ordering, and throughput are shard-shaped |
| Sequence number | Unique identifier within shard | Helps ordering and checkpointing |
| Producer | Writes records | Must handle throttling/retries |
| Consumer | Reads records | Can be Lambda, KCL app, Flink, Firehose, custom app |
| Checkpoint | Consumer progress marker | Avoids reprocessing from the beginning |
| Iterator age | Consumer lag signal | High value means consumers are falling behind |

### Shards and ordering

Kinesis preserves ordering **within a shard**, and records with the same partition key map to the same shard until resharding changes the shard topology.

Implications:

- Need ordering per `customer_id` or `device_id` -> use that as the partition key.
- Need global ordering across all records -> difficult at scale; Kinesis is partitioned.
- Hot partition key -> one shard becomes overloaded while others are underused.
- More shards increase throughput but can complicate consumer scaling and ordering assumptions.

### Throughput model

Current AWS documentation states that a shard supports up to **1 MB/sec or 1,000 records/sec write throughput**, and up to **2 MB/sec or 2,000 records/sec read throughput** through shared consumers. AWS documentation also describes larger intermittent record payload handling and Data Plane API limits; always verify exact quota wording because AWS service quotas can change.

Exam-safe model:

```text
More producers / more write throughput -> more shards or on-demand mode
More consumers / read contention -> enhanced fan-out or separate delivery path
One partition key too hot -> redesign partition key
Consumer lag rising -> scale consumers, tune batch/parallelization, or increase read path capacity
```

### Consumer types

| Consumer type | How it reads | Use when |
|---|---|---|
| Shared throughput consumer | Polls via `GetRecords`; consumers share per-shard read throughput | Few consumers, cost-conscious, moderate fanout |
| Enhanced fan-out consumer | Dedicated per-consumer per-shard throughput using push-style SubscribeToShard | Multiple consumers need independent low-latency reads |
| Lambda event source mapping | Lambda polls or uses enhanced fan-out depending configuration | Event-driven processing without managing consumer fleet |
| KCL application | Kinesis Client Library manages shard leases/checkpointing | Custom app logic on EC2/ECS/EKS |
| Firehose as consumer | Reads stream and delivers to destination | Archive/delivery path without custom consumer logic |
| Flink app | Reads stream and performs stateful analytics | Windows, joins, aggregations, anomaly/event processing |

### Enhanced fan-out

Use **enhanced fan-out (EFO)** when multiple consumers contend for read throughput or need lower-latency dedicated reads.

Exam cues:

- many consumer applications
- one consumer must not slow another
- low-latency processing for each consumer
- “dedicated throughput per consumer”

Trade-off: EFO costs more than shared polling. Do not choose it when there is one lightweight consumer and cost is prioritized.

### Retention and replay

Kinesis Data Streams stores records for a retention window. This makes replay possible within the retention period.

Exam cues for retention/replay:

- reprocess last N hours/days of events
- recover from failed downstream consumer
- multiple independent consumers start at different positions
- audit or re-drive stream processing

Do not confuse this with SQS DLQ redrive. SQS is a queue; Kinesis is a stream.

### Kinesis Data Streams vs SQS

| Requirement | Kinesis Data Streams | SQS |
|---|---|---|
| Multiple independent consumers replay same data | Strong fit | Weak fit unless SNS fanout to multiple queues |
| Ordered per partition key | Strong fit | FIFO queue gives order per message group |
| Work queue where each message should be processed by one worker | Weak fit | Strong fit |
| Backpressure buffer for background jobs | Possible but not ideal | Strong fit |
| Stream analytics | Strong fit | Weak fit |
| Consumer group style replay | Strong fit | Not the core model |
| Simple decoupling and retries | Overkill | Strong fit |

### Kinesis Data Streams vs Firehose

| Requirement | Kinesis Data Streams | Firehose |
|---|---|---|
| Custom consumers | Yes | Limited transformation/delivery model |
| Replay | Yes, within retention | No general replay stream semantics |
| Multiple independent consumers | Yes | Not the main purpose |
| Minimal ops delivery to S3/Redshift/OpenSearch | Can, but more work | Strong fit |
| Sub-second custom processing | Strong fit | Usually not |
| Buffer/compress/convert/deliver | Needs consumer logic | Built-in delivery capabilities |

Exam trap:

> “Real-time clickstream data must be processed by several independent applications and reprocessed if a consumer fails.”

Answer: **Kinesis Data Streams**, not Firehose.

> “Streaming application logs should be delivered to S3 with minimal operational overhead.”

Answer: **Kinesis Data Firehose**, not Kinesis Data Streams unless custom replay/consumers are required.

## Kinesis Data Firehose

### What it is

Firehose is a managed streaming delivery service. It receives streaming records, buffers them, optionally transforms them, and delivers them to destinations.

### Choose Firehose when

- delivery to S3/Redshift/OpenSearch/Splunk/HTTP endpoint is the main goal
- minimal operations are required
- near-real-time delivery is acceptable
- compression, buffering, format conversion, and delivery retry are useful
- Lambda transformation is enough

### Avoid Firehose when

- multiple custom consumers need to read the same event stream
- the app needs replay semantics
- processing must happen with very low latency per record
- complex stateful stream processing is required

### Common pattern

```text
Application logs
  -> Firehose
  -> S3 raw prefix
  -> Glue crawler / Glue Data Catalog
  -> Athena / Redshift Spectrum / QuickSight
```

Or:

```text
Kinesis Data Streams
  -> Consumer A: real-time fraud scoring
  -> Firehose: archive all events to S3
```

## Managed Service for Apache Flink / Kinesis Data Analytics

### What it is

This is for stateful stream processing: windowed aggregations, event-time processing, joins, anomaly detection, and stream enrichment.

### Choose it when

- SQL-like or Flink-based stream processing is required
- you need tumbling/sliding/session windows
- output goes to another stream, S3, OpenSearch, or a dashboard path
- business logic requires continuous analytics, not just delivery

### Avoid it when

- simple delivery to S3 is enough -> Firehose
- simple event routing is enough -> EventBridge
- simple work queue is enough -> SQS
- batch ETL is enough -> Glue

## Amazon MSK vs Kinesis

| Requirement | Better fit |
|---|---|
| Existing Kafka clients, Kafka APIs, Kafka ecosystem | Amazon MSK |
| AWS-native, lower operational burden, shard-based managed stream | Kinesis Data Streams |
| Need Kafka Connect ecosystem or strict Kafka compatibility | MSK |
| No Kafka dependency, simpler AWS-native stream | Kinesis |
| Self-managed Kafka migration with minimal app change | MSK |

## Lambda with Kinesis

### How it works

Lambda can consume Kinesis records through an event source mapping. Lambda batches records and invokes the function.

### Important settings

| Setting | Why it matters |
|---|---|
| Batch size | Larger batches improve throughput but increase retry blast radius |
| Maximum batching window | Trades latency for batching efficiency |
| Parallelization factor | More concurrent batches per shard, useful for high throughput but may affect ordering assumptions |
| Bisect batch on error | Helps isolate poison records |
| Maximum record age | Drops old records after configured age |
| On-failure destination | Captures failed batches/metadata |
| Partial batch response | Allows successful records to be checkpointed while failed records retry |

### Failure mode

```text
Bad record in batch
  -> Lambda fails whole batch
  -> same batch retries
  -> iterator age rises
  -> downstream becomes stale
```

Mitigations:

- implement partial batch response
- use idempotent processing
- use smaller batch size for sensitive workloads
- configure failure destination
- alarm on iterator age and function errors

## DynamoDB Streams vs Kinesis Data Streams

| Requirement | DynamoDB Streams | Kinesis Data Streams |
|---|---|---|
| React to item-level table changes | Strong fit | Possible via DynamoDB Kinesis streaming but more setup |
| General app event stream | Weak fit | Strong fit |
| Long retention/replay | Limited compared with Kinesis options | Stronger fit |
| Multiple downstream apps consuming table changes at scale | Kinesis integration can help | Strong fit |
| Per-item change trigger to Lambda | Strong fit | Not the first choice |

## Observability

Watch these metrics:

| Metric | What it tells you |
|---|---|
| `WriteProvisionedThroughputExceeded` | Producers are throttled; shard count/partition design issue |
| `ReadProvisionedThroughputExceeded` | Consumers are throttled; EFO or more capacity may be needed |
| `GetRecords.IteratorAgeMilliseconds` | Consumer lag |
| Lambda `IteratorAge` | Stream consumer is falling behind |
| Lambda errors/throttles | Function or concurrency issue |
| Firehose delivery failures | Destination, permissions, transform, or buffering issue |

## Security

Minimum design controls:

- Use IAM least privilege for producers and consumers.
- Use KMS encryption where required.
- Use VPC endpoints for private access when workloads are in private subnets.
- Protect downstream destinations such as S3 with bucket policies and KMS key policies.
- Log producer/consumer errors and throttle metrics.

## Kinesis exam traps

| Trap | Correct thinking |
|---|---|
| “Kinesis is a queue” | No. It is a stream. Consumers read records; records can be replayed within retention. |
| “Use Firehose for custom stream processing” | Firehose is delivery-focused. Use Data Streams for custom consumers. |
| “Use SQS for clickstream analytics with many consumers” | SQS is a work queue. Use Kinesis Data Streams for stream fanout/replay. |
| “One partition key for all events” | Hot shard risk. Use a partition key with enough cardinality. |
| “More Lambda concurrency solves all Kinesis lag” | Concurrency is shard-shaped; per-shard ordering and parallelization settings matter. |
| “Enhanced fan-out is always best” | It improves dedicated consumer throughput but costs more. Use only when needed. |
| “Firehose guarantees immediate delivery” | Firehose buffers and delivers near-real-time; not a sub-second custom processing service. |
| “Kinesis solves permanent retention” | Retention is finite; archive to S3 for long-term storage. |
| “Ordering is global” | Ordering is per shard/partition key, not across the whole stream. |

## Practice mini-scenarios

### Scenario 1

A company collects clickstream events from web and mobile clients. Fraud detection, personalization, and analytics teams each need to consume all events independently. If the fraud detector fails for 30 minutes, it must replay the missed events.

Answer: **Kinesis Data Streams** with multiple consumers. Consider **enhanced fan-out** if consumers need dedicated throughput.

### Scenario 2

A company wants to send application logs to S3 and Redshift with minimal operational overhead. No custom real-time processing is required.

Answer: **Kinesis Data Firehose**.

### Scenario 3

A company needs rolling 5-minute aggregations from IoT telemetry and must write anomaly scores to another stream.

Answer: **Kinesis Data Streams + Managed Service for Apache Flink**.

### Scenario 4

A background thumbnailing system must process each image job once, retry failures, and isolate poison messages.

Answer: **SQS + Lambda/ECS workers + DLQ**, not Kinesis.

### Scenario 5

A Kafka-based trading platform is migrating to AWS with minimal code change and existing Kafka clients must continue to work.

Answer: **Amazon MSK**, not Kinesis.

## Rapid recall

- **Stream**: Kinesis.
- **Queue**: SQS.
- **Broadcast**: SNS.
- **Event routing**: EventBridge.
- **Workflow state machine**: Step Functions.
- **Delivery to S3/OpenSearch/Redshift**: Firehose.
- **Windowed stream analytics**: Managed Service for Apache Flink.
- **Kafka compatibility**: MSK.

## Source references

- Amazon Kinesis Data Streams introduction: https://docs.aws.amazon.com/streams/latest/dev/introduction.html
- Amazon Kinesis Data Streams quotas and limits: https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html
- Enhanced fan-out consumers: https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html
- Lambda event source mappings: https://docs.aws.amazon.com/lambda/latest/dg/invocation-eventsourcemapping.html
