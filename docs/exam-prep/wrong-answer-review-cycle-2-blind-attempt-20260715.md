# SAP-C02 Wrong-Answer Review Cycle 2 - Blind Attempt

<!-- markdownlint-disable MD012 MD013 MD022 MD060 -->

- Date opened: 2026-07-15
- Date completed: 2026-07-18
- Mode: untimed, closed-note, free response
- Status: explicitly submitted; corrected final answers scored 4/4

> **Evidence boundary:** the file preserves the initial saved draft answers and
> the materially revised final answers explicitly submitted on 2026-07-18. The
> final answers are correct, but the revision means this must not be described
> as an unchanged clean blind pass.

## Evidence Boundary

This drill retests the four durable wrong-answer themes in
`docs/exam-prep/wrong-answers.md` without reproducing its correction text or an
answer key.

Complete it from memory without opening the wrong-answer log, tracker, Route 53
lesson, Networking notes, AWS documentation, search results, or AI assistance.
Do not edit an earlier answer after consulting a source.

The file was opened on the same date as Review Cycle 1, but the final answers
were submitted on 2026-07-18. This provides a three-day spacing interval. The
saved draft contains substantive gaps that the final submission corrects, so
the artifact evidences a completed review-and-correction cycle rather than a
clean four-answer blind recall result.

## Question 1

A company has a private hosted zone in AWS and an authoritative DNS zone in its
on-premises data centre.

- On-premises clients must resolve names from the AWS private hosted zone.
- AWS workloads must resolve names from the on-premises zone.
- A private network path between AWS and the data centre already exists.

Describe the DNS components and directional configuration required for both
query flows. Also identify one plausible AWS networking service that does not,
by itself, perform the DNS forwarding.

### Initial Saved Draft Answer 1

Route 53 VPC Resolver  - provides recursive DNS inside VPCs and hybrid DNS through inbound/outbound endpoints and rules.
On premises clients use Inbound Resolver endpoints +
conditional forwarder to resolves AWS Workload private names
ON premises -> conditional forwarder -> inbound endpoint -> VPC Resolver
AWS Workloads use outbound resolvers + Resolver forwarding rule to resolve on-premises clients.
AWS workload -> VP Resolver rule -> outbound endpoint -> on-premise DNS

## Question 2

A company must leave its data centre within eight weeks. It has hundreds of
supported virtual machines, limited engineering capacity, and a strict
requirement to minimize application changes before exit. Leadership wants to
modernize the applications later.

State the migration pattern and primary AWS migration service that should be
used first. Explain why a refactor-first proposal loses under these constraints
and when modernization should occur.

### Initial Saved Draft Answer 2
Rehost (Lift and Shift). Move applications 1:1 to the cloud without changing their architecture
and AWS Transform MGN should be used, automates replication, conversion and cutover of physical, virtual and cloud based servers. Move on-premises workloads to EC2 with minimal downtime.

Refactor loses because their insufficient time (only eight weeks) to rearchitect the application reimagining the application with cloud optimized architecture


## Question 3

Dozens of VPCs require private connectivity to an on-premises network. The
design needs centralized multi-VPC routing and bidirectional private DNS. No
workload requires public internet ingress or egress for this scenario.

Name the AWS components that satisfy:

1. the private hybrid transport;
2. the scalable multi-VPC routing; and
3. the two DNS directions.

Explain why adding an Internet Gateway would be an incorrect selection.

### Initial Saved Draft Answer 3

PrivateLink, Route 53 and AWS Transit gateway used in a hub and spoke pattern. TG used for centralized routing.
Internet gateway would be incorrect because no workload requires ingress or egress, therefore internet access is not required.



## Question 4

An event-ingestion platform must support all of these requirements:

- sustained high throughput;
- ordering for each customer identifier;
- replay for up to seven days; and
- three independent consumer applications that read the same events at their
  own pace.

Choose the AWS ingestion service and describe the key design mechanism that
preserves ordering. Explain why selecting an ordered queue only because the
scenario mentions ordering would miss the broader requirement.

### Initial Saved Draft Answer 4

Kinesis Data Streams preserves ordering within each shard, has superior buffering and than SQS, it can also support replay. Customer ID used to order between shards, it can also support multiple consumers unlike the Kinesis Firehouse.
SQS is unable to sustain high throughput


## Explicit Final Submission - 2026-07-18

### Final Answer 1

Use **Route 53 Resolver inbound and outbound endpoints**.

For on-premises clients resolving AWS private hosted-zone records:

```text
On-premises DNS
  -> conditional forwarder for AWS private domain
  -> Route 53 Resolver inbound endpoint
  -> VPC Resolver
  -> Route 53 private hosted zone
```

For AWS workloads resolving on-premises DNS records:

```text
AWS workload
  -> VPC Resolver
  -> Route 53 Resolver rule
  -> Route 53 Resolver outbound endpoint
  -> on-premises DNS server
```

The private network path can be Direct Connect or Site-to-Site VPN, but those
services do not by themselves perform DNS forwarding. Transit Gateway can
provide centralized routing, but it is not a DNS forwarding service.

### Final Answer 2

The correct migration pattern is **rehost**, also known as **lift and shift**.
The primary AWS service is **AWS Application Migration Service (AWS MGN)**. It
replicates physical, virtual, or cloud-based servers and helps cut them over to
EC2 with minimal application change.

A refactor-first approach loses because the company has an eight-week exit
deadline, hundreds of VMs, limited engineering capacity, and a strict
requirement to minimize application changes. Refactoring requires redesign,
code changes, testing, and operational change, which is too risky for the exit
deadline.

The better sequence is:

```text
rehost first
stabilize on AWS
then modernize selectively after migration
```

### Final Answer 3

| Requirement | AWS component |
|---|---|
| Private hybrid transport | AWS Direct Connect or Site-to-Site VPN |
| Scalable multi-VPC routing | AWS Transit Gateway |
| On-premises to AWS private DNS | Route 53 Resolver inbound endpoint |
| AWS to on-premises DNS | Route 53 Resolver outbound endpoint plus forwarding rule |

The private routing flow is:

```text
On-premises network
  -> Direct Connect / VPN
  -> Transit Gateway
  -> attached VPCs
```

For DNS:

```text
On-premises DNS -> Resolver inbound endpoint -> AWS private hosted zone
AWS workloads -> Resolver outbound endpoint -> on-premises DNS
```

An Internet Gateway is incorrect because the requirement explicitly says there
is no public internet ingress or egress. The design needs private hybrid
connectivity, not internet access. PrivateLink is not the primary answer for
hybrid transport: it exposes specific services privately rather than providing
centralized many-VPC-to-on-premises routing.

### Final Answer 4

Use **Amazon Kinesis Data Streams**. It supports high-throughput event
ingestion, ordered records within a shard, replay through stream retention, and
multiple independent consumers.

Use the customer identifier as the partition key:

```text
partition_key = customer_id
```

This sends events for the same customer to the same shard and preserves their
ordering. SQS FIFO provides an ordered work queue but is not the best fit for a
replayable event stream with multiple independent consumers reading the same
events at their own pace. Once an SQS message is successfully consumed and
deleted, it is unavailable to other consumers unless separate fan-out queues
are built.

```text
SQS FIFO:
  ordered work queue

Kinesis Data Streams:
  ordered, replayable event stream with multiple consumers
```

## Final Assessment

| Question | Result | Assessment |
|---:|:---:|---|
| 1 | Correct | Correct Resolver direction, forwarding flow, private hosted-zone path, and DNS-versus-transport distinction. |
| 2 | Correct | Correct rehost/MGN choice, constraint-led rejection of refactor first, and modernization sequence. |
| 3 | Correct | Correct hybrid transport, Transit Gateway routing hub, both Resolver directions, Internet Gateway exclusion, and PrivateLink boundary. |
| 4 | Correct | Correct Kinesis choice, customer-ID partition key, per-shard ordering, replay, and independent-consumer distinction from SQS FIFO. |

Final submitted-answer score: **4/4 (100%)**.

The initial saved draft had material precision gaps: it omitted part of the
first question's non-DNS-service explanation, conflated the MGN service name,
selected PrivateLink for hybrid transport, and misstated parts of the
partition/shard and SQS comparison. The final submission corrects those gaps.

Review Cycle 2 is complete as a reviewed-and-corrected retention cycle. No new
wrong-answer theme is needed because the corrections belong to the four
existing entries. This remains untimed and does not count as a practice exam or
booking score.

The separately submitted
[25-question mixed practice Block 2](sap-c02-mixed-practice-block-2-submission-20260718.md)
remains independent clean-pass evidence.
