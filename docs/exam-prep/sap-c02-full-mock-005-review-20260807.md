# SAP-C02 Full Mock 005 - Independent Review

<!-- markdownlint-disable MD013 MD060 -->

**Reviewed:** 2026-08-07
**Last revised:** 2026-08-09
**Submission:** [frozen Full Mock 005](sap-c02-full-mock-005-75q-20260807.md)
**Result:** **73/75 (97.3%)**
**Time:** **108 of 180 minutes; 72 minutes remaining**

## Evidence Boundary

The learner froze all 75 responses before marking. Multiple-response questions
were scored only for exact matches. This is full timed-mock evidence and is
eligible for the longitudinal readiness series; it does not move the booking
decision ahead of the post-Mock-008 gate.

## Score Summary

| Measure | Correct | Total | Score |
|---|---:|---:|---:|
| Single response | 46 | 48 | 95.8% |
| Multiple response, exact match | 27 | 27 | 100% |
| Learner-marked uncertain | 14 | 16 | 87.5% |
| **Overall** | **73** | **75** | **97.3%** |

| SAP-C02 domain | Correct | Total | Score |
|---|---:|---:|---:|
| Domain 1 - Design Solutions for Organizational Complexity | 20 | 20 | 100% |
| Domain 2 - Design for New Solutions | 22 | 22 | 100% |
| Domain 3 - Continuous Improvement for Existing Solutions | 17 | 18 | 94.4% |
| Domain 4 - Accelerate Workload Migration and Modernization | 14 | 15 | 93.3% |
| **Overall** | **73** | **75** | **97.3%** |

## Answer Key and Marking

```text
 1 B   correct      26 CE  correct      51 AC  correct
 2 BD  correct      27 D   correct      52 D   correct
 3 C   correct      28 D   correct      53 B   correct
 4 ACE correct      29 BE  correct      54 CE  correct
 5 C   correct      30 C   correct      55 A   correct
 6 B   correct      31 AC  correct      56 A   incorrect (submitted D)
 7 BD  correct      32 B   correct      57 BE  correct
 8 D   correct      33 C   correct      58 C   correct
 9 CE  correct      34 BE  correct      59 A   correct
10 D   correct      35 C   correct      60 BDF correct
11 B   correct      36 C   correct      61 D   correct
12 AEF correct      37 BE  correct      62 A   correct
13 B   correct      38 B   correct      63 AC  correct
14 C   correct      39 D   correct      64 A   correct
15 BD  correct      40 BCF correct      65 A   correct
16 B   correct      41 A   correct      66 BDF correct
17 A   correct      42 AD  correct      67 C   correct
18 CE  correct      43 B   correct      68 A   correct
19 D   correct      44 C   correct      69 CE  correct
20 ABD correct      45 BD  correct      70 A   correct
21 D   correct      46 D   correct      71 D   correct
22 B   correct      47 B   incorrect (submitted BC)
23 AD  correct      48 BE  correct      72 AC  correct
24 D   correct      49 A   correct      73 C   correct
25 B   correct      50 C   correct      74 A   correct
                                            75 ACE correct
```

## Miss 1 - Question 47: ARC Routing-Control Mechanism

The item was single-response. The submission selected **B and C**; the correct
answer was **B**.

**Why B wins:** ARC routing controls are highly available on/off switches. ARC
cluster endpoints are used to change their state, and routing-control health
checks integrate that state with Route 53 DNS failover records.

**Why C loses:** ARC does not replace the Route 53 data plane with an
Application Load Balancer. The answer added a false architecture even though
the correct mechanism was already selected.

**Error classification:** single-response over-selection / reading discipline,
not a demonstrated ARC knowledge gap.

**Decision rule:** if the heading does not say `Choose TWO` or `Choose THREE`,
submit exactly one option. For ARC, remember:

```text
ARC cluster endpoint -> routing-control state
                     -> routing-control health check
                     -> Route 53 failover record
```

## Miss 2 - Question 56: AS2 Service Selection

The submission selected **D, Amazon MQ for MQTT**; the correct answer was
**A, AWS Transfer Family AS2**.

AS2 is a business-to-business message/file exchange protocol. AWS Transfer
Family supports AS2 servers, partner profiles, certificates, agreements,
connectors, Message Disposition Notifications, S3-backed files, and CloudWatch
audit records. Amazon MQ is a managed message-broker service; it is not the
managed AWS AS2 partner-file exchange.

**Error classification:** genuine service-comparison retrieval gap.

**Decision rule:**

```text
Partner file transfer over SFTP / FTPS / FTP -> AWS Transfer Family
Partner B2B/EDI exchange using AS2           -> AWS Transfer Family AS2
Managed ActiveMQ or RabbitMQ message broker  -> Amazon MQ
```

## Transfer Evidence

- The Mock 004 CloudFront/Lambda@Edge misconception transferred successfully:
  Question 5 correctly separated built-in read-method origin failover from a
  separately engineered write path.
- ECS CodeDeploy blue/green completeness transferred through Question 31.
- DynamoDB MREC/MRSC and TTL selection transferred through Question 30.
- The prior SCP/permissions-boundary and Region-exemption patterns also held.
- Multiple-response discipline was perfect at 27/27; Question 47 was instead an
  over-selection on an explicitly single-response item.

## Readiness Interpretation

This is the fifth consecutive full mock above 93%. Timing, domain floors, and
exact-match multiple-response performance all exceed the tracker thresholds.
The result strengthens the evidence of broad capability, but the learner's
chosen booking gate remains after Full Mock 008.

Do not add a broad remediation block. Review the AS2 decision rule and the
single-response submission rule, then continue to Full Mock 006. A later mock
or a very small spaced check can provide transfer evidence without replacing a
full mock.

## Official References

- [ARC routing control](https://docs.aws.amazon.com/r53recovery/latest/dg/routing-control.html)
- [AWS Transfer Family AS2 message flow](https://docs.aws.amazon.com/transfer/latest/userguide/send-as2-messages.html)
- [Amazon MQ architecture](https://docs.aws.amazon.com/amazon-mq/latest/developer-guide/amazon-mq-broker-architecture.html)
