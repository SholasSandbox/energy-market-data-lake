# SAP-C02 Wrong-Answer Review Cycle 2 - Blind Attempt

- Date opened: 2026-07-15
- Mode: untimed, closed-note, free response
- Status: awaiting learner submission

> **Blind-attempt boundary:** this is the active test artifact. Return to the
> [Exam-Prep Revision Hub](README.md) only for workflow guidance. Do not open
> lessons, reviews, or the wrong-answer log until the learner explicitly
> submits the answers.

## Evidence Boundary

This drill retests the four durable wrong-answer themes in
`docs/exam-prep/wrong-answers.md` without reproducing its correction text or an
answer key.

Complete it from memory without opening the wrong-answer log, tracker, Route 53
lesson, Networking notes, AWS documentation, search results, or AI assistance.
Do not edit an earlier answer after consulting a source.

Because Review Cycle 1 was completed earlier on the same date, this attempt
retains a same-day spacing caveat. It can evidence a second blind attempt, but
the final review must not overstate the strength of the spacing interval.

## Question 1

A company has a private hosted zone in AWS and an authoritative DNS zone in its
on-premises data centre.

- On-premises clients must resolve names from the AWS private hosted zone.
- AWS workloads must resolve names from the on-premises zone.
- A private network path between AWS and the data centre already exists.

Describe the DNS components and directional configuration required for both
query flows. Also identify one plausible AWS networking service that does not,
by itself, perform the DNS forwarding.

### Learner Answer 1



## Question 2

A company must leave its data centre within eight weeks. It has hundreds of
supported virtual machines, limited engineering capacity, and a strict
requirement to minimize application changes before exit. Leadership wants to
modernize the applications later.

State the migration pattern and primary AWS migration service that should be
used first. Explain why a refactor-first proposal loses under these constraints
and when modernization should occur.

### Learner Answer 2



## Question 3

Dozens of VPCs require private connectivity to an on-premises network. The
design needs centralized multi-VPC routing and bidirectional private DNS. No
workload requires public internet ingress or egress for this scenario.

Name the AWS components that satisfy:

1. the private hybrid transport;
2. the scalable multi-VPC routing; and
3. the two DNS directions.

Explain why adding an Internet Gateway would be an incorrect selection.

### Learner Answer 3



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

### Learner Answer 4

Kinesis Data Streams preserves ordering with each Shard


## Submission Rule

The attempt becomes a learner submission only when the learner explicitly says
to submit or score the four answers. Until then:

- do not add an answer key;
- do not mark Review Cycle 2 complete;
- do not update the booking criterion; and
- do not infer a score from partial or draft answers.

After explicit submission, return to the
[Exam-Prep Revision Hub](README.md) for the review-and-record sequence.
