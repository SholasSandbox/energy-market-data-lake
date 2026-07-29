# SAP-C02 Non-Relational Databases Spaced Retest Review - 2026-07-28

<!-- markdownlint-disable MD013 MD060 -->

**Last revised:** 2026-07-28<br>
**Document role:** answer-bearing exact-match assessment. Keep closed during any later blind attempt.

## Result

| Field | Result |
|---|---|
| Questions | 6 |
| Exact-match score | **6/6 (100%)** |
| Start / end | 11:45 AM / 12:00 PM |
| Elapsed time | **15 minutes** |
| Single-response | **1/1 (100%)** |
| Multiple-response | **5/5 (100%)** |
| Learner-marked uncertainty | Questions 1 and 2 |
| Uncertain answers correct | **2/2** |
| Misses | None |

The learner explicitly froze the answer block before marking. The learner later
clarified that the end time was `12:00PM`; this replaces the inconsistent
`12:00AM` marker in the chat submission and the earlier `11:57AM` value saved
in the working document. The final evidenced duration is 15 minutes.

Closed-book compliance is learner-attested and is not independently observable.
The attempt was completed on the earliest permitted date and met the 15-minute
target exactly.

## Exact-Match Marking

| Question | Required selections | Learner answer | Correct answer | Result |
|---:|---:|---|---|---|
| 1 | Three | A, B, C | A, B, C | Correct |
| 2 | Two | A, C | A, C | Correct |
| 3 | Three | A, B, C | A, B, C | Correct |
| 4 | One | A | A | Correct |
| 5 | Two | A, B | A, B | Correct |
| 6 | Three | A, B, C | A, B, C | Correct |

## Question Review

### 1. Reconstructing ordered results

**A, B, and C.** Random write sharding distributes one logical leaderboard
across several physical partition-key values. The read path must query every
relevant suffix, merge the result sets, and establish the required order across
the merged data. A GSI does not perform that application-level reconstruction,
and GSI reads cannot be strongly consistent.

### 2. Alternate lookup after sharding

**A and C.** GSI reads are eventually consistent only. When the latest
committed value is mandatory, use a supported table or LSI access path with a
strong read rather than claiming that a GSI, DAX, or capacity-mode change
provides strong consistency.

### 3. Expiry, reaction, and rewind

**A, B, and C.** TTL handles asynchronous expiry, DynamoDB Streams provides
item-change records for the Lambda consumer, and PITR enables restoration to an
earlier point. A GSI supplies an alternate query path and does not emit events.

### 4. Index purpose

**A.** DynamoDB Streams is the change-data-capture mechanism for reacting to
inserts, updates, and deletes. A GSI is an index, not an event stream.

### 5. Operational state and history

**A and B.** Timestream fits timestamp-centred history and time-window
analytics. DynamoDB fits millisecond latest-state retrieval by a known train
identifier. The answer correctly maps both explicit access patterns.

### 6. Clause-to-option discipline

**A, B, and C.** TTL maps to session expiry, Streams maps to change processing,
and a primary key based on device ID maps to latest-state lookup. The response
selected one valid component for every stated clause without adding unrelated
services.

## Gap-Closure Decision

The focused spaced-recall gate is complete:

- the random-shard fan-out, merge, and cross-shard ordering consequence was
  recalled correctly;
- GSI eventual-consistency limits were retained;
- TTL, Streams, PITR, and GSI roles were separated correctly; and
- Timestream history and DynamoDB latest-state responsibilities were fully
  decomposed.

This closes the targeted remediation item. It does not prove complete database
domain mastery or replace an independent timed mock. Monitor for recurrence in
full mock 002 and the remaining validation series; reopen focused remediation
only if the same error pattern returns.

## Official AWS References

- [DynamoDB write sharding](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-sharding.html)
- [DynamoDB read consistency](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html)
- [DynamoDB Streams](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html)
- [DynamoDB TTL](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html)
- [DynamoDB point-in-time recovery](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Point-in-time-recovery.html)
- [Amazon Timestream](https://docs.aws.amazon.com/timestream/latest/developerguide/what-is-timestream.html)
