# SAP-C02 Non-Relational Databases Spaced Retest - 6 Questions

<!-- markdownlint-disable MD013 MD060 -->

**Date prepared:** 2026-07-25<br>
**Last revised:** 2026-07-25<br>
**Earliest attempt:** 2026-07-28<br>
**Status:** Question-only; no answer key, explanations, or hints are included.

## Attempt Rules

- Complete closed book and keep the lesson, diagnostic review, and wrong-answer log closed.
- Target time: **15 minutes**.
- Use exact-match scoring for every multiple-response question.
- Freeze the answer block before requesting marking.
- Do not attempt before 2026-07-28; the spacing is part of the evidence.

## Questions

### 1. Reconstructing ordered results — Choose THREE

A gaming platform distributes a popular leaderboard's writes across 32 random
DynamoDB partition-key suffixes. A request must return every score for the
original leaderboard in descending score order.

Which three actions or constraints belong in the read design? Choose THREE.

A. Query the partition-key value for each relevant suffix.<br>
B. Merge the result sets returned by the shard queries.<br>
C. Establish the required order across the merged results.<br>
D. Set `ConsistentRead=true` on a GSI and allow the index to merge the shards.<br>
E. Replace the shard queries with one filtered Scan because the filter is applied before reads.<br>
F. Use DAX to infer which random suffix was assigned to every item.

### 2. Alternate lookup after sharding — Choose TWO

An order table uses random write-shard suffixes. A new GSI supports lookup by
`customerId`, but the application also requires the latest committed value and
assumes the GSI will provide a strongly consistent, globally ordered result.

Which two statements are correct? Choose TWO.

A. GSI reads are eventually consistent only.<br>
B. A GSI automatically merges and globally orders all base-table shard queries.<br>
C. If the latest committed base-table value is mandatory, use a supported strongly consistent table or LSI access path rather than relying on the GSI.<br>
D. Changing the base table to on-demand capacity makes GSI reads strongly consistent.<br>
E. DAX turns an eventually consistent GSI read into a strongly consistent read.

### 3. Expiry, reaction, and rewind — Choose THREE

A session platform needs records to age out automatically, a Lambda consumer to
react to item modifications, and restoration to a point immediately before an
accidental bulk update.

Which three DynamoDB capabilities map to those requirements? Choose THREE.

A. Time to Live<br>
B. DynamoDB Streams<br>
C. Point-in-time recovery<br>
D. Global secondary index<br>
E. DynamoDB Accelerator<br>
F. On-demand capacity mode

### 4. Index purpose

A team proposes a GSI solely to obtain an asynchronous event whenever an item
is inserted or updated.

Which change most directly meets the stated requirement?

A. Use DynamoDB Streams and an event consumer.<br>
B. Retain the GSI and enable strongly consistent reads.<br>
C. Replace the GSI with TTL.<br>
D. Add a random write-shard suffix.

### 5. Operational state and history — Choose TWO

A rail operator ingests timestamped telemetry. Analysts query trends across
long time windows, while an operational API retrieves the latest state for one
known train ID with millisecond latency.

Which two storage decisions best separate these access patterns? Choose TWO.

A. Store/query time-window telemetry in Amazon Timestream.<br>
B. Store the latest per-train operational state in DynamoDB keyed by train ID.<br>
C. Use Neptune for both because consecutive measurements form relationships.<br>
D. Keep all history only in ElastiCache because it provides lower latency.<br>
E. Use a DocumentDB collection solely because telemetry can be encoded as JSON.

### 6. Clause-to-option discipline — Choose THREE

A smart-building platform requires: automatic removal of expired device
sessions, processing of every device-state change, and fast latest-state lookup
by a known device ID. Historical time-window analytics is handled elsewhere.

Which three choices map directly to the stated requirements? Choose THREE.

A. DynamoDB TTL for session expiry.<br>
B. DynamoDB Streams for change processing.<br>
C. A DynamoDB primary-key design supporting lookup by device ID.<br>
D. Timestream as the only latest-state key-value lookup.<br>
E. A GSI solely to emit item-change events.<br>
F. DAX as the durable source of truth.

## Frozen Submission

```text
Start:
End:
Uncertain:

1:
2:
3:
4:
5:
6:

Submission status: FROZEN / NOT FROZEN
```

## Post-Attempt Boundary

After the learner freezes and submits the answers, create the answer-bearing
review and update the tracker only from demonstrated retest evidence.
