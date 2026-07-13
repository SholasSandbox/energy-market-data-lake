# Domain 1 Change Note - Archive Bucket Lifecycle - 2026-07-13

## Status

Completed on 2026-07-13 under direct user instruction after the narrowly scoped
lifecycle-operator access change validated.

## Decision

Apply the same lifecycle rule to the CloudTrail and AWS Config archive buckets:

| Object age | Storage class | Rationale |
|---:|---|---|
| 0-29 days | S3 Standard | Immediate investigation access for recent security evidence. |
| 30-89 days | S3 Standard-IA | Lower-cost storage with millisecond retrieval. |
| 90+ days | S3 Glacier Flexible Retrieval | Lower-cost long-term archive; restore is required before access. |

The rule applies only to objects larger than 128 KiB. Smaller log objects stay
in Standard because S3 transition requests and minimum-size charges can exceed
their storage savings at this platform's current low volume. No expiration,
deletion, version-expiration, Object Lock, encryption, or bucket-policy change
is included.

The exact JSON is
`docs/policies/s3-log-archive-lifecycle-30-90-days.example.json`.

## Precheck and Validation

Immediately before the write, use `GetLifecycleConfiguration` for both
buckets. If either bucket already has lifecycle rules, merge only after review;
do not replace an unknown configuration. Confirm bucket versioning, encryption,
and public-access-block remain unchanged.

After the write, read each lifecycle configuration and verify the one enabled
rule, its larger-than-128-KiB filter, `STANDARD_IA` day 30 transition, and
`GLACIER` day 90 transition. Lifecycle execution is asynchronous, so the
postchange result proves configuration acceptance rather than immediate object
movement.

## Blast Radius, Rollback, and Cost

The blast radius is limited to future and existing eligible objects in the two
central archive buckets. Investigations of objects aged 90 days or more require
a restore request; this is why Glacier Flexible Retrieval is delayed until day
90.

Rollback before transitions occur is to restore the prior lifecycle
configuration from the immediate precheck. After transitions occur, removing a
rule does not automatically move archived objects back; restores or copies
would be separate, approval-bound operations.

Standard-IA has a 30-day minimum duration and Glacier Flexible Retrieval has a
90-day minimum duration. Transition and retrieval requests also incur charges.
The object-size filter avoids applying those charges to small log objects where
savings are unlikely.

## Execution Result

Neither target bucket had an existing lifecycle configuration. The rule was
accepted and read back from both buckets with the expected 30-day
`STANDARD_IA`, 90-day `GLACIER`, and larger-than-128-KiB settings.
