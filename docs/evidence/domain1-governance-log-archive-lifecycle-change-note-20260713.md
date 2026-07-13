# Domain 1 Governance Change Note - Log Archive Lifecycle - 2026-07-13

## Status

Completed under direct user approval. This change used two bounded units:

1. a least-privilege IAM Identity Center lifecycle-operator assignment in the
   Security Log Archive account; and
2. an S3 lifecycle rule applied to the CloudTrail and AWS Config archive
   buckets.

## Access Change

`LogArchiveLifecycleOperator` was created with a one-hour session duration and
directly assigned to the existing normal administrator in Security Log Archive
only. Its validated inline policy permits lifecycle read/write and read-only
versioning, encryption, and public-access-block checks on exactly the two
central archive buckets. It does not permit object reads or deletes, bucket or
KMS policy changes, encryption changes, IAM administration, or Organizations
actions.

The new role successfully assumed into Security Log Archive. This closes the
specific lifecycle-administration gap without granting broad archive-account
access.

## S3 Precheck

Immediately before the lifecycle write, both archive buckets had:

- no existing lifecycle configuration;
- versioning enabled;
- default SSE-KMS encryption; and
- all four S3 public-access-block settings enabled.

## Applied Lifecycle Configuration

Both archive buckets now have one enabled rule, `ArchiveLargeLogsByAge`:

| Eligible object age | Storage class |
|---:|---|
| 30 days | S3 Standard-IA |
| 90 days | S3 Glacier Flexible Retrieval |

The rule applies only to objects larger than 128 KiB. No expiry, delete,
version-expiry, encryption, public-access, or bucket-policy action was added.
Lifecycle is asynchronous, so this verifies accepted configuration rather than
an immediate transition of existing objects.

## Rollback and Cost

Before any eligible transition, rollback is removal of the lifecycle rule. Once
objects transition, removing the rule does not restore them; restoration or copy
would be a separately approved action. The IAM access rollback is removal of
the direct assignment and, after confirming no other assignments exist, the
permission set.

Standard-IA has a 30-day minimum duration and Glacier Flexible Retrieval has a
90-day minimum duration. Excluding small objects avoids lifecycle request and
minimum-size costs that could outweigh savings at the current log volume.

## SAP-C02 Relevance

This supports Domain 1 by applying lifecycle cost control to central audit
storage through a least-privilege, approval-bounded access path while
preserving retention and protection controls.
