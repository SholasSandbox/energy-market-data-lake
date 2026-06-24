# Domain 1 Governance Change Note - CloudTrail Organization Trail And Log Archive Baseline - 2026-06-22

<!-- markdownlint-disable MD013 -->

## Target Account And OU

- Trail control-plane account: `349687196588` / `management-account-alias`
- Intended organization scope: all current organization accounts, including
  `Lakehouse Workloads OU` and `Container Sandbox`
- Intended log-archive boundary: future security/log archive account in the
  future `Security OU`
- Baseline check Region: `eu-west-2`

## Current State

Read-only baseline capture confirms:

- the active shell identity is the management-account SSO administrator session;
- the current organization design still points to a future dedicated
  security/log archive account for the central bucket and KMS key;
- `aws cloudtrail list-trails --region eu-west-2` currently returns no trails;
- `aws s3api list-buckets` in the management account currently returns no
  buckets.

Current interpretation:

- there is no current CloudTrail trail evidenced in the baseline Region used for
  this note;
- there is no management-account S3 bucket available yet for a central
  CloudTrail log archive;
- the accepted target design is still blocked from direct implementation because
  the future security/log archive account does not yet exist.

Evidence files:

- `docs/evidence/domain1-governance-cloudtrail-sts-prechange-20260622.json`
- `docs/evidence/domain1-governance-cloudtrail-list-prechange-20260622.json`
- `docs/evidence/domain1-governance-cloudtrail-s3-buckets-prechange-20260622.json`
- `docs/evidence/domain1-governance-org-inventory-summary-20260621.md`
- `docs/evidence/domain1-governance-lakehouse-account-move-change-note-20260622.md`
- `docs/planning/domain-1-cloudtrail-log-archive-design-20260621.md`

Read-only commands used:

```bash
aws sts get-caller-identity \
  --profile org-admin \
  --output json

aws cloudtrail list-trails \
  --profile org-admin \
  --region eu-west-2 \
  --output json

aws s3api list-buckets \
  --profile org-admin \
  --query 'Buckets[].Name' \
  --output json
```

## Proposed Change

No live CloudTrail, S3, or KMS change is executed in this note.

This note packages the smallest useful next decision boundary for the
CloudTrail/log archive lane:

1. Preferred target path: create the future security/log archive account first,
   then create one multi-Region organization trail that writes to a dedicated
   CloudTrail bucket with the chosen encryption posture in that account.
2. Interim fallback path: if earlier live audit coverage is more valuable than
   waiting for the dedicated account, explicitly approve a temporary
   management-account implementation with a dedicated log bucket and a later
   migration plan.

Why now:

- CloudTrail is a high-leverage control that supports every current workload;
- it unlocks the later `Deny disabling CloudTrail` and log-archive-protection
  SCP candidates with a real target to protect;
- it gives the cleanest foundation for later AWS Config and GuardDuty work.

## Expected Blast Radius

Today:

- no live AWS resources are created, updated, or deleted;
- no workload behavior changes.

If the later live change is approved:

- all current organization accounts become part of the organization-trail audit
  surface;
- CloudTrail, S3, and possibly KMS costs begin;
- central audit-log ownership and read access become part of the control-plane
  model.

## Rollback Path

For this note:

- no rollback action is needed because no live change is applied.

For a later approved interim management-account implementation:

- stop logging on the trail;
- delete the trail if required;
- remove the dedicated bucket policy or bucket only if retention and evidence
  handling are explicitly approved;
- record post-rollback evidence with the same bounded file pattern used for
  other governance changes.

## Validation

Completed validation for this baseline note:

- the management-account SSO session is active;
- the CloudTrail baseline file shows `"Trails": []` in `eu-west-2`;
- the management-account S3 bucket baseline shows `[]`.

Future live-change validation commands:

```bash
aws cloudtrail list-trails \
  --profile org-admin \
  --region eu-west-2 \
  --output json

aws cloudtrail get-trail \
  --profile org-admin \
  --name "<trail-name-or-arn>" \
  --region eu-west-2 \
  --output json

aws cloudtrail get-trail-status \
  --profile org-admin \
  --name "<trail-name-or-arn>" \
  --region eu-west-2 \
  --output json

aws s3api get-bucket-versioning \
  --bucket "<log-bucket-name>"

aws s3api get-bucket-encryption \
  --bucket "<log-bucket-name>"

aws s3api get-public-access-block \
  --bucket "<log-bucket-name>"

aws s3api get-bucket-policy \
  --bucket "<log-bucket-name>"
```

Success criteria for the next live change:

- one organization trail exists in the chosen home Region;
- logging is active;
- the log bucket exists in the explicitly approved ownership boundary;
- versioning, public-access block, and encryption match the approved design;
- the chosen delete-protection posture is documented and reviewable.

## Cost Impact

- No cost impact from this read-only baseline capture.
- Future cost will be small but real across CloudTrail, S3 storage, requests,
  and possibly KMS usage.
- The final cost shape depends on whether the implementation stays at
  management-events only or later adds higher-volume data-event logging.

## Approval

- Approval source: direct user instruction
- Approval text: `Let start with CloudTrail / log archive, that's very useful across the entire workload`
- Approval date: 2026-06-22
- Scope of approval: read-only baseline capture and bounded CloudTrail/log
  archive change-note preparation only

## Result

The CloudTrail/log archive lane is now prepared with current live baseline
evidence.

The remaining blocker is no longer "what should we do?" but "which ownership
path do we intentionally choose for the first live trail?":

- wait for the future security/log archive account and preserve the accepted
  target design; or
- explicitly approve a temporary management-account trail-and-bucket fallback
  with a later migration step.
