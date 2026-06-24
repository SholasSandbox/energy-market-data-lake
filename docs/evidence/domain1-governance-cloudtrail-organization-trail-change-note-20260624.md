# Domain 1 Governance Change Note - Organization CloudTrail Trail Enablement - 2026-06-24

<!-- markdownlint-disable MD013 -->

## Status Note

This organization-trail step was executed live on 2026-06-24 under explicit
user approval.

The resulting live state is:

- `cloudtrail.amazonaws.com` trusted access is now enabled for the
  organization;
- trail `organization-management-events` now exists in management account
  `349687196588` with home Region `eu-west-2`;
- the trail is multi-Region, organization-scoped, and has log file validation
  enabled;
- the trail targets bucket
  `org-cloudtrail-log-archive-955659429518-eu-west-2`;
- the trail targets KMS key
  `arn:aws:kms:eu-west-2:955659429518:key/201876fa-d5f9-4810-9721-676b17f737f3`;
- logging is active;
- CloudTrail status now reports successful delivery and digest delivery;
- the security-account bucket now contains real delivered digest and trail log
  objects, including:
  `AWSLogs/o-hmvgqmav88/349687196588/CloudTrail/eu-west-2/2026/06/24/349687196588_CloudTrail_eu-west-2_20260624T1240Z_zpKV6nRhbMHCJjFV.json.gz`
  and
  `AWSLogs/o-hmvgqmav88/349687196588/CloudTrail-Digest/eu-west-2/2026/06/24/349687196588_CloudTrail-Digest_eu-west-2_organization-management-events_eu-west-2_20260624T123601Z.json.gz`.

## Target Account And OU

- Trail owner account: `349687196588` / `management-account-alias`
- Organization scope: all current member accounts plus future in-organization
  accounts
- Home Region: `eu-west-2`
- Intended log bucket owner: `955659429518` / `Security Log Archive`
- Intended bucket: `org-cloudtrail-log-archive-955659429518-eu-west-2`
- Intended KMS alias: `alias/org-cloudtrail-log-archive`
- Intended trail name: `organization-management-events`

## Current State

Fresh read-only evidence confirms:

- there is still no CloudTrail trail in `eu-west-2`;
- `cloudtrail.amazonaws.com` is not yet listed under enabled AWS service access
  for the organization;
- the dedicated `Security Log Archive` account boundary now exists and is the
  intended destination for the log bucket and KMS key;
- the storage step documented in
  `docs/evidence/domain1-governance-cloudtrail-log-archive-storage-change-note-20260624.md`
  has now been executed, so the dedicated bucket and KMS key already exist.

Evidence files:

- `docs/evidence/domain1-governance-cloudtrail-management-sts-prechange-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-service-access-prechange-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-list-prechange-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-log-archive-storage-change-note-20260624.md`
- `docs/evidence/domain1-governance-cloudtrail-log-archive-bucket-create-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-log-archive-kms-key-create-20260624.json`

Read-only commands used:

```bash
aws sts get-caller-identity \
  --profile org-admin \
  --output json

aws organizations list-aws-service-access-for-organization \
  --profile org-admin \
  --output json

aws cloudtrail list-trails \
  --profile org-admin \
  --region eu-west-2 \
  --output json
```

## Preconditions

Do not execute this step until both are true:

1. the dedicated log-archive bucket and customer-managed KMS key exist in the
   `Security Log Archive` account; and
2. the bucket and key policies are resolved with the final live values from the
   storage-step note.

Those two storage prerequisites are now satisfied by the 2026-06-24 storage
boundary execution.

## Proposed Change

Create the management-account organization trail as the second narrow live
step:

1. enable trusted access for CloudTrail in AWS Organizations if it is still
   absent:
   `aws organizations enable-aws-service-access --service-principal cloudtrail.amazonaws.com`;
2. create one multi-Region organization trail named
   `organization-management-events` in `eu-west-2`;
3. target the dedicated security-account bucket
   `org-cloudtrail-log-archive-955659429518-eu-west-2`;
4. target the customer-managed KMS key behind
   `alias/org-cloudtrail-log-archive`;
5. enable log file integrity validation;
6. start logging;
7. leave data events, Insights, and CloudTrail Lake disabled in this baseline
   step.

Recommended command shape:

```bash
aws organizations enable-aws-service-access \
  --profile org-admin \
  --service-principal cloudtrail.amazonaws.com

aws cloudtrail create-trail \
  --profile org-admin \
  --region eu-west-2 \
  --name organization-management-events \
  --s3-bucket-name org-cloudtrail-log-archive-955659429518-eu-west-2 \
  --kms-key-id <security-account-kms-key-arn> \
  --is-organization-trail \
  --is-multi-region-trail \
  --enable-log-file-validation

aws cloudtrail start-logging \
  --profile org-admin \
  --region eu-west-2 \
  --name organization-management-events
```

## Why This Boundary

This is the correct SAP-C02-style follow-on step after the storage boundary:

- it creates one organization-wide management-event audit plane from the
  management account;
- it uses the already-created security account as the durable log owner;
- it unlocks later `Deny disabling CloudTrail` and log-bucket protection SCPs
  with a real target to protect;
- it keeps the baseline small enough to validate before adding noisier features.

## Trade-Offs

Accepted in this boundary:

- use the management account as the CloudTrail control plane rather than
  introducing delegated administration now;
- use one multi-Region organization trail because it best matches the accepted
  design and exam pattern;
- include management events and global service events as the baseline posture.

Rejected for now:

- single-Region organization trail, because it weakens default audit coverage;
- organization-wide data events, because they add cost and event volume too
  early;
- CloudTrail Insights and CloudTrail Lake, because they are useful but not the
  first control to prove.

## Expected Blast Radius

- CloudTrail begins organization-wide management-event delivery for current and
  future accounts in the organization;
- a CloudTrail service-linked role is created where AWS requires it for member
  account logging;
- CloudTrail, S3 storage, S3 requests, and KMS request cost begin;
- the `Security Log Archive` bucket starts receiving `AWSLogs/` objects for the
  management account and organization scope.

## Rollback Path

If the trail must be reversed shortly after creation:

- stop logging;
- validate whether any log objects have already landed in the bucket;
- delete the trail from the management account home Region if that is still the
  approved action;
- do not disable `cloudtrail.amazonaws.com` trusted access in the same step
  unless there is explicit approval and no remaining organization-trail
  dependency;
- retain or review delivered evidence before deleting storage resources.

## Validation

Success criteria after execution:

- `list-aws-service-access-for-organization` includes
  `cloudtrail.amazonaws.com`;
- `get-trail` shows `IsOrganizationTrail=true`;
- `get-trail` shows `IsMultiRegionTrail=true`;
- `get-trail` shows the intended bucket and KMS key ARN;
- `get-trail-status` shows active logging with successful delivery;
- the security-account bucket contains actual `CloudTrail/*.json.gz` and
  `CloudTrail-Digest/*.json.gz` objects under the organization path.

Validation commands:

```bash
aws organizations list-aws-service-access-for-organization \
  --profile org-admin \
  --output json

aws cloudtrail get-trail \
  --profile org-admin \
  --region eu-west-2 \
  --name organization-management-events \
  --output json

aws cloudtrail get-trail-status \
  --profile org-admin \
  --region eu-west-2 \
  --name organization-management-events \
  --output json

aws s3api list-objects-v2 \
  --bucket org-cloudtrail-log-archive-955659429518-eu-west-2 \
  --prefix AWSLogs/ \
  --max-keys 20
```

Completed postchange validation evidence:

- `docs/evidence/domain1-governance-cloudtrail-service-access-pretrail-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-service-access-posttrail-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-list-pretrail-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-create-trail-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-list-posttrail-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-get-trail-postchange-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-get-trail-status-postchange-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-get-trail-status-posttrail-check1-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-get-trail-status-posttrail-check2-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-get-trail-status-posttrail-check3-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-log-archive-objects-posttrail-check1-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-log-archive-objects-posttrail-check2-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-log-archive-objects-posttrail-check3-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-get-trail-status-deepcheck-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-log-archive-objects-posttrail-deepcheck-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-log-archive-org-trail-objects-posttrail-deepcheck-20260624.json`

## Cost Impact

- This is the first step that starts recurring CloudTrail delivery cost.
- The initial cost posture remains low because only management events are in
  scope.
- KMS request cost begins only as CloudTrail encrypts delivered log files.

## Approval

- Approval source: direct user instruction
- Approval text: `Proceed to the next life step`
- Approval date: 2026-06-24
- Approval status: executed
- Dependency note: this note assumes the storage-step note has already been
  executed successfully

## Result

The CloudTrail/log archive path is now packaged into two clean live units:

1. storage ownership in the `Security Log Archive` account;
2. organization-trail enablement from the management account.

Both units are now complete.

That keeps the path enterprise-shaped and avoids mixing storage bring-up,
trusted-access enablement, and organization logging in one apply.

## References

- Creating a trail for an organization:
  `https://docs.aws.amazon.com/awscloudtrail/latest/userguide/creating-trail-organization.html`
- Creating a trail for an organization with the AWS CLI:
  `https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-create-and-update-an-organizational-trail-by-using-the-aws-cli.html`
