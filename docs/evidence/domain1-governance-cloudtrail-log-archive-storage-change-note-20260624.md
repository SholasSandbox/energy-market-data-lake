# Domain 1 Governance Change Note - CloudTrail Log Archive Storage Baseline - 2026-06-24

<!-- markdownlint-disable MD013 -->

## Status Note

This storage boundary was executed live on 2026-06-24 under explicit user
approval.

The resulting live state is:

- S3 bucket `org-cloudtrail-log-archive-955659429518-eu-west-2` now exists in
  account `955659429518` and Region `eu-west-2`;
- S3 Block Public Access is fully enabled on that bucket;
- bucket versioning is enabled;
- default bucket encryption now points to customer-managed KMS key
  `arn:aws:kms:eu-west-2:955659429518:key/201876fa-d5f9-4810-9721-676b17f737f3`;
- KMS alias `alias/org-cloudtrail-log-archive` now resolves to that key;
- automatic KMS key rotation is enabled;
- the CloudTrail bucket policy now scopes writes to trail ARN
  `arn:aws:cloudtrail:eu-west-2:349687196588:trail/organization-management-events`
  and the two expected log paths;
- a later separate live step on 2026-06-24 created the management-account
  organization trail and started logging; that later trail execution is
  recorded in
  `docs/evidence/domain1-governance-cloudtrail-organization-trail-change-note-20260624.md`.

## Target Account And OU

- Target account: `955659429518` / `Security Log Archive`
- Target OU: `ou-gbyf-mug20ym0` / `Security OU`
- Target Region: `eu-west-2`
- Intended management-account trail owner: `349687196588` / `management-account-alias`
- Recommended bucket name: `org-cloudtrail-log-archive-955659429518-eu-west-2`
- Recommended KMS alias: `alias/org-cloudtrail-log-archive`
- Intended trail name to pre-scope policies now: `organization-management-events`

## Current State

Fresh read-only evidence now confirms that the dedicated security boundary is
live, but the CloudTrail storage layer does not yet exist:

- the management account session is active;
- the organization currently exposes trusted access for
  `account.amazonaws.com` and `sso.amazonaws.com`, but not yet for
  `cloudtrail.amazonaws.com`;
- `aws cloudtrail list-trails --region eu-west-2` still returns no trails;
- the `Security Log Archive` account is reachable by
  `OrganizationAccountAccessRole`;
- the `Security Log Archive` account currently has no S3 buckets;
- the `Security Log Archive` account currently has no customer-managed KMS keys
  in `eu-west-2`;
- only AWS-managed KMS aliases currently exist in the new account.

Evidence files:

- `docs/evidence/domain1-governance-cloudtrail-management-sts-prechange-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-service-access-prechange-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-list-prechange-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-security-log-archive-sts-prechange-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-security-log-archive-s3-buckets-prechange-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-security-log-archive-kms-aliases-prechange-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-security-log-archive-kms-keys-prechange-20260624.json`

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

aws sts assume-role \
  --profile org-admin \
  --role-arn arn:aws:iam::955659429518:role/OrganizationAccountAccessRole \
  --role-session-name cloudtrail-boundary-20260624 \
  --output json

aws s3api list-buckets \
  --query 'Buckets[].Name' \
  --output json

aws kms list-aliases \
  --region eu-west-2 \
  --output json

aws kms list-keys \
  --region eu-west-2 \
  --output json
```

## Proposed Change

Create the dedicated CloudTrail storage boundary inside the `Security Log
Archive` account as its own narrow live step:

1. create one S3 bucket named
   `org-cloudtrail-log-archive-955659429518-eu-west-2`;
2. enable S3 Block Public Access and versioning on that bucket;
3. create one customer-managed KMS key in `eu-west-2` and assign the alias
   `alias/org-cloudtrail-log-archive`;
4. set default bucket encryption to SSE-KMS with that key;
5. attach the exact CloudTrail bucket policy from
   `docs/policies/s3-cloudtrail-log-archive-bucket-policy.example.json`
   after substituting live values;
6. attach the exact KMS key policy from
   `docs/policies/kms-cloudtrail-log-archive-key-policy.example.json`
   after substituting live values;
7. do not create or start the organization trail in this same change.

Recommended command shape:

```bash
aws s3api create-bucket \
  --bucket org-cloudtrail-log-archive-955659429518-eu-west-2 \
  --region eu-west-2 \
  --create-bucket-configuration LocationConstraint=eu-west-2

aws s3api put-public-access-block \
  --bucket org-cloudtrail-log-archive-955659429518-eu-west-2 \
  --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

aws s3api put-bucket-versioning \
  --bucket org-cloudtrail-log-archive-955659429518-eu-west-2 \
  --versioning-configuration Status=Enabled

aws kms create-key \
  --region eu-west-2 \
  --policy file://docs/policies/kms-cloudtrail-log-archive-key-policy.example.json

aws kms create-alias \
  --region eu-west-2 \
  --alias-name alias/org-cloudtrail-log-archive \
  --target-key-id <new-key-id>

aws s3api put-bucket-encryption \
  --bucket org-cloudtrail-log-archive-955659429518-eu-west-2 \
  --server-side-encryption-configuration \
  file://docs/policies/s3-cloudtrail-log-archive-encryption.example.json

aws s3api put-bucket-policy \
  --bucket org-cloudtrail-log-archive-955659429518-eu-west-2 \
  --policy file://docs/policies/s3-cloudtrail-log-archive-bucket-policy.example.json
```

## Why This Boundary

This is the smallest SAP-C02-aligned live step after the account boundary:

- it keeps storage ownership and trail ownership separate in the intended way;
- it avoids combining S3, KMS, service access, and trail start-up in one change;
- it lets bucket and key posture be validated before CloudTrail begins
  redelivery attempts against a misconfigured target;
- it keeps the target SSE-KMS design rather than dropping to an SSE-S3 fallback.

## Trade-Offs

Accepted in this boundary:

- use one dedicated bucket with no custom prefix, so AWS-native `AWSLogs/`
  paths remain simple and exam-friendly;
- use one customer-managed KMS key in the same Region as the bucket;
- keep the management account as trail owner for now rather than introducing a
  CloudTrail delegated administrator design.

Intentionally deferred:

- S3 Bucket Keys, because they add extra KMS policy considerations during the
  first bring-up step;
- S3 Object Lock, because immutability is stronger but operationally heavier;
- lifecycle transitions, because the log volume and retention window are not
  yet proven;
- data events, Insights, and CloudTrail Lake, because the baseline target is
  management-event coverage with low noise.

## Expected Blast Radius

- creates one new bucket in the `Security Log Archive` account;
- creates one new customer-managed KMS key in the same account and Region;
- starts small ongoing S3 and KMS cost, but no CloudTrail delivery cost yet;
- does not change any workload account runtime behavior directly;
- does not yet enable logging.

## Rollback Path

If this storage step is executed before the trail exists:

- verify no CloudTrail trail is pointing at the bucket;
- verify the bucket is empty;
- remove the bucket policy if required;
- delete the bucket;
- schedule KMS key deletion deliberately rather than trying to force immediate
  removal;
- capture post-rollback evidence separately.

If the bucket or key already has trail traffic, do not treat rollback as a
lightweight delete. Stop and reassess evidence retention first.

## Validation

Success criteria after the storage step:

- the bucket exists in `eu-west-2`;
- Block Public Access is fully enabled;
- versioning is enabled;
- default encryption points to the intended customer-managed key;
- the bucket policy contains the CloudTrail ACL check plus both
  `AWSLogs/349687196588/*` and `AWSLogs/o-hmvgqmav88/*` write targets;
- the KMS key exists and the alias `alias/org-cloudtrail-log-archive` resolves
  to it;
- the KMS key policy allows CloudTrail `kms:GenerateDataKey*` and
  `kms:DescribeKey` for the intended trail ARN.
- the KMS key policy also allows CloudTrail `kms:Decrypt` and allows the
  security-account audit role to decrypt trail logs.

Validation commands:

```bash
aws s3api get-bucket-versioning \
  --bucket org-cloudtrail-log-archive-955659429518-eu-west-2

aws s3api get-bucket-encryption \
  --bucket org-cloudtrail-log-archive-955659429518-eu-west-2

aws s3api get-public-access-block \
  --bucket org-cloudtrail-log-archive-955659429518-eu-west-2

aws s3api get-bucket-policy \
  --bucket org-cloudtrail-log-archive-955659429518-eu-west-2

aws kms list-aliases \
  --region eu-west-2 \
  --query \"Aliases[?AliasName=='alias/org-cloudtrail-log-archive']\"

aws kms get-key-policy \
  --region eu-west-2 \
  --key-id <new-key-id-or-arn> \
  --policy-name default
```

Completed postchange validation evidence:

- `docs/evidence/domain1-governance-cloudtrail-log-archive-bucket-create-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-log-archive-bucket-location-postchange-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-log-archive-public-access-block-postchange-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-log-archive-versioning-postchange-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-log-archive-encryption-postchange-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-log-archive-bucket-policy-postchange-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-log-archive-objects-postchange-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-security-log-archive-s3-buckets-postchange-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-log-archive-kms-key-create-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-log-archive-kms-key-describe-postchange-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-log-archive-kms-key-policy-postchange-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-log-archive-kms-key-rotation-postchange-20260624.json`
- `docs/evidence/domain1-governance-cloudtrail-log-archive-kms-alias-postchange-20260624.json`

## Cost Impact

- S3 bucket storage and request cost begins at a small baseline.
- One customer-managed KMS key begins its standing monthly charge.
- KMS request cost stays near zero until CloudTrail actually starts writing.

## Approval

- Approval source: direct user instruction
- Approval text: `Proceed to the storage boundary first in the Security Log Archive account`
- Approval date: 2026-06-24
- Approval status: executed
- Dependency note: this change note is intentionally separate from the later
  organization-trail enablement note

## Result

The storage boundary is now live.

This note still preserves the storage boundary as its own change unit even
though the later organization-trail step has now also been executed
separately.

## References

- AWS CloudTrail bucket policy guidance:
  `https://docs.aws.amazon.com/awscloudtrail/latest/userguide/create-s3-bucket-policy-for-cloudtrail.html`
- AWS CloudTrail KMS key policy guidance:
  `https://docs.aws.amazon.com/awscloudtrail/latest/userguide/create-kms-key-policy-for-cloudtrail.html`
- Creating an organization trail with the AWS CLI:
  `https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-create-and-update-an-organizational-trail-by-using-the-aws-cli.html`
