# Domain 1 Governance Change Note - AWS Config Log Archive Storage Baseline - 2026-06-24

<!-- markdownlint-disable MD013 -->

## Status Note

This storage boundary was executed live on 2026-06-24 under explicit user
approval.

The resulting live state is:

- S3 bucket `org-config-log-archive-955659429518-eu-west-2` now exists
  in account `955659429518` and Region `eu-west-2`;
- S3 Block Public Access is fully enabled on that bucket;
- bucket versioning is enabled;
- default bucket encryption now points to customer-managed KMS key
  `arn:aws:kms:eu-west-2:955659429518:key/8078ec71-b17b-4826-b904-8cf62c0ad94b`;
- KMS alias `alias/org-aws-config-log-archive` now resolves to that key;
- exact bucket and KMS policies now scope the intended management, lakehouse,
  and security-account delivery paths only;
- the post-storage check still shows no AWS Config recorder, delivery channel,
  or aggregator in the security account;
- no trusted-access, delegated-administrator, or rule enablement was bundled
  into this change.

## Target Account And OU

- Target account: `955659429518` / `Security Log Archive`
- Target OU: `ou-gbyf-mug20ym0` / `Security OU`
- Target Region: `eu-west-2`
- Intended first recorder source accounts:
  `349687196588`, `464975959576`, and `955659429518`
- Recommended bucket name:
  `org-config-log-archive-955659429518-eu-west-2`
- Recommended KMS alias: `alias/org-aws-config-log-archive`

## Current State

Fresh baseline evidence now confirms that AWS Config is still absent in the
first in-scope accounts and that the organization has not yet enabled the
Config organization-integration prerequisites:

- the management account currently has no AWS Config recorder, recorder status,
  delivery channel, rules, or aggregator in `eu-west-2`;
- the lakehouse workload account currently has no AWS Config recorder,
  recorder status, delivery channel, rules, or aggregator in `eu-west-2`;
- the security/log archive account currently has no AWS Config recorder,
  recorder status, delivery channel, rules, or aggregator in `eu-west-2`;
- the organization currently does not expose
  `config.amazonaws.com` or `config-multiaccountsetup.amazonaws.com` trusted
  access;
- the organization currently has no AWS Config delegated administrator
  registered for either service principal;
- the security account already has the CloudTrail archive bucket and key, but
  does not yet have a dedicated AWS Config archive bucket or AWS Config KMS
  alias.

Evidence files:

- `docs/evidence/domain1-governance-config-management-sts-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-management-recorders-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-management-recorder-status-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-management-delivery-channels-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-management-rules-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-management-aggregators-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-lakehouse-sts-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-lakehouse-recorders-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-lakehouse-recorder-status-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-lakehouse-delivery-channels-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-lakehouse-rules-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-lakehouse-aggregators-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-security-sts-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-security-recorders-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-security-recorder-status-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-security-delivery-channels-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-security-rules-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-security-aggregators-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-service-access-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-delegated-admin-config-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-delegated-admin-multiaccountsetup-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-security-log-archive-s3-buckets-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-log-archive-kms-aliases-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-log-archive-kms-keys-prechange-20260624.json`

Read-only commands used:

```bash
aws organizations list-aws-service-access-for-organization \
  --profile org-admin \
  --output json

aws organizations list-delegated-administrators \
  --profile org-admin \
  --service-principal config.amazonaws.com \
  --output json

aws organizations list-delegated-administrators \
  --profile org-admin \
  --service-principal config-multiaccountsetup.amazonaws.com \
  --output json

aws configservice describe-configuration-recorders \
  --region eu-west-2 \
  --output json

aws configservice describe-configuration-recorder-status \
  --region eu-west-2 \
  --output json

aws configservice describe-delivery-channels \
  --region eu-west-2 \
  --output json

aws configservice describe-config-rules \
  --region eu-west-2 \
  --output json

aws configservice describe-configuration-aggregators \
  --region eu-west-2 \
  --output json
```

## Proposed Change

Create the dedicated AWS Config storage boundary inside the `Security Log
Archive` account as its own narrow live step:

1. create one S3 bucket named
   `org-config-log-archive-955659429518-eu-west-2`;
2. enable S3 Block Public Access and versioning on that bucket;
3. create one customer-managed KMS key in `eu-west-2` and assign the alias
   `alias/org-aws-config-log-archive`;
4. set default bucket encryption to SSE-KMS with that key;
5. attach the exact AWS Config bucket policy from
   `docs/policies/s3-config-log-archive-bucket-policy.example.json`
   after substituting live values;
6. attach the exact AWS Config KMS key policy from
   `docs/policies/kms-config-log-archive-key-policy.example.json`
   after substituting live values;
7. do not enable AWS Config trusted access, delegated administration,
   aggregators, recorders, delivery channels, or rules in this same change.

## Why This Boundary

This is the smallest SAP-C02-aligned live AWS Config step after the CloudTrail
foundations:

- it preserves the intended centralized security/log archive ownership model;
- it keeps storage ownership separate from later organization-level Config
  service-access and delegated-administrator changes;
- it avoids combining security-account S3/KMS creation with management-account
  AWS Organizations changes;
- it makes later delivery-channel troubleshooting simpler because the bucket and
  key can be validated before any recorder starts writing;
- it leaves the root-user SCP blocker and the AWS Config control-plane rollout
  as independent workstreams.

## Trade-Offs

Accepted in this boundary:

- use one bucket dedicated to AWS Config archive delivery instead of reusing
  the existing CloudTrail archive bucket;
- use one customer-managed KMS key dedicated to AWS Config archive delivery
  instead of reusing the existing CloudTrail KMS key;
- keep the first-wave source-account scope to management, lakehouse, and
  security accounts only, leaving the container sandbox for a later explicit
  revisit after the baseline path is stable.

Rejected for this boundary:

- reuse the CloudTrail bucket and key, because that would merge two different
  policy surfaces and make rollback or troubleshooting noisier;
- enable org trusted access and delegated administration first, because that
  raises the blast radius before the delivery target exists;
- start with AWS Config rules, because rules add cost and compliance noise
  before recorders and aggregation are proven healthy.

## Expected Blast Radius

- creates one new bucket in the `Security Log Archive` account;
- creates one new customer-managed KMS key in the same account and Region;
- starts small ongoing S3 and KMS cost, but no AWS Config rule-evaluation cost;
- does not change any workload account runtime permissions directly;
- does not yet enable recording or aggregation.

## Rollback Path

If this storage step is executed before any delivery channel points to the
bucket:

- verify that no AWS Config delivery channel uses the bucket;
- verify that the bucket is empty;
- remove the bucket policy if required;
- delete the bucket;
- schedule KMS key deletion deliberately rather than trying immediate removal;
- capture post-rollback evidence separately.

If configuration snapshots or history objects already exist in the bucket, stop
and reassess before deleting retained audit evidence.

## Validation

Success criteria after the storage step:

- the bucket exists in `eu-west-2`;
- Block Public Access is fully enabled;
- versioning is enabled;
- default encryption points to the intended customer-managed key;
- the bucket policy contains explicit management, lakehouse, and security
  delivery statements only;
- the KMS key exists and the alias `alias/org-aws-config-log-archive` resolves
  to it;
- the KMS key policy allows the AWS Config service principal from the intended
  source accounts only;
- no AWS Config recorder, delivery channel, rule, or aggregator was created as
  part of this storage-only change.

Completed postchange validation evidence:

- `docs/evidence/domain1-governance-config-log-archive-bucket-create-20260624.json`
- `docs/evidence/domain1-governance-config-log-archive-bucket-location-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-log-archive-public-access-block-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-log-archive-versioning-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-log-archive-encryption-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-log-archive-bucket-policy-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-security-log-archive-s3-buckets-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-log-archive-kms-key-create-20260624.json`
- `docs/evidence/domain1-governance-config-log-archive-kms-alias-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-log-archive-kms-key-policy-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-security-recorders-poststorage-check-20260624.json`
- `docs/evidence/domain1-governance-config-security-delivery-channels-poststorage-check-20260624.json`
- `docs/evidence/domain1-governance-config-security-aggregators-poststorage-check-20260624.json`

## Next Bounded Step After This One

After the storage boundary is validated, the next AWS Config step should be its
own separate control-plane change unit:

1. enable `config.amazonaws.com` and
   `config-multiaccountsetup.amazonaws.com` trusted access;
2. register account `955659429518` as delegated administrator for both service
   principals;
3. create the organization aggregator in `eu-west-2` from that account using a
   dedicated organization-read role;
4. only after that, enable the first recorders and delivery channels in the
   management, lakehouse, and security accounts.
