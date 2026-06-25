# Domain 1 Governance Change Note - AWS Config Organization Aggregation Control Plane - 2026-06-24

<!-- markdownlint-disable MD013 -->

## Status Note

This control-plane boundary was executed live on 2026-06-24 under explicit
user approval.

The resulting live state is:

- `config.amazonaws.com` and `config-multiaccountsetup.amazonaws.com` are now
  enabled as trusted services in the organization;
- account `955659429518` / `Security Log Archive` is now the delegated
  administrator for both AWS Config service principals;
- IAM role `aws-config-organization-aggregator-role` now exists in the
  security account with trust to `config.amazonaws.com`;
- AWS managed policy `AWSConfigRoleForOrganizations` is attached to that role;
- aggregator `organization-config-aggregator-eu-west-2` now exists in the
  security account in `eu-west-2`;
- the aggregator source status already reports `SUCCEEDED` for the
  organization source in `eu-west-2`;
- no AWS Config recorder, delivery channel, or rule was created as part of
  this control-plane change.

## Target Account And OU

- Management account: `349687196588` / `management-account-alias`
- Delegated administrator target account: `955659429518` / `Security Log Archive`
- Target OU: `ou-gbyf-mug20ym0` / `Security OU`
- Home Region: `eu-west-2`
- Recommended delegated-admin aggregator role:
  `aws-config-organization-aggregator-role`
- Recommended aggregator name:
  `organization-config-aggregator-eu-west-2`

## Current State

Fresh read-only evidence confirms that the AWS Config storage boundary now
exists, but the organization-level control plane is still absent:

- the organization currently does not expose
  `config.amazonaws.com` or `config-multiaccountsetup.amazonaws.com` trusted
  access;
- the organization currently has no delegated administrator registered for
  either AWS Config service principal;
- the security account currently has no organization-aggregator IAM role;
- the security account currently has no AWS Config aggregator;
- the management, lakehouse, and security accounts still have no AWS Config
  recorders or delivery channels, so the aggregator should be created now as a
  control-plane boundary only, not as a data-populated reporting surface yet.

Evidence files:

- `docs/evidence/domain1-governance-config-service-access-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-delegated-admin-config-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-delegated-admin-multiaccountsetup-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-security-aggregators-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-security-recorders-poststorage-check-20260624.json`
- `docs/evidence/domain1-governance-config-security-delivery-channels-poststorage-check-20260624.json`
- `docs/evidence/domain1-governance-config-security-aggregator-role-prechange-20260624.err`
- `docs/evidence/domain1-governance-config-awsconfigrolefororganizations-managed-policy-20260624.json`
- `docs/evidence/domain1-governance-config-log-archive-storage-change-note-20260624.md`

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

aws configservice describe-configuration-aggregators \
  --region eu-west-2 \
  --output json

aws iam get-policy \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSConfigRoleForOrganizations \
  --output json

aws iam get-role \
  --role-name aws-config-organization-aggregator-role \
  --output json
```

## Proposed Change

Execute the first AWS Config organization control-plane step as one bounded
change:

1. enable trusted access for
   `config-multiaccountsetup.amazonaws.com` from the management account;
2. enable trusted access for `config.amazonaws.com` from the management
   account;
3. register member account `955659429518` as delegated administrator for
   `config-multiaccountsetup.amazonaws.com`;
4. register member account `955659429518` as delegated administrator for
   `config.amazonaws.com`;
5. in the security account, create IAM role
   `aws-config-organization-aggregator-role` using trust policy
   `docs/policies/iam-config-organization-aggregator-role-trust-policy.example.json`;
6. attach AWS managed policy
   `arn:aws:iam::aws:policy/service-role/AWSConfigRoleForOrganizations` to
   that role;
7. in the security account, create aggregator
   `organization-config-aggregator-eu-west-2` with
   `OrganizationAggregationSource` scoped to `eu-west-2` and the dedicated
   role ARN;
8. do not enable AWS Config recorders, delivery channels, or rules in this
   same change.

Recommended command shape:

```bash
aws organizations enable-aws-service-access \
  --service-principal config-multiaccountsetup.amazonaws.com

aws organizations enable-aws-service-access \
  --service-principal config.amazonaws.com

aws organizations register-delegated-administrator \
  --account-id 955659429518 \
  --service-principal config-multiaccountsetup.amazonaws.com

aws organizations register-delegated-administrator \
  --account-id 955659429518 \
  --service-principal config.amazonaws.com

aws iam create-role \
  --role-name aws-config-organization-aggregator-role \
  --assume-role-policy-document \
  file://docs/policies/iam-config-organization-aggregator-role-trust-policy.example.json

aws iam attach-role-policy \
  --role-name aws-config-organization-aggregator-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSConfigRoleForOrganizations

aws configservice put-configuration-aggregator \
  --configuration-aggregator-name organization-config-aggregator-eu-west-2 \
  --organization-aggregation-source \
  RoleArn=arn:aws:iam::955659429518:role/aws-config-organization-aggregator-role,AwsRegions=eu-west-2,AllAwsRegions=false
```

## Why This Boundary

This is the smallest correct follow-on step after the storage boundary:

- it completes the organization integration and delegated-admin shape without
  yet starting resource recording;
- it keeps control-plane setup separate from recorder and delivery-channel
  rollout, which reduces troubleshooting scope;
- it follows the SAP-C02-preferred pattern of centralizing governance
  aggregation into the security account rather than the management account;
- it creates the exact home for later organization-aware Config reasoning
  before any compliance rules are introduced.

## Trade-Offs

Accepted in this boundary:

- use the security account, not the management account, as the delegated-admin
  aggregator owner;
- limit the aggregator to `eu-west-2` rather than all Regions for the first
  pass;
- create a dedicated organization-read role instead of relying on a looser IAM
  pattern.

Rejected for this boundary:

- creating the aggregator in the management account, because it weakens the
  intended security-operations boundary;
- enabling all Regions now, because the current design intentionally keeps
  Region scope narrow;
- bundling recorder and delivery-channel enablement with the aggregator, because
  that would blur control-plane and account-local failures.

## Expected Blast Radius

- changes AWS Organizations trusted-service state for two AWS Config service
  principals;
- grants delegated-administrator rights for AWS Config to member account
  `955659429518`;
- creates one IAM role in the security account and attaches one AWS managed
  policy to it;
- creates one AWS Config aggregator in `eu-west-2`;
- does not yet start account-level configuration recording or rule evaluation.

## Rollback Path

If this control-plane step needs to be rolled back before recorders or
organization rules are enabled:

1. delete the aggregator
   `organization-config-aggregator-eu-west-2`;
2. detach the managed policy from
   `aws-config-organization-aggregator-role`;
3. delete the role;
4. deregister delegated administrator `955659429518` for
   `config.amazonaws.com`;
5. deregister delegated administrator `955659429518` for
   `config-multiaccountsetup.amazonaws.com`;
6. disable the two trusted-service principals only if no other Config
   organization features now depend on them.

Do not use this rollback if later recorder, organization-rule, or conformance
pack work has already started. Reassess dependency impact first.

## Validation

Success criteria after the control-plane step:

- `list-aws-service-access-for-organization` includes both
  `config.amazonaws.com` and `config-multiaccountsetup.amazonaws.com`;
- `list-delegated-administrators` returns account `955659429518` for both
  service principals;
- role `aws-config-organization-aggregator-role` exists in the security account
  with trust to `config.amazonaws.com`;
- the role has AWS managed policy
  `AWSConfigRoleForOrganizations` attached;
- aggregator `organization-config-aggregator-eu-west-2` exists in the security
  account in `eu-west-2`;
- the aggregator points to role
  `arn:aws:iam::955659429518:role/aws-config-organization-aggregator-role`;
- no AWS Config recorders, delivery channels, or rules were created by this
  control-plane change.

Validation commands:

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

aws iam get-role \
  --role-name aws-config-organization-aggregator-role \
  --output json

aws iam list-attached-role-policies \
  --role-name aws-config-organization-aggregator-role \
  --output json

aws configservice describe-configuration-aggregators \
  --region eu-west-2 \
  --output json

aws configservice describe-configuration-aggregator-sources-status \
  --configuration-aggregator-name organization-config-aggregator-eu-west-2 \
  --region eu-west-2 \
  --output json

aws configservice describe-configuration-recorders \
  --region eu-west-2 \
  --output json

aws configservice describe-delivery-channels \
  --region eu-west-2 \
  --output json
```

Completed postchange validation evidence:

- `docs/evidence/domain1-governance-config-service-access-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-delegated-admin-config-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-delegated-admin-multiaccountsetup-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-security-aggregator-role-create-20260624.json`
- `docs/evidence/domain1-governance-config-security-aggregator-role-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-security-aggregator-role-attached-policies-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-security-aggregator-create-20260624.json`
- `docs/evidence/domain1-governance-config-security-aggregators-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-security-aggregator-sources-status-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-security-recorders-postaggregator-check-20260624.json`
- `docs/evidence/domain1-governance-config-security-delivery-channels-postaggregator-check-20260624.json`
- `docs/evidence/domain1-governance-config-security-rules-postaggregator-check-20260624.json`

## Next Bounded Step After This One

After the control-plane step is validated, the next AWS Config step should be
the first recorder-and-delivery-channel rollout in the management, lakehouse,
and security accounts, using the dedicated security-account archive bucket and
KMS key that now exist live.
