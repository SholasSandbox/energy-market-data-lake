# Domain 1 Governance Change Note - AWS Config Management Recorder First Step - 2026-06-24

<!-- markdownlint-disable MD013 -->

## Status Note

This management-account seed step was executed live on 2026-06-24 under
explicit user approval.

The resulting live state is:

- service-linked role `AWSServiceRoleForConfig` now exists in the management
  account;
- customer managed recorder `default` now exists in `eu-west-2`;
- recorder status is now `SUCCESS` with `recording=true`;
- delivery channel `default` now points to bucket
  `org-config-log-archive-955659429518-eu-west-2` and KMS key
  `arn:aws:kms:eu-west-2:955659429518:key/8078ec71-b17b-4826-b904-8cf62c0ad94b`;
- a manual snapshot delivery succeeded;
- the security-account bucket now contains both the AWS Config writability
  check file and a real snapshot object under
  `AWSLogs/349687196588/Config/`;
- the organization aggregator remains present after the management-account
  rollout.

## Target Account And OU

- Target account: `349687196588` / `management-account-alias`
- Account role: AWS Organizations management account
- Home Region: `eu-west-2`
- Aggregator target account: `955659429518` / `Security Log Archive`
- Aggregator name already live:
  `organization-config-aggregator-eu-west-2`
- Config archive bucket already live:
  `org-config-log-archive-955659429518-eu-west-2`

## Current State

The AWS Config storage boundary and organization aggregation control plane now
exist, but the management account itself still has no customer managed
configuration recorder or delivery channel:

- `config.amazonaws.com` and
  `config-multiaccountsetup.amazonaws.com` trusted access are already enabled;
- delegated administrator `955659429518` is already registered for both AWS
  Config service principals;
- the security-account organization aggregator already exists and reports
  successful organization-source status in `eu-west-2`;
- the management account currently has no `AWSServiceRoleForConfig`
  service-linked role;
- the management account currently has no customer managed configuration
  recorder, recorder status, delivery channel, or delivery-channel status in
  `eu-west-2`.

Evidence files:

- `docs/evidence/domain1-governance-config-service-access-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-delegated-admin-config-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-delegated-admin-multiaccountsetup-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-security-aggregators-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-security-aggregator-sources-status-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-management-recorders-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-management-recorder-status-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-management-delivery-channels-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-management-delivery-channel-status-prechange-20260624.json`
- `docs/evidence/domain1-governance-config-management-service-linked-role-prechange-20260624.err`

## Proposed Change

Execute the smallest recorder rollout that advances the design without opening a
full three-account wave:

1. create the AWS Config service-linked role `AWSServiceRoleForConfig` in the
   management account;
2. create one customer managed configuration recorder in `eu-west-2` using the
   service-linked role ARN;
3. set the recorder to continuous recording with the baseline regional posture:
   all supported resource types, excluding global IAM resource types;
4. create one delivery channel in `eu-west-2` that points to
   `org-config-log-archive-955659429518-eu-west-2` and the dedicated
   Config KMS key already live in the security account;
5. start the management-account recorder;
6. trigger one configuration snapshot delivery to validate that the bucket and
   key path work end to end;
7. do not enable the lakehouse or security-account recorders in this same
   change.

## Why This Boundary

This is the smallest defensible next step while weekly usage is nearly
exhausted:

- it proves that the new Config archive bucket, KMS key, delegated-admin path,
  and aggregator can support a real source account;
- it keeps the live rollout to one account instead of three;
- it uses the management account, where working access is simplest and rollback
  is easiest to reason about;
- it leaves the lakehouse and security-account recorders as the next repeatable
  bounded step.

## Trade-Offs

Accepted in this boundary:

- use only the management account now instead of completing the whole
  management-plus-lakehouse-plus-security wave in one pass;
- use the AWS Config service-linked role for the customer managed recorder
  rather than inventing a separate custom IAM role;
- keep global IAM resource types out of scope for this first pass, matching the
  current design posture.

Rejected for this boundary:

- rolling out all three accounts now, because that is more likely to overrun
  the remaining weekly allowance and would widen troubleshooting scope;
- stopping at recorder creation without starting it, because that would not
  validate the cross-account bucket and KMS path;
- bundling Config rules now, because rule enablement belongs after recorder
  rollout is stable.

## Expected Blast Radius

- creates one AWS service-linked role in the management account;
- creates one customer managed configuration recorder and one delivery channel
  in `eu-west-2`;
- starts AWS Config recording cost in the management account only;
- writes configuration history and snapshot objects into the security-account
  bucket;
- does not change the lakehouse or security-account recorder state.

## Rollback Path

If this first-account step needs to be rolled back before other accounts are
enabled:

1. stop the customer managed configuration recorder;
2. delete the delivery channel;
3. delete the customer managed configuration recorder;
4. leave the already-captured S3 evidence in place unless there is an explicit
   retention decision to remove it later;
5. delete the service-linked role only if no AWS Config feature in the account
   still depends on it.

Do not use this rollback blindly after additional accounts or Config rules are
enabled. Reassess dependencies first.

## Validation

Success criteria after the step:

- the management account has `AWSServiceRoleForConfig`;
- the management account has one customer managed recorder in `eu-west-2`;
- the recorder status shows recording is enabled;
- the management account has one delivery channel pointing at the security
  account bucket and Config KMS key;
- snapshot delivery can be triggered successfully;
- the security-account bucket shows Config objects under
  `AWSLogs/349687196588/Config/`;
- the organization aggregator remains present after the step.

Completed postchange validation evidence:

- `docs/evidence/domain1-governance-config-management-service-linked-role-create-20260624.json`
- `docs/evidence/domain1-governance-config-management-service-linked-role-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-management-recorder-applied-20260624.json`
- `docs/evidence/domain1-governance-config-management-recorders-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-management-recorder-status-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-management-recorder-status-postverify-20260624.json`
- `docs/evidence/domain1-governance-config-management-delivery-channel-applied-20260624.json`
- `docs/evidence/domain1-governance-config-management-delivery-channels-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-management-delivery-channel-status-poststart-20260624.json`
- `docs/evidence/domain1-governance-config-management-deliver-config-snapshot-20260624.json`
- `docs/evidence/domain1-governance-config-management-delivery-channel-status-postsnapshot-20260624.json`
- `docs/evidence/domain1-governance-config-management-security-bucket-objects-postchange-20260624.json`
- `docs/evidence/domain1-governance-config-security-aggregators-postmanagement-step-20260624.json`

## Next Bounded Step After This One

After this management-account seed rollout is validated, the next step should
repeat the same recorder-and-delivery-channel pattern for the lakehouse account,
then for the security account.
