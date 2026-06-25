# Domain 1 Governance Change Note - AWS Config Lakehouse Recorder Step - 2026-06-25

<!-- markdownlint-disable MD013 -->

## Status Note

This lakehouse-account step was executed live on 2026-06-25 under explicit
user approval.

The resulting live state is:

- service-linked role `AWSServiceRoleForConfig` now exists in the lakehouse
  account;
- customer managed recorder `default` now exists in `eu-west-2`;
- recorder status is now `SUCCESS` with `recording=true`;
- delivery channel `default` now points to bucket
  `org-config-log-archive-955659429518-eu-west-2` and KMS key
  `arn:aws:kms:eu-west-2:955659429518:key/8078ec71-b17b-4826-b904-8cf62c0ad94b`;
- a manual snapshot delivery succeeded from the lakehouse account;
- no Config rules were created as part of this step;
- this step did not modify the already-live security-account organization
  aggregator.

## Target Account And OU

- Target account: `464975959576` / `lakehouse-workload-account`
- Target OU: `ou-gbyf-m6ppfmpq` / `Lakehouse Workloads OU`
- Home Region: `eu-west-2`
- Aggregator target account already live:
  `955659429518` / `Security Log Archive`
- Aggregator name already live:
  `organization-config-aggregator-eu-west-2`
- Config archive bucket already live:
  `org-config-log-archive-955659429518-eu-west-2`

## Current State

As of 2026-06-25, the organization-level AWS Config control plane and the
management-account seed recorder already exist, but the lakehouse account still
has no recorder state of its own:

- the delegated-admin and aggregation path was already established on
  2026-06-24 in the security account;
- the management account already has a successful first recorder and delivery
  channel rollout recorded on 2026-06-24;
- the lakehouse account currently has no `AWSServiceRoleForConfig`
  service-linked role;
- the lakehouse account currently has no customer managed configuration
  recorder, recorder status, delivery channel, delivery-channel status, or
  Config rules in `eu-west-2`.

Evidence files:

- `docs/evidence/domain1-governance-config-lakehouse-sts-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-lakehouse-service-linked-role-prechange-20260625.err`
- `docs/evidence/domain1-governance-config-lakehouse-recorders-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-lakehouse-recorder-status-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-lakehouse-delivery-channels-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-lakehouse-delivery-channel-status-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-lakehouse-rules-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-lakehouse-aggregators-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-management-recorder-change-note-20260624.md`
- `docs/evidence/domain1-governance-config-organization-aggregation-change-note-20260624.md`

## Proposed Change

Repeat the proven recorder rollout pattern for the lakehouse account only:

1. create the AWS Config service-linked role `AWSServiceRoleForConfig` in the
   lakehouse account;
2. create one customer managed configuration recorder in `eu-west-2` using the
   service-linked role ARN;
3. set the recorder to continuous recording with the same baseline regional
   posture used in the management account: all supported resource types,
   excluding global IAM resource types;
4. create one delivery channel in `eu-west-2` that points to
   `org-config-log-archive-955659429518-eu-west-2` and the existing
   Config KMS key in the security account;
5. start the lakehouse recorder;
6. trigger one configuration snapshot delivery to validate the cross-account
   bucket and KMS path from the lakehouse account;
7. do not enable the security-account recorder or any Config rules in this same
   change.

## Why This Boundary

This is the next correct bounded step in the sequence:

- it repeats the already-proven management-account pattern instead of designing
  a new rollout shape;
- it advances the applied Energy Data Lakehouse account directly, which is the
  most relevant workload boundary in the repo;
- it leaves the security-account recorder as the final account-local recorder
  step after the lakehouse path is proven;
- it keeps Config rule enablement deferred until recorder rollout is stable in
  all intended accounts.

## Trade-Offs

Accepted in this boundary:

- roll out only the lakehouse account now, not the security account as well;
- keep the recorder broad within the active Region rather than trying to
  micro-scope resource types early;
- use the service-linked role for the lakehouse recorder, matching the
  management-account pattern.

Rejected for this boundary:

- jumping straight to Config rules, because that would blur baseline recording
  rollout with compliance-signal design;
- enabling the security-account recorder first, because the lakehouse account
  is the more important applied workload boundary;
- changing Region scope, because the accepted design still centers on
  `eu-west-2`.

## Expected Blast Radius

- creates one AWS service-linked role in the lakehouse account;
- creates one customer managed configuration recorder and one delivery channel
  in `eu-west-2`;
- starts AWS Config recording cost in the lakehouse account;
- writes configuration history and snapshot objects into the existing
  security-account bucket;
- does not change the security-account recorder state.

## Rollback Path

If this lakehouse step needs to be rolled back before the security-account
recorder is enabled:

1. stop the customer managed configuration recorder;
2. delete the delivery channel;
3. delete the customer managed configuration recorder;
4. leave already-delivered S3 evidence in place unless there is an explicit
   later retention decision to remove it;
5. delete the service-linked role only if no AWS Config dependency still uses
   it.

Do not use this rollback after rules or additional recorder-dependent controls
are enabled. Reassess dependencies first.

## Validation

Success criteria after the step:

- the lakehouse account has `AWSServiceRoleForConfig`;
- the lakehouse account has one customer managed recorder in `eu-west-2`;
- the recorder status shows recording is enabled;
- the lakehouse account has one delivery channel pointing at the security
  account bucket and Config KMS key;
- snapshot delivery can be triggered successfully;
- the delivery-channel status reports snapshot success;
- the step does not modify the already-live organization aggregator.

Completed postchange validation evidence:

- `docs/evidence/domain1-governance-config-lakehouse-service-linked-role-create-20260625.json`
- `docs/evidence/domain1-governance-config-lakehouse-service-linked-role-postchange-20260625.json`
- `docs/evidence/domain1-governance-config-lakehouse-recorder-applied-20260625.json`
- `docs/evidence/domain1-governance-config-lakehouse-recorders-postchange-20260625.json`
- `docs/evidence/domain1-governance-config-lakehouse-recorder-status-postchange-20260625.json`
- `docs/evidence/domain1-governance-config-lakehouse-recorder-status-postverify-20260625.json`
- `docs/evidence/domain1-governance-config-lakehouse-delivery-channel-applied-20260625.json`
- `docs/evidence/domain1-governance-config-lakehouse-delivery-channels-postchange-20260625.json`
- `docs/evidence/domain1-governance-config-lakehouse-delivery-channel-status-poststart-20260625.json`
- `docs/evidence/domain1-governance-config-lakehouse-deliver-config-snapshot-20260625.json`
- `docs/evidence/domain1-governance-config-lakehouse-delivery-channel-status-postsnapshot-20260625.json`

## Next Bounded Step After This One

After this lakehouse rollout is validated, the next bounded AWS Config step
should be the same recorder-and-delivery-channel rollout for the security
account itself.
