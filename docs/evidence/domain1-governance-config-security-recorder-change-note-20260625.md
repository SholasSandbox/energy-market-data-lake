# Domain 1 Governance Change Note - AWS Config Security Account Recorder Step - 2026-06-25

<!-- markdownlint-disable MD013 -->

## Status Note

This security-account step was executed live on 2026-06-25 under explicit
user approval.

The resulting live state is:

- service-linked role `AWSServiceRoleForConfig` now exists in the security
  account;
- customer managed recorder `default` now exists in `eu-west-2`;
- recorder status is now `SUCCESS` with `recording=true`;
- delivery channel `default` now points to bucket
  `org-config-log-archive-955659429518-eu-west-2` and KMS key
  `arn:aws:kms:eu-west-2:955659429518:key/8078ec71-b17b-4826-b904-8cf62c0ad94b`;
- a manual snapshot delivery succeeded from the security account;
- the security-account archive bucket now contains both the AWS Config
  writability check file and a real snapshot object under
  `AWSLogs/955659429518/Config/`;
- the organization aggregator remains present with successful source status;
- no Config rules were created as part of this step.

## Target Account And OU

- Target account: `955659429518` / `Security Log Archive`
- Target OU: `ou-gbyf-mug20ym0` / `Security OU`
- Home Region: `eu-west-2`
- Aggregator account: `955659429518` / `Security Log Archive`
- Aggregator name already live:
  `organization-config-aggregator-eu-west-2`
- Config archive bucket already live:
  `org-config-log-archive-955659429518-eu-west-2`

## Current State

As of 2026-06-25, the storage boundary, organization aggregation control
plane, management-account recorder, and lakehouse-account recorder already
exist, but the security account still had no local recorder state of its own:

- the delegated-admin and aggregation path was already established on
  2026-06-24 in the security account;
- the management-account recorder and delivery channel were already live from
  2026-06-24;
- the lakehouse-account recorder and delivery channel were already live from
  2026-06-25;
- the security account currently had no `AWSServiceRoleForConfig`
  service-linked role;
- the security account currently had no customer managed configuration
  recorder, recorder status, delivery channel, delivery-channel status, or
  Config rules in `eu-west-2`;
- the security-account bucket currently had no objects under
  `AWSLogs/955659429518/Config/`.

Evidence files:

- `docs/evidence/domain1-governance-config-security-sts-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-security-service-linked-role-prechange-20260625.err`
- `docs/evidence/domain1-governance-config-security-recorders-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-security-recorder-status-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-security-delivery-channels-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-security-delivery-channel-status-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-security-rules-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-security-bucket-objects-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-organization-aggregation-change-note-20260624.md`
- `docs/evidence/domain1-governance-config-management-recorder-change-note-20260624.md`
- `docs/evidence/domain1-governance-config-lakehouse-recorder-change-note-20260625.md`

## Proposed Change

Repeat the proven recorder rollout pattern for the security account only:

1. create the AWS Config service-linked role `AWSServiceRoleForConfig` in the
   security account;
2. create one customer managed configuration recorder in `eu-west-2` using the
   service-linked role ARN;
3. set the recorder to continuous recording with the same baseline regional
   posture used in the management and lakehouse accounts: all supported
   resource types, excluding global IAM resource types;
4. create one delivery channel in `eu-west-2` that points to
   `org-config-log-archive-955659429518-eu-west-2` and the existing
   Config KMS key in the same account;
5. start the security-account recorder;
6. trigger one configuration snapshot delivery to validate the local bucket and
   KMS path for the archive account itself;
7. do not enable any Config rules in this same change.

## Why This Boundary

This is the next correct bounded step in the sequence:

- it finishes the intended three-account recorder rollout without widening into
  rules or additional security services;
- it proves that the archive account can both host the central bucket and
  record its own configuration state cleanly;
- it keeps the remaining open AWS Config work to compliance-signal design
  rather than more recorder plumbing;
- it preserves the same `eu-west-2`-only baseline already accepted elsewhere
  in the repo.

## Trade-Offs

Accepted in this boundary:

- roll out only the security-account recorder now, not the first Config rule
  set;
- use the AWS Config service-linked role again, matching the management and
  lakehouse pattern;
- validate same-account delivery into the archive bucket now instead of waiting
  for a later rule phase.

Rejected for this boundary:

- jumping straight to Config rules, because rule enablement should follow after
  recorder rollout is stable across all intended accounts;
- bundling GuardDuty or Security Hub work, because that would widen both cost
  and troubleshooting scope;
- changing the active Region design, because the accepted baseline still
  centers on `eu-west-2`.

## Expected Blast Radius

- creates one AWS service-linked role in the security account;
- creates one customer managed configuration recorder and one delivery channel
  in `eu-west-2`;
- starts AWS Config recording cost in the security account;
- writes configuration history and snapshot objects into the existing
  security-account archive bucket;
- does not create any Config rules.

## Rollback Path

If this security-account step needs to be rolled back before Config rules are
enabled:

1. stop the customer managed configuration recorder;
2. delete the delivery channel;
3. delete the customer managed configuration recorder;
4. leave already-delivered S3 evidence in place unless there is an explicit
   later retention decision to remove it;
5. delete the service-linked role only if no AWS Config dependency still uses
   it;
6. do not delete the existing organization aggregator as part of a recorder-only
   rollback.

Do not use this rollback after rules or additional recorder-dependent controls
are enabled. Reassess dependencies first.

## Validation

Success criteria after the step:

- the security account has `AWSServiceRoleForConfig`;
- the security account has one customer managed recorder in `eu-west-2`;
- the recorder status shows recording is enabled and `lastStatus=SUCCESS`;
- the security account has one delivery channel pointing at the security
  account bucket and Config KMS key;
- snapshot delivery can be triggered successfully;
- the security-account bucket shows Config objects under
  `AWSLogs/955659429518/Config/`;
- the organization aggregator remains present and source status remains
  successful;
- no Config rules are created by the step.

Completed postchange validation evidence:

- `docs/evidence/domain1-governance-config-security-service-linked-role-create-20260625.json`
- `docs/evidence/domain1-governance-config-security-service-linked-role-postchange-20260625.json`
- `docs/evidence/domain1-governance-config-security-recorder-applied-20260625.json`
- `docs/evidence/domain1-governance-config-security-recorders-postchange-20260625.json`
- `docs/evidence/domain1-governance-config-security-recorder-status-postchange-20260625.json`
- `docs/evidence/domain1-governance-config-security-recorder-status-postverify-20260625.json`
- `docs/evidence/domain1-governance-config-security-delivery-channel-applied-20260625.json`
- `docs/evidence/domain1-governance-config-security-delivery-channels-postchange-20260625.json`
- `docs/evidence/domain1-governance-config-security-delivery-channel-status-poststart-20260625.json`
- `docs/evidence/domain1-governance-config-security-deliver-config-snapshot-20260625.json`
- `docs/evidence/domain1-governance-config-security-delivery-channel-status-postsnapshot-20260625.json`
- `docs/evidence/domain1-governance-config-security-bucket-objects-postchange-20260625.json`
- `docs/evidence/domain1-governance-config-security-aggregators-postsecurity-step-20260625.json`
- `docs/evidence/domain1-governance-config-security-aggregator-sources-status-postsecurity-step-20260625.json`
- `docs/evidence/domain1-governance-config-security-rules-postchange-20260625.json`

## Next Bounded Step After This One

After this security-account rollout, the recorder baseline is live in all
three intended accounts. The next bounded AWS Config step should be one narrow
rule change note, starting with a governance-relevant rule such as CloudTrail
presence/hygiene, S3 public-exposure prevention, or required-tag governance.
