# Domain 1 Governance Change Note - AWS Config Sandbox Recorder Step - 2026-06-25

<!-- markdownlint-disable MD013 -->

## Status Note

This sandbox-account step was executed live on 2026-06-25 under explicit user
approval.

The resulting live state is:

- service-linked role `AWSServiceRoleForConfig` exists in the sandbox account;
- customer managed recorder `default` exists in `eu-west-2`;
- recorder status reaches `SUCCESS` with `recording=true`;
- delivery channel `default` points to bucket
  `org-config-log-archive-955659429518-eu-west-2` and KMS key
  `arn:aws:kms:eu-west-2:955659429518:key/8078ec71-b17b-4826-b904-8cf62c0ad94b`;
- a manual snapshot delivery succeeds from the sandbox account;
- the central archive bucket contains Config objects under
  `AWSLogs/974893866311/Config/`;
- no new Config rules are created in this step;
- the first delivery-channel attempt failed because the central Config archive
  bucket policy and Config KMS key policy still only allowed management,
  lakehouse, and security accounts;
- the central bucket policy and KMS key policy were extended to include sandbox
  account `974893866311`, after which the same delivery-channel and recorder
  path succeeded;
- the current organization CloudTrail rule still excludes `974893866311` at the
  end of this step, which keeps re-inclusion into that rule as the next bounded
  follow-on change.

## Target Account And OU

- Target account: `974893866311` / `containers-lab.com`
- Target OU: `ou-gbyf-zs0f26b5` / `Container Sandbox`
- Home Region: `eu-west-2`
- Aggregator target account already live:
  `955659429518` / `Security Log Archive`
- Config archive bucket already live:
  `org-config-log-archive-955659429518-eu-west-2`

## Current State

As of 2026-06-25, the organization aggregator, the management/lakehouse/security
recorders, and the first sandbox-excluded organization rule already exist, but
the sandbox account still has no local recorder state:

- the sandbox account now matters as an intended workload-bearing member account
  because containers and microservices are planned to live there;
- the account currently has no `AWSServiceRoleForConfig` service-linked role;
- the account currently has no customer managed recorder, recorder status,
  delivery channel, delivery-channel status, or Config rules in `eu-west-2`;
- the central Config archive bucket currently has no objects under
  `AWSLogs/974893866311/Config/` when viewed from the security account;
- the sandbox account can see the organization CloudTrail, but that does not
  change the fact that AWS Config recorder state is absent.

Evidence files:

- `docs/evidence/domain1-governance-config-sandbox-sts-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-sandbox-service-linked-role-prechange-20260625.err`
- `docs/evidence/domain1-governance-config-sandbox-recorders-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-sandbox-recorder-status-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-sandbox-delivery-channels-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-sandbox-delivery-channel-status-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-sandbox-rules-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-sandbox-bucket-objects-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-sandbox-cloudtrail-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-org-cloudtrail-rule-change-note-20260625.md`

## Proposed Change

Repeat the proven recorder rollout pattern for the sandbox account only:

1. create the AWS Config service-linked role `AWSServiceRoleForConfig` in the
   sandbox account;
2. create one customer managed recorder in `eu-west-2` using the service-linked
   role ARN;
3. use the same baseline regional posture used in the other in-scope accounts:
   all supported resource types, excluding global IAM resource types;
4. create one delivery channel in `eu-west-2` that points to the existing
   central Config archive bucket and Config KMS key in the security account;
5. start the sandbox recorder;
6. trigger one configuration snapshot delivery to validate the cross-account
   bucket and KMS path from the sandbox account;
7. do not re-include the sandbox account in the organization rule during this
   same change.

## Why This Boundary

This is the smallest correct next step after the sandbox exclusion decision was
reversed:

- it resolves the exact structural reason the first organization rule excluded
  the sandbox account;
- it keeps the current step narrow by proving recorder plumbing first rather
  than editing the organization rule and recorder state together;
- it aligns the sandbox account with its intended future use as a containers
  and microservices workload boundary;
- it preserves the ability to decide later whether the sandbox needs scoped
  exclusions for churn or cost.

## Trade-Offs

Accepted in this boundary:

- bring the sandbox into recorder scope now, because it is becoming a real
  workload boundary rather than a disposable lab;
- keep the recorder broad within `eu-west-2` rather than prematurely excluding
  resource types before any observed churn;
- extend the central Config archive bucket and KMS policies now, because the
  sandbox account must be able to deliver to the same central archive path as
  the other in-scope accounts;
- defer re-inclusion in the organization rule until after this recorder is
  proven.

Rejected for this boundary:

- leaving the sandbox permanently excluded, because that would weaken the
  governance model for the planned container and microservice work;
- re-including the sandbox in the organization rule before recorder rollout is
  validated, because that would blur blocker removal with recorder plumbing;
- broadening into GuardDuty or other rule families now, because that would
  widen cost and troubleshooting scope.

## Expected Blast Radius

- creates one AWS service-linked role in the sandbox account;
- creates one customer managed recorder and one delivery channel in
  `eu-west-2`;
- updates the central Config archive bucket policy and Config KMS key policy in
  the security account so sandbox delivery is authorized;
- starts AWS Config recording cost in the sandbox account;
- writes configuration history and snapshot objects into the existing central
  archive bucket;
- does not alter the current sandbox exclusion on the organization rule yet.

## Rollback Path

If this sandbox step needs to be rolled back before sandbox re-inclusion into
the organization rule:

1. stop the sandbox configuration recorder;
2. delete the delivery channel;
3. delete the customer managed recorder;
4. leave already-delivered S3 evidence in place unless there is an explicit
   later retention decision to remove it;
5. delete the service-linked role only if no AWS Config dependency still uses
   it.

Do not use this rollback after the organization rule is updated to include the
sandbox account. Reassess dependencies first.

## Validation

Success criteria after the step:

- the sandbox account has `AWSServiceRoleForConfig`;
- the sandbox account has one customer managed recorder in `eu-west-2`;
- the recorder status shows recording is enabled and reaches `SUCCESS`;
- the sandbox account has one delivery channel pointing at the security-account
  bucket and Config KMS key;
- snapshot delivery can be triggered successfully;
- the central bucket shows Config objects under `AWSLogs/974893866311/Config/`;
- the central Config bucket policy and Config KMS key policy explicitly include
  sandbox account `974893866311`;
- no new Config rules are created by the step.

Completed postchange validation evidence:

- `docs/evidence/domain1-governance-config-sandbox-service-linked-role-create-20260625.json`
- `docs/evidence/domain1-governance-config-sandbox-service-linked-role-postchange-20260625.json`
- `docs/evidence/domain1-governance-config-sandbox-recorder-applied-20260625.json`
- `docs/evidence/domain1-governance-config-sandbox-recorders-postchange-20260625.json`
- `docs/evidence/domain1-governance-config-sandbox-recorder-status-postchange-20260625.json`
- `docs/evidence/domain1-governance-config-sandbox-recorder-status-postverify-20260625.json`
- `docs/evidence/domain1-governance-config-sandbox-delivery-channel-failed-prepolicyfix-20260625.err`
- `docs/evidence/domain1-governance-config-log-archive-bucket-policy-pre-sandbox-fix-20260625.json`
- `docs/evidence/domain1-governance-config-log-archive-kms-key-policy-pre-sandbox-fix-20260625.json`
- `docs/evidence/domain1-governance-config-log-archive-bucket-policy-post-sandbox-fix-20260625.json`
- `docs/evidence/domain1-governance-config-log-archive-kms-key-policy-post-sandbox-fix-20260625.json`
- `docs/evidence/domain1-governance-config-sandbox-delivery-channel-applied-20260625.json`
- `docs/evidence/domain1-governance-config-sandbox-delivery-channels-postchange-20260625.json`
- `docs/evidence/domain1-governance-config-sandbox-delivery-channel-status-poststart-20260625.json`
- `docs/evidence/domain1-governance-config-sandbox-deliver-config-snapshot-20260625.json`
- `docs/evidence/domain1-governance-config-sandbox-delivery-channel-status-postsnapshot-20260625.json`
- `docs/evidence/domain1-governance-config-sandbox-bucket-objects-postchange-20260625.json`
- `docs/evidence/domain1-governance-config-sandbox-rules-postchange-20260625.json`
- `docs/evidence/domain1-governance-config-org-rule-post-sandbox-recorder-20260625.json`

## Follow-On Status

This recorder rollout's intended follow-on was completed later on 2026-06-25:
`org-multi-region-cloudtrail-enabled` was updated so sandbox account
`974893866311` is no longer excluded, and the final organization-rule
validation now shows successful deployment across management, lakehouse,
security, and sandbox accounts in
`docs/evidence/domain1-governance-config-org-cloudtrail-rule-change-note-20260625.md`.
