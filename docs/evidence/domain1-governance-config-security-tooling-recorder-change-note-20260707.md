# Domain 1 Governance Change Note - Security Tooling AWS Config Recorder - 2026-07-07

<!-- markdownlint-disable MD013 -->

## Status

Completed.

On 2026-07-07, the `Security Tooling` account (`668848431187`) was onboarded
to the AWS Config recorder baseline in `eu-west-2`, and only that account was
removed from the migrated organization CloudTrail Config rule exclusions.

Final state:

- `Security Tooling` has `AWSServiceRoleForConfig`;
- customer-managed AWS Config recorder `default` exists in `eu-west-2`;
- recorder `default` is running with `recording=true` and
  `lastStatus=SUCCESS`;
- delivery channel `default` points to the existing central AWS Config archive
  bucket and KMS key owned by `Security Log Archive` (`955659429518`);
- the central AWS Config archive bucket policy and KMS key policy now include
  the Security Tooling source-account permissions required for delivery;
- a manual configuration snapshot delivery wrote objects under
  `AWSLogs/668848431187/Config/`;
- organization Config rule `org-multi-region-cloudtrail-enabled` now excludes
  only `so-aws-admin` (`054394900225`);
- Security Tooling is included in that organization rule and reports
  `UPDATE_SUCCESSFUL`.

Public evidence note: raw private bucket names, local principal names, and
account emails are not recorded in this public repository. Evidence files use
the repository's sanitized public naming pattern while live AWS API calls used
the real AWS resource names.

## Approval Boundary

User approval for this bounded live change:

> Onboard the Security Tooling AWS Config recorder in eu-west-2, then remove
> only that account from the migrated Config rule exclusions.

Explicitly out of scope and not performed:

- enabling an AWS Config recorder in `so-aws-admin`;
- enabling GuardDuty;
- enabling Security Hub;
- configuring OAM;
- changing SCPs;
- moving, renaming, closing, or retiring accounts;
- modifying workload resources;
- broadening AWS Config Region scope beyond `eu-west-2`;
- adding new AWS Config rules.

## Prechange Evidence

Fresh prechange evidence showed the expected state:

| Evidence | File |
|---|---|
| Management caller identity | `docs/evidence/domain1-governance-config-security-tooling-recorder-management-sts-prechange-20260707.json` |
| AWS Config delegated administrators | `docs/evidence/domain1-governance-config-security-tooling-recorder-delegated-admin-config-prechange-20260707.json`; `docs/evidence/domain1-governance-config-security-tooling-recorder-delegated-admin-multiaccountsetup-prechange-20260707.json` |
| Security Tooling caller identity | `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-sts-prechange-20260707.json` |
| Security Tooling Config recorder baseline | `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-recorders-prechange-20260707.json`; `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-recorder-status-prechange-20260707.json`; `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-delivery-channels-prechange-20260707.json`; `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-delivery-channel-status-prechange-20260707.json` |
| Existing aggregator and organization rule | `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-aggregators-prechange-20260707.json`; `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-aggregator-source-status-prechange-20260707.json`; `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-org-rule-prechange-20260707.json`; `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-org-rule-detailed-status-prechange-20260707.json` |
| Archive bucket and KMS policy readiness | `docs/evidence/domain1-governance-config-security-tooling-recorder-config-archive-bucket-policy-prechange-20260707.json`; `docs/evidence/domain1-governance-config-security-tooling-recorder-config-archive-kms-key-policy-prechange-20260707.json`; `docs/evidence/domain1-governance-config-security-tooling-recorder-config-archive-policy-security-tooling-prechange-20260707.status` |
| Security Tooling archive prefix | `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-bucket-objects-prechange-20260707.json` |

Prechange summary:

- `Security Tooling` was already the AWS Config delegated administrator and
  organization aggregator account.
- `Security Tooling` had no local Config recorder or delivery channel.
- The central Config archive bucket and KMS key did not yet include the
  Security Tooling source-account permissions.
- The migrated organization CloudTrail Config rule excluded exactly
  `054394900225` and `668848431187`.

## Change Executed

The live change was executed in this order.

1. Extended the central AWS Config archive bucket policy only for account
   `668848431187`.
2. Extended the central AWS Config archive KMS key policy only for account
   `668848431187`.
3. Created `AWSServiceRoleForConfig` in `Security Tooling`.
4. Created customer-managed Config recorder `default` in `eu-west-2` using the
   Config service-linked role.
5. Created delivery channel `default` in `eu-west-2` pointing at the existing
   central Config archive bucket and KMS key.
6. Started recorder `default`.
7. Triggered one manual configuration snapshot delivery.
8. Updated organization Config rule `org-multi-region-cloudtrail-enabled` to
   exclude only `054394900225`, thereby including `668848431187`.

Key execution evidence:

- `docs/evidence/domain1-governance-config-security-tooling-recorder-config-archive-bucket-policy-apply-20260707.status`
- `docs/evidence/domain1-governance-config-security-tooling-recorder-config-archive-kms-key-policy-reapply-20260707.status`
- `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-service-linked-role-create-20260707.status`
- `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-recorder-apply-20260707.status`
- `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-delivery-channel-apply-20260707.status`
- `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-recorder-start-20260707.status`
- `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-deliver-config-snapshot-20260707.json`
- `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-org-rule-put-security-tooling-included-20260707.json`

## Postchange Evidence

| Evidence | File |
|---|---|
| Archive policy postchange verification | `docs/evidence/domain1-governance-config-security-tooling-recorder-config-archive-bucket-policy-postchange-20260707.json`; `docs/evidence/domain1-governance-config-security-tooling-recorder-config-archive-kms-key-policy-postchange-20260707.json`; `docs/evidence/domain1-governance-config-security-tooling-recorder-config-archive-policy-security-tooling-postchange-20260707.status` |
| Service-linked role | `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-service-linked-role-postcreate-20260707.json` |
| Recorder and delivery channel | `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-recorders-poststart-20260707.json`; `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-recorder-status-postverify-20260707.json`; `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-delivery-channels-poststart-20260707.json`; `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-delivery-channel-status-postsnapshot-20260707.json` |
| Snapshot delivery | `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-bucket-objects-postsnapshot-20260707.json` |
| Aggregator and organization rule | `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-aggregators-postverify-20260707.json`; `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-aggregator-source-status-postverify-20260707.json`; `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-org-rule-postverify-20260707.json`; `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-org-rule-statuses-postverify-20260707.json`; `docs/evidence/domain1-governance-config-security-tooling-recorder-security-tooling-org-rule-detailed-status-postverify-20260707.json` |

Postchange validation:

- bucket policy contains the Security Tooling source-account permissions;
- KMS key policy contains the Security Tooling source-account permissions;
- recorder `default` is recording and reports `lastStatus=SUCCESS`;
- snapshot evidence exists in the central archive under the Security Tooling
  account prefix;
- organization rule exclusions are exactly `054394900225`;
- Security Tooling member-account rule status is `UPDATE_SUCCESSFUL`;
- the existing organization aggregator remains present in Security Tooling.

## Rollback

Rollback would be a recorder-scope rollback and must be separately approved:

1. Re-add `668848431187` to the organization Config rule exclusions.
2. Stop the Security Tooling recorder.
3. Delete the Security Tooling delivery channel.
4. Delete the Security Tooling recorder.
5. Remove only the Security Tooling statements from the central Config archive
   bucket policy and KMS key policy if no longer needed.
6. Leave delivered S3 evidence in place unless a separate retention decision is
   approved.
7. Delete `AWSServiceRoleForConfig` only after confirming no Config dependency
   still requires it.

Do not use this rollback after GuardDuty, Security Hub, OAM, additional Config
rules, or other recorder-dependent controls are enabled without reassessing
dependencies.

## Follow-Up Required

- Keep `so-aws-admin` (`054394900225`) excluded while it remains on the
  decommission path.
- Collect read-only dependency evidence before any `so-aws-admin` retirement
  action.
- The next security-service transition can move to GuardDuty live-readiness
  planning and prechange evidence under separate explicit approval.

## SAP-C02 Relevance

This closes the AWS Config recorder-scope gap for the delegated security
tooling account. It supports SAP-C02 Domain 1 by demonstrating the correct
multi-account sequence:

- separate archive storage from active security tooling;
- migrate delegated administration before dependent service rollouts;
- enable recorder coverage before including an account in an organization
  Config rule;
- keep decommission-path accounts excluded instead of expanding cost and
  technical debt.
