# Domain 1 AWS Config Delegated Administration Migration - 2026-07-06

<!-- markdownlint-disable MD013 -->

## Status

Completed.

AWS Config delegated administration and organization aggregation were migrated
from `Security Log Archive` account `955659429518` to `Security Tooling` account
`668848431187` in `eu-west-2`.

Final state:

- `Security Tooling` is the delegated administrator for
  `config.amazonaws.com`;
- `Security Tooling` is the delegated administrator for
  `config-multiaccountsetup.amazonaws.com`;
- AWS Config aggregator `organization-config-aggregator-eu-west-2` exists in
  `Security Tooling`;
- the aggregator points to
  `arn:aws:iam::668848431187:role/aws-config-organization-aggregator-role`;
- the aggregator source status is `SUCCEEDED`;
- organization Config rule `org-multi-region-cloudtrail-enabled` exists in
  `Security Tooling` and reports `UPDATE_SUCCESSFUL`;
- `Security Log Archive` no longer has the old Config aggregator, organization
  Config rule, or custom aggregator IAM role.

## Approval Boundary

User approval for this bounded live change:

> I explicitly approve migrating AWS Config delegated administration and
> aggregation from Security Log Archive account 955659429518 to Security Tooling
> account 668848431187 in eu-west-2. Do not enable GuardDuty, Security Hub, OAM,
> change SCPs, move accounts, or modify workload resources.

Explicitly out of scope and not performed:

- GuardDuty enablement or delegated-administrator setup;
- Security Hub enablement or delegated-administrator setup;
- OAM setup;
- SCP changes;
- account moves;
- workload resource changes;
- new AWS Config recorder or delivery-channel rollout.

## Prechange Evidence

Fresh read-only evidence showed the transitional state:

| Evidence | File |
|---|---|
| Management caller identity | `docs/evidence/domain1-governance-config-security-tooling-migration-management-sts-prechange-20260706.json` |
| Trusted service access | `docs/evidence/domain1-governance-config-security-tooling-migration-service-access-prechange-20260706.json` |
| `config.amazonaws.com` delegated administrator | `docs/evidence/domain1-governance-config-security-tooling-migration-delegated-admin-config-prechange-20260706.json` |
| `config-multiaccountsetup.amazonaws.com` delegated administrator | `docs/evidence/domain1-governance-config-security-tooling-migration-delegated-admin-multiaccountsetup-prechange-20260706.json` |
| Old delegated services | `docs/evidence/domain1-governance-config-security-tooling-migration-old-delegated-services-prechange-20260706.json` |
| New delegated-services precheck | `docs/evidence/domain1-governance-config-security-tooling-migration-new-delegated-services-prechange-20260706.status`; `docs/evidence/domain1-governance-config-security-tooling-migration-new-delegated-services-prechange-20260706.err` |
| Old aggregator | `docs/evidence/domain1-governance-config-security-tooling-migration-old-aggregators-prechange-20260706.json` |
| Old aggregator source status | `docs/evidence/domain1-governance-config-security-tooling-migration-old-aggregator-source-status-prechange-20260706.json` |
| Old organization Config rule | `docs/evidence/domain1-governance-config-security-tooling-migration-old-organization-config-rules-prechange-20260706.json` |
| Old organization Config rule detailed status | `docs/evidence/domain1-governance-config-security-tooling-migration-old-organization-config-rule-detailed-status-prechange-20260706.json` |
| New aggregator precheck | `docs/evidence/domain1-governance-config-security-tooling-migration-new-aggregators-prechange-20260706.json` |
| New organization Config rule precheck | `docs/evidence/domain1-governance-config-security-tooling-migration-new-organization-config-rules-prechange-20260706.json` |
| Parent and SCP boundary | `docs/evidence/domain1-governance-config-security-tooling-migration-old-parent-prechange-20260706.json`; `docs/evidence/domain1-governance-config-security-tooling-migration-new-parent-prechange-20260706.json`; `docs/evidence/domain1-governance-config-security-tooling-migration-security-ou-policies-prechange-20260706.json` |

Prechange summary:

- `Security Log Archive` (`955659429518`) was the delegated administrator for
  both AWS Config service principals;
- `Security Tooling` (`668848431187`) was not a delegated administrator;
- the old aggregator existed in `Security Log Archive` and pointed to
  `arn:aws:iam::955659429518:role/aws-config-organization-aggregator-role`;
- the target `Security Tooling` account had no aggregator role, no aggregator,
  and no organization Config rule;
- both `Security Log Archive` and `Security Tooling` remained in `Security OU`;
- Security OU SCP attachments were unchanged before the migration;
- the existing organization Config rule already showed
  `NoAvailableConfigurationRecorder` for `so-aws-admin` (`054394900225`) and
  `Security Tooling` (`668848431187`).

## Change Executed

The live migration was executed in this order.

1. In `Security Tooling`, create IAM role
   `aws-config-organization-aggregator-role` with trust to
   `config.amazonaws.com`.
2. Attach AWS managed policy
   `arn:aws:iam::aws:policy/service-role/AWSConfigRoleForOrganizations` to that
   role.
3. In `Security Log Archive`, delete organization Config rule
   `org-multi-region-cloudtrail-enabled`.
4. In `Security Log Archive`, delete Config aggregator
   `organization-config-aggregator-eu-west-2`.
5. Deregister `Security Log Archive` as delegated administrator for
   `config.amazonaws.com`.
6. Deregister `Security Log Archive` as delegated administrator for
   `config-multiaccountsetup.amazonaws.com`.
7. Register `Security Tooling` as delegated administrator for
   `config-multiaccountsetup.amazonaws.com`.
8. Register `Security Tooling` as delegated administrator for
   `config.amazonaws.com`.
9. In `Security Tooling`, create Config aggregator
   `organization-config-aggregator-eu-west-2` using the new role ARN and
   `eu-west-2` organization aggregation source.
10. In `Security Tooling`, recreate organization Config rule
    `org-multi-region-cloudtrail-enabled` with the same CloudTrail managed-rule
    definition.
11. Because the prechange detailed status already showed no recorder in
    `054394900225` and `668848431187`, update the migrated organization Config
    rule to exclude exactly those two accounts until their recorder scope is
    separately approved.
12. In `Security Log Archive`, detach
    `AWSConfigRoleForOrganizations` from the old custom aggregator role and
    delete that role.

Key execution evidence:

- `docs/evidence/domain1-governance-config-security-tooling-migration-new-aggregator-role-create-20260706.json`
- `docs/evidence/domain1-governance-config-security-tooling-migration-new-aggregator-role-policy-attach-20260706.status`
- `docs/evidence/domain1-governance-config-security-tooling-migration-old-organization-config-rule-delete-20260706.status`
- `docs/evidence/domain1-governance-config-security-tooling-migration-old-aggregator-delete-20260706.status`
- `docs/evidence/domain1-governance-config-security-tooling-migration-old-deregister-config-20260706.status`
- `docs/evidence/domain1-governance-config-security-tooling-migration-old-deregister-multiaccountsetup-20260706.status`
- `docs/evidence/domain1-governance-config-security-tooling-migration-new-register-multiaccountsetup-20260706.status`
- `docs/evidence/domain1-governance-config-security-tooling-migration-new-register-config-20260706.status`
- `docs/evidence/domain1-governance-config-security-tooling-migration-new-aggregator-put-20260706.json`
- `docs/evidence/domain1-governance-config-security-tooling-migration-new-organization-config-rule-put-20260706.json`
- `docs/evidence/domain1-governance-config-security-tooling-migration-new-organization-config-rule-put-exclusions-20260706.json`
- `docs/evidence/domain1-governance-config-security-tooling-migration-old-aggregator-role-policy-detach-20260706.status`
- `docs/evidence/domain1-governance-config-security-tooling-migration-old-aggregator-role-delete-20260706.status`

## Postchange Evidence

| Evidence | File |
|---|---|
| `config.amazonaws.com` delegated administrator | `docs/evidence/domain1-governance-config-security-tooling-migration-delegated-admin-config-postchange-20260706.json` |
| `config-multiaccountsetup.amazonaws.com` delegated administrator | `docs/evidence/domain1-governance-config-security-tooling-migration-delegated-admin-multiaccountsetup-postchange-20260706.json` |
| New delegated services | `docs/evidence/domain1-governance-config-security-tooling-migration-new-delegated-services-postchange-20260706.json` |
| Old delegated-services postcheck | `docs/evidence/domain1-governance-config-security-tooling-migration-old-delegated-services-postchange-20260706.status`; `docs/evidence/domain1-governance-config-security-tooling-migration-old-delegated-services-postchange-20260706.err` |
| New aggregator | `docs/evidence/domain1-governance-config-security-tooling-migration-new-aggregators-postchange-20260706.json` |
| New aggregator source status | `docs/evidence/domain1-governance-config-security-tooling-migration-new-aggregator-source-status-postchange-20260706.json` |
| New organization Config rule | `docs/evidence/domain1-governance-config-security-tooling-migration-new-organization-config-rules-postchange-20260706.json` |
| New organization Config rule status | `docs/evidence/domain1-governance-config-security-tooling-migration-new-organization-config-rule-statuses-postchange-20260706.json` |
| New organization Config rule detailed status | `docs/evidence/domain1-governance-config-security-tooling-migration-new-organization-config-rule-detailed-status-postchange-20260706.json` |
| Old aggregator and rule absence | `docs/evidence/domain1-governance-config-security-tooling-migration-old-aggregators-postchange-20260706.json`; `docs/evidence/domain1-governance-config-security-tooling-migration-old-organization-config-rules-postchange-20260706.json` |
| Old custom role absence | `docs/evidence/domain1-governance-config-security-tooling-migration-old-aggregator-role-postchange-20260706.status`; `docs/evidence/domain1-governance-config-security-tooling-migration-old-aggregator-role-postchange-20260706.err` |
| Parent and SCP boundary | `docs/evidence/domain1-governance-config-security-tooling-migration-old-parent-postchange-20260706.json`; `docs/evidence/domain1-governance-config-security-tooling-migration-new-parent-postchange-20260706.json`; `docs/evidence/domain1-governance-config-security-tooling-migration-security-ou-policies-postchange-20260706.json` |
| Recorder and delivery-channel boundary | `docs/evidence/domain1-governance-config-security-tooling-migration-old-recorders-postchange-20260706.json`; `docs/evidence/domain1-governance-config-security-tooling-migration-old-delivery-channels-postchange-20260706.json`; `docs/evidence/domain1-governance-config-security-tooling-migration-new-recorders-postchange-20260706.json`; `docs/evidence/domain1-governance-config-security-tooling-migration-new-delivery-channels-postchange-20260706.json` |
| GuardDuty/Security Hub no-admin checks | `docs/evidence/domain1-governance-config-security-tooling-migration-guardduty-admin-accounts-postchange-20260706.json`; `docs/evidence/domain1-governance-config-security-tooling-migration-securityhub-admin-accounts-postchange-20260706.json` |

Postchange validation:

- `Security Tooling` is returned as delegated administrator for both AWS Config
  service principals;
- `Security Log Archive` is no longer returned as delegated administrator for
  either AWS Config service principal;
- `Security Tooling` delegated services list includes
  `config.amazonaws.com` and `config-multiaccountsetup.amazonaws.com`;
- `Security Log Archive` delegated-services check returns
  `AccountNotRegisteredException`;
- the new aggregator exists in `Security Tooling` and points to the new role ARN;
- the new aggregator source status is `SUCCEEDED`;
- the organization Config rule exists in `Security Tooling` and reports
  `UPDATE_SUCCESSFUL`;
- detailed organization-rule status is `UPDATE_SUCCESSFUL` for the current
  recorder-bearing accounts:
  `349687196588`, `464975959576`, `955659429518`, and `974893866311`;
- organization-rule exclusions are exactly `054394900225` (`so-aws-admin`) and
  `668848431187` (`Security Tooling`) because both lacked recorders before this
  migration;
- `Security Log Archive` now has no Config aggregator and no organization Config
  rule;
- the old custom aggregator IAM role no longer exists in `Security Log Archive`;
- trusted service access did not change;
- parent placement for both accounts did not change;
- Security OU SCP attachments did not change;
- Config recorders and delivery channels in the old and new accounts did not
  change;
- GuardDuty and Security Hub organization-admin checks return empty admin-account
  lists.

## Follow-Up Required

Two accounts remain intentionally outside the migrated organization Config rule
until their recorder scope is separately approved:

| Account | Reason |
|---|---|
| `054394900225` / `so-aws-admin` | Pre-existing no-recorder state. |
| `668848431187` / `Security Tooling` | New delegated-admin account has no local recorder yet. |

Do not fix this by silently enabling recorders. Treat recorder onboarding for
those accounts as a separate AWS Config recorder-scope decision with its own
approval, evidence, cost posture, and rollback path.

## Rollback

Rollback would now be a migration back to the old account, not a simple undo.
Use only under separate explicit approval:

1. In `Security Tooling`, delete organization Config rule
   `org-multi-region-cloudtrail-enabled`.
2. In `Security Tooling`, delete aggregator
   `organization-config-aggregator-eu-west-2`.
3. Detach `AWSConfigRoleForOrganizations` from
   `aws-config-organization-aggregator-role` in `Security Tooling`, then delete
   the role.
4. Deregister `Security Tooling` for `config.amazonaws.com` and
   `config-multiaccountsetup.amazonaws.com`.
5. Register `Security Log Archive` for both AWS Config service principals.
6. Recreate the old custom aggregator role in `Security Log Archive`.
7. Recreate the old aggregator in `Security Log Archive`.
8. Recreate the organization Config rule from `Security Log Archive`, preserving
   any recorder-scope exclusions that are still required.

## SAP-C02 Relevance

This closes the next SAP-C02 Domain 1 governance migration step by moving AWS
Config delegated administration and organization aggregation into the dedicated
`Security Tooling` account. It reinforces the target separation between:

- management-account Organizations control-plane duties;
- storage-only `Security Log Archive` duties;
- active delegated security operations in `Security Tooling`.

GuardDuty remains the next security-service delegated-admin candidate if
adopted. Security Hub and OAM remain later, separately approved decisions.
