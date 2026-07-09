# Domain 1 so-aws-admin Direct Inventory Live Evidence - 2026-07-09

<!-- markdownlint-disable MD013 -->

## Status

Approved live Identity Center change completed, direct in-account read-only
inventory captured, and the target-account assignment removed.

No account closure, SCP change, service enablement or disablement, IAM user
creation, long-lived credential creation, Terraform apply, administrator access,
billing-policy expansion, or change to another AWS account was performed.

## Approval Scope Used

The approved live-change scope was:

- Identity Center instance:
  `arn:aws:sso:::instance/ssoins-7535b4aeae7b57d7`;
- target account: `so-aws-admin` / `054394900225`;
- permission set: `SoAwsAdminReadOnlyInventory`;
- session duration: `PT1H`;
- AWS managed policies: `SecurityAudit` and `ViewOnlyAccess`;
- user principal: `b6b262e4-0041-70a7-df00-66e62c9af94a`;
- create only the target-account assignment, collect read-only inventory, remove
  the assignment, and verify removal.

## Live Change Performed

- Created the `SoAwsAdminReadOnlyInventory` permission set.
- Attached only AWS managed `SecurityAudit` and AWS managed
  `job-function/ViewOnlyAccess`.
- Assigned the permission set only to user
  `b6b262e4-0041-70a7-df00-66e62c9af94a` for account `054394900225`.
- Obtained temporary role credentials for the inventory run without writing
  credential values to evidence.
- Deleted the account assignment after evidence capture.
- Verified the target account has zero remaining account assignments for the
  permission set and zero provisioned permission sets.
- Verified a fresh post-delete role-credential request fails with
  `ForbiddenException`.

The permission set object was initially left in IAM Identity Center because the
approved rollback scope required assignment removal and verification, not
permission-set deletion. Under separate explicit approval, it was later deleted:
`docs/evidence/domain1-governance-so-aws-admin-permission-set-cleanup-20260709.md`.

## Direct Inventory Findings

The temporary read-only session reached `054394900225` successfully.

Cleared by direct inventory:

- IAM users: `0`.
- IAM groups: `0`.
- IAM local customer-managed policies: `0`.
- IAM instance profiles: `0`.
- IAM user access keys: no users existed, so no user access keys were present.
- AWS Config recorders, delivery channels, rules, conformance packs, and
  account-local aggregators: `0`.
- GuardDuty detectors in `eu-west-2`: `0`.
- Security Hub: not subscribed in `eu-west-2`.
- OAM sinks in `eu-west-2`: `0`.
- CloudWatch alarms and log groups in `eu-west-2`: `0`.
- EventBridge default-bus rules in `eu-west-2`: `0`; only the default event bus
  exists.
- SNS topics and subscriptions in `eu-west-2`: `0`.
- S3 buckets owned by the account: `0`.
- KMS keys and aliases in `eu-west-2`: `0`.
- Route 53 hosted zones: `0`; hosted-zone names were not stored.
- AWS Backup vaults in `eu-west-2`: `0`.
- Alternate `BILLING`, `OPERATIONS`, and `SECURITY` contacts: not present;
  contact values were not stored.

Observed but not retirement-cleared:

- IAM roles: `9` were visible during the inventory run.
- One role was the temporary `AWSReservedSSO_SoAwsAdminReadOnlyInventory...`
  role created by the approved assignment.
- Existing non-temporary roles include AWS service-linked roles and
  `BreakGlassAdminRole`.
- IAM account summary reports account password presence and account MFA enabled.
  Treat this as expected root/break-glass posture, but it still belongs in the
  closure checklist.
- The organization CloudTrail trail is visible from the account, but its ARN is
  owned by the management account and writes to the Security Log Archive bucket.
  It is not an account-local trail dependency.

Not cleared by the approved policy set:

- `cloudtrail:ListEventDataStores` returned `AccessDeniedException`, so
  CloudTrail Lake event data stores are not proven absent.
- `budgets:ViewBudget` returned `AccessDeniedException`, so account-local
  budget metadata is not proven absent.
- Primary contact presence could not be recaptured under the approved
  permission set; no contact values were stored. The earlier management-account
  presence-only check remains the public-safe contact evidence.

## Retirement Readiness Decision

Closer, but still not ready for retirement.

The direct inventory substantially reduces the unknown service-dependency
surface. Before any closure package is drafted, the remaining items are:

- decide the fate of `BreakGlassAdminRole`;
- account for service-linked roles and root/break-glass posture in the closure
  checklist;
- clear or explicitly waive CloudTrail Lake event-data-store evidence;
- clear or privately review budget/cost metadata;
- record a separate explicit account-closure approval if closure is ever pursued.

## Evidence Files

Identity Center live change and rollback:

- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-create-permission-set-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-permission-set-managed-policies-after-attach-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-create-account-assignment-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-create-account-assignment-poll-20260709.jsonl`
- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-delete-account-assignment-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-delete-account-assignment-poll-20260709.jsonl`
- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-target-account-assignments-after-delete-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-target-provisioned-permission-sets-after-delete-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-post-delete-role-credentials-check-20260709.jsonl`

Inventory summary:

- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-target-sts-sanitized-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-inventory-count-summary-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-inventory-run-summary-20260709.json`

Detailed inventory evidence:

- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-iam-account-summary-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-iam-users-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-iam-roles-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-config-recorders-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-guardduty-detectors-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-securityhub-hub-20260709.status`
- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-securityhub-hub-20260709.err`
- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-cloudtrail-describe-trails-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-s3-buckets-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-route53-hosted-zone-summary-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-budgets-20260709.status`
- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-budgets-20260709.err`

## SAP-C02 Relevance

This supports Domain 1 by using an explicitly approved, least-privilege,
time-bound Identity Center path to inspect a decommission-path account. It
supports Domain 3 by proving rollback and keeping account closure gated on
dependency evidence, evidence preservation, and a separate destructive-action
approval.
