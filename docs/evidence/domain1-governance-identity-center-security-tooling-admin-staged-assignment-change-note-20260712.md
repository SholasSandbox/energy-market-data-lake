# Domain 1 Evidence - SecurityToolingAdmin Staged Assignment - 2026-07-12

<!-- markdownlint-disable MD013 -->

## Status

Stage 1 completed under the user's instruction to proceed to the next accepted
migration step.

The one-hour `SecurityToolingAdmin` permission set is live with only the
validated custom inline policy. The existing `security-tooling-admins` group is
assigned to it in `Security Tooling` account `668848431187` only.

The temporary broad `AdministratorAccess` assignment remains in place. No
Config, GuardDuty, IAM role, Organizations, SCP, Security Hub, OAM, archive
storage, account, or workload resource was changed.

## Immediate Precheck

Immediately before execution:

- the management-account SSO session and Identity Center instance were active;
- `security-tooling-admins` existed with exactly the normal Workforce Identity;
- `SecurityToolingAdmin` did not exist;
- Security Tooling had the expected `AdministratorAccess` and `SecurityAudit`
  group assignments and no `BreakGlassAdmin` assignment;
- Config still had the expected aggregator, recorder, delivery channel, and
  organization rule in `eu-west-2`;
- GuardDuty still had one detector, three approved members, and organization
  auto-enable set to `NONE`; and
- IAM Access Analyzer returned zero findings for the prepared policy.

## Change Executed

1. Created `SecurityToolingAdmin` with `PT1H` and the approved description.
2. Applied only
   `docs/policies/iam-identity-center-security-tooling-admin.inline-policy.example.json`.
3. Confirmed the live inline policy length matched the validated 6,552-byte
   policy and that no AWS-managed or customer-managed policy was attached.
4. Assigned the existing `security-tooling-admins` group to the permission set
   in Security Tooling only. The request reached `SUCCEEDED` at 11:32 BST.

## Validation

- The AWS access portal exposes `SecurityToolingAdmin`, `SecurityAudit`, and
  `AdministratorAccess` for Security Tooling.
- A short-lived `AWSReservedSSO_SecurityToolingAdmin_*` session assumed into
  account `668848431187` successfully.
- The session listed the expected Config aggregator, recorder, delivery
  channel, organization rule, and five organization-rule status records.
- The session listed one GuardDuty detector and three members and confirmed
  organization auto-enable remains `NONE`.
- The session read the named Config aggregator IAM role, one CloudTrail event,
  and organization account context.
- Postchange assignments show the same admin group on both the custom and
  temporary broad paths, the auditor group unchanged, and no Security Tooling
  break-glass assignment.
- CloudTrail event history records `CreatePermissionSet`,
  `PutInlinePolicyToPermissionSet`, and `CreateAccountAssignment` at the
  execution timestamps.

No write-capable Config or GuardDuty action was invoked during validation.

## Rollback

Delete only the Security Tooling `SecurityToolingAdmin` account assignment and
wait for asynchronous deletion to succeed. Confirm the custom role disappears
from the portal. Leave the temporary broad, auditor, and break-glass paths
unchanged. Permission-set deletion is a separate cleanup decision.

## Remaining Gate

Before removing broad `AdministratorAccess`:

1. approve one reversible representative Config or GuardDuty write test;
2. verify the result and CloudTrail event through `SecurityToolingAdmin`;
3. confirm the documented dependent-action caveat does not require widening
   the routine policy; and
4. separately approve deletion of only the broad Security Tooling account
   assignment.

## SAP-C02 Relevance

This supports Domain 1 by demonstrating a staged move from broad federated
administration to a task-specific permission set, with group assignment,
temporary credentials, policy validation, live entitlement proof, separation
of duties, audit evidence, and rollback before privilege reduction.
