# Domain 1 Evidence - SecurityToolingAdmin Staged Assignment - 2026-07-12

<!-- markdownlint-disable MD013 -->

## Status

Stage 1 completed under the user's instruction to proceed to the next accepted
migration step.

The one-hour `SecurityToolingAdmin` permission set is live with only the
validated custom inline policy. The existing `security-tooling-admins` group is
assigned to it in `Security Tooling` account `668848431187` only.

At the stage-1 point, the temporary broad `AdministratorAccess` assignment
remained in place. It was later removed under separate explicit approval after
the custom role passed representative-write and audit validation; see
`docs/evidence/domain1-governance-identity-center-security-tooling-admin-broad-assignment-removal-change-note-20260712.md`.

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

## Completed Follow-On Gate

The follow-on gate was completed under separate explicit approval:

1. the approved idempotent GuardDuty write completed with unchanged
   postconditions;
2. delayed Event History and the organization-trail object confirmed that
   custom-role action; and
3. only the broad Security Tooling assignment was deleted, with the custom and
   audit paths validated afterward.

## SAP-C02 Relevance

This supports Domain 1 by demonstrating a staged move from broad federated
administration to a task-specific permission set, with group assignment,
temporary credentials, policy validation, live entitlement proof, separation
of duties, audit evidence, and rollback before privilege reduction.
