# Domain 1 Governance Evidence - IAM Identity Center Current State - 2026-06-25

<!-- markdownlint-disable MD013 -->

## Status Note

This is same-day evidence captured on 2026-06-25 to clarify the IAM Identity
Center state for the governance track.

It combines earlier read-only CLI inventory, later console follow-up evidence,
and the bounded live permission-set change completed from the active
management-account session.

It does not create or modify users, groups, permission sets, or assignments.

## Current Live State

- one IAM Identity Center instance exists and is `ACTIVE`:
  `management-identity-center`;
- the instance is owned by management account `349687196588`;
- earlier same-day read-only CLI evidence showed one enabled live admin
  principal in the identity store:
  `org-admin-principal` /
  `[redacted-email]`;
- later same-day console evidence now shows a second enabled user:
  `breakglass-principal` /
  `[redacted-email]`;
- that dedicated break-glass user shows a verified email address and one MFA
  device enrolled;
- same-day evidence now shows two visible permission sets:
  `AdministratorAccess` and `BreakGlassAdmin`;
- `AdministratorAccess` currently has at least one direct user assignment in
  the management account for `org-admin-principal`;
- `AdministratorAccess` is also provisioned to sandbox account
  `974893866311` through group assignment;
- `BreakGlassAdmin` is configured with a `PT1H` session duration;
- `BreakGlassAdmin` currently has the AWS-managed
  `AdministratorAccess` policy attached as the first staged implementation;
- `BreakGlassAdmin` is currently assigned only to management account
  `349687196588` for user `breakglass-principal`.

## Break-Glass Clarification

The governance documents previously recorded
`[redacted-email]` as the emergency owner target identity.

The current evidence now clarifies that:

- the dedicated target emergency owner identity now exists as live IAM Identity
  Center user `breakglass-principal` /
  `[redacted-email]`;
- that dedicated break-glass user already has one MFA device enrolled;
- a dedicated `BreakGlassAdmin` permission set now exists and is assigned to
  the management-account recovery path for that user;
- the currently live management-account admin principal is
  `org-admin-principal` /
  `[redacted-email]`;
- the repo should still distinguish the target break-glass identity from the
  currently live management-account admin principal, because they now represent
  separate normal and emergency access paths.

## Recovery-Path Clarification

For the current OU-targeted root-user emergency-only SCP planning:

- the target SCP would affect the workload member account under
  `Lakehouse Workloads OU`, not the management account;
- however, Organizations attach/detach recovery still depends on a
  management-account-capable path unless explicit delegated Organizations
  policy management is later implemented.

## Follow-on Assignment Cleanup - 2026-07-02

Later live read-only verification showed that `breakglass-principal` also
inherited management-account `AdministratorAccess` through
`cloud-lab-aws-admins`.

Under explicit approval, that group membership was removed. Postchange
verification shows:

- `breakglass-principal` has no group memberships;
- `cloud-lab-aws-admins` still contains the normal
  `org-admin-principal` principal;
- `BreakGlassAdmin` remains directly assigned to `breakglass-principal` for
  management account `349687196588`;
- `AdministratorAccess` remains available for the normal management-account
  admin path, but is no longer inherited by `breakglass-principal`.

Detailed evidence is recorded in
`docs/evidence/domain1-governance-breakglass-group-membership-cleanup-20260702.md`.

## Evidence Files

- `docs/evidence/domain1-governance-identity-center-instances-status-20260625.json`
- `docs/evidence/domain1-governance-identity-center-users-status-20260625.json`
- `docs/evidence/domain1-governance-identity-center-permission-sets-status-20260625.json`
- `docs/evidence/domain1-governance-identity-center-admin-permission-set-status-20260625.json`
- `docs/evidence/domain1-governance-identity-center-admin-assignments-management-20260625.json`
- `docs/evidence/domain1-governance-identity-center-admin-assignments-sandbox-20260625.json`
- `docs/evidence/domain1-governance-breakglass-permission-set-create-20260625.json`
- `docs/evidence/domain1-governance-breakglass-permission-set-status-20260625.json`
- `docs/evidence/domain1-governance-breakglass-permission-set-managed-policies-20260625.json`
- `docs/evidence/domain1-governance-breakglass-permission-set-assignment-create-20260625.json`
- `docs/evidence/domain1-governance-breakglass-permission-set-assignment-status-20260625.json`
- `docs/evidence/domain1-governance-breakglass-permission-set-assignment-management-20260625.json`

The files above now capture both the earlier same-day baseline state and the
later same-day bounded `BreakGlassAdmin` implementation evidence.
