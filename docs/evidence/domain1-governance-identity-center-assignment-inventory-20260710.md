# Domain 1 Governance Evidence - IAM Identity Center Assignment Inventory - 2026-07-10

<!-- markdownlint-disable MD013 -->

## Status

Fresh read-only inventory captured from the active management-account SSO
session. No IAM Identity Center users, groups, permission sets, assignments,
IAM roles, or AWS accounts were created, modified, assigned, or removed.

## Scope

The inventory covered the active management, lakehouse workload, Security Log
Archive, Security Tooling, and sandbox accounts. The closed `so-aws-admin`
account was excluded from the assignment queries.

## Identity Center Baseline

- One management-owned IAM Identity Center instance is `ACTIVE`.
- The identity store has two enabled users: the normal administrator and the
  dedicated emergency administrator.
- The identity store has two groups: `cloud-lab-aws-admins` and
  `sandbox-cloud-admins`.
- The only current permission sets are `AdministratorAccess` and
  `BreakGlassAdmin`; each has a one-hour session duration and the AWS-managed
  `AdministratorAccess` policy attached. Neither has an inline policy,
  customer-managed policy reference, or permissions boundary.
- The dedicated emergency administrator has no group memberships.
- The normal administrator belongs to both existing administrator groups.

## Assignment Baseline

| Account scope | `AdministratorAccess` | `BreakGlassAdmin` |
|---|---|---|
| Management | Direct normal-administrator user assignment; `cloud-lab-aws-admins` group assignment; `sandbox-cloud-admins` group assignment | Direct dedicated-emergency-administrator user assignment |
| Sandbox | `sandbox-cloud-admins` group assignment | None |
| Lakehouse workload | None | None |
| Security Log Archive | None | None |
| Security Tooling | None | None |

This matches the assignment identifiers recorded in the 2026-06-25 sanitized
inventory. The existing direct and group-based management administrator paths
are therefore documented baseline state, not a new assignment or an approved
target model.

## Decision Impact

The 2026-07-10 documentation-only assignment decision remains valid:

- the direct, management-only `BreakGlassAdmin` path is intact and remains
  separate from normal group membership;
- no routine permission set is provisioned for the lakehouse or security
  accounts; and
- the future first routine assignment remains `SecurityAudit`, subject to the
  separate named-principal, account-scope, policy-validation, rollback, and
  explicit-approval gates.

This inventory satisfies the decision's baseline-inventory prerequisite only.
It does not authorize consolidation of the existing normal administrator paths
or any live assignment change. Recapture the inventory immediately before an
approved IAM Identity Center change.

## SAP-C02 Relevance

This supports Domain 1 by evidencing the separation of routine and emergency
human access, validating current cross-account assignment boundaries, and
preserving a pre-change baseline for least-privilege rollout and rollback.
