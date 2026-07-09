# Domain 1 so-aws-admin BreakGlassAdminRole Decision - 2026-07-09

<!-- markdownlint-disable MD013 -->

## Status

Decision recorded from read-only evidence. No IAM role, trust policy, permission
policy, Identity Center assignment, SCP, service, budget, CloudTrail Lake, or
account-closure change was performed by this note.

## Decision

Retain `BreakGlassAdminRole` in `so-aws-admin` (`054394900225`) only as a
temporary account-recovery path while the account remains on the decommission
path.

Do not use this role as a durable governance, tooling, audit, or operating
access path. The accepted long-term governance model remains:

- active security tooling in `Security Tooling` (`668848431187`);
- durable log/archive storage in `Security Log Archive` (`955659429518`);
- normal human/admin access through IAM Identity Center in the management
  account, with documented break-glass procedure evidence.

If `so-aws-admin` proceeds to account closure, this role can be handled as part
of the final account-closure package rather than as a standalone pre-closure
delete. If the account is retained instead of closed, this role should be
removed or redesigned under a separate explicit approval because it grants full
administrator authority.

## Read-Only Evidence

Evidence was captured using a read-only inspection flow. Temporary role
credentials were used only in-process and were not written to evidence files.
The inspection created normal CloudTrail STS/IAM read events, but no resource
configuration was changed.

Evidence files:

- `docs/evidence/domain1-governance-so-aws-admin-breakglass-role-assume-check-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-breakglass-role-assume-check-20260709.status`
- `docs/evidence/domain1-governance-so-aws-admin-breakglass-role-target-sts-sanitized-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-breakglass-role-get-role-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-breakglass-role-attached-policies-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-breakglass-role-attached-policy-versions-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-breakglass-role-inline-policy-names-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-breakglass-role-inline-policies-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-breakglass-role-instance-profiles-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-breakglass-role-inventory-summary-20260709.json`

Findings:

- `sts:AssumeRole` into `BreakGlassAdminRole` succeeded from the management-side
  administrator session.
- The role trust policy allows `arn:aws:iam::349687196588:root` to assume the
  role.
- The role has AWS managed `AdministratorAccess` attached.
- The role has no inline role policies.
- The role is not attached to any instance profiles.
- The prior direct inventory found no IAM users, groups, local
  customer-managed policies, or instance profiles in the account.

## Retirement Impact

This resolves the role-specific decision blocker, but it does not by itself
make `so-aws-admin` ready for retirement.

Follow-on read-only closure-blocker evidence is recorded in
`docs/evidence/domain1-governance-so-aws-admin-closure-blockers-20260709.md`.
That follow-on evidence clears CloudTrail Lake event-data-store evidence, clears
public-safe budget metadata, and records root plus service-linked-role posture
as closure-procedural rather than standalone dependency blockers.

Remaining boundaries before closure:

- preserve any required evidence outside the account;
- document closure blast radius and rollback limitations;
- optionally complete a private billing/contact console check;
- obtain separate explicit approval for any account closure or IAM deletion.

## SAP-C02 Relevance

This supports Domain 1 by making the emergency-access boundary explicit and by
separating durable security-tooling ownership from a decommission-path account.
It supports Domain 3 by reducing ambiguity before account retirement and by
keeping destructive changes gated on evidence and separate approval.
