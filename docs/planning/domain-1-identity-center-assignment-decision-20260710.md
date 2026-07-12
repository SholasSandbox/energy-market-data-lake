# Domain 1 IAM Identity Center Assignment Decision - 2026-07-10

## Status

Accepted as a documentation-only decision. It does not create, modify, assign,
or remove IAM Identity Center users, groups, permission sets, assignments, IAM
roles, or AWS accounts.

## Scope And Evidence Boundary

This decision applies to routine and emergency human access for the management,
lakehouse workload, Security Tooling, Security Log Archive, and sandbox
accounts. It uses the recorded IAM Identity Center state from
`docs/evidence/domain1-governance-identity-center-current-state-20260625.md`
and the 2026-07-02 break-glass cleanup evidence. A fresh read-only baseline is
now recorded in
`docs/evidence/domain1-governance-identity-center-assignment-inventory-20260710.md`.

A fresh read-only Identity Center inventory is mandatory before any assignment
change. Do not use this document as authorization for a live change.

## Decisions

| Access path | Decision | Reason | Revisit condition |
|---|---|---|---|
| Normal management administration | Retain the existing direct and group-based `AdministratorAccess` paths for `org-admin-principal`; do not create or assign `OrganizationAdmin`, or consolidate the existing normal paths, in this slice. | The fresh baseline confirms direct normal-administrator, `cloud-lab-aws-admins`, and `sandbox-cloud-admins` assignments in the management account. They preserve the known Organizations recovery path while their intended least-privilege replacement is not yet proven. | A reviewed custom `OrganizationAdmin` policy, named owner, group-scope decision, rollback procedure, and explicit change approval exist. |
| Emergency administration | Retain the direct, management-account-only `BreakGlassAdmin` assignment for `breakglass-principal`; do not create an emergency group, add principals, broaden account scope, or alter the staged policy. | The direct assignment, MFA, notification, recovery-code, and post-use reduction paths are already evidenced; changing them now would add risk without closing a current tracker gap. | The exact recovery actions are proven and a single-purpose emergency-group owner and rollback plan are approved. |
| `SecurityAudit` | The first routine permission set is live for the normal Workforce Identity through the dedicated `security-tooling-auditors` group in Security Tooling only. It has a one-hour session and the approved custom no-mutation inline policy. | This validates the assignment lifecycle, portal entitlement, read-only delegated-security review, evidence collection, and rollback without broadening routine access. | Any additional principal, account, policy action, session change, or group-scope change requires a new precheck, change note, validation, rollback plan, and separate approval. |
| `SecurityToolingAdmin` | The one-hour, Security Tooling-only custom permission set is live for the existing `security-tooling-admins` group alongside temporary broad `AdministratorAccess`. | This stages the least-privilege replacement while preserving management, archive, emergency, read-only, and rollback boundaries. Portal assumption, read-only checks, and an idempotent GuardDuty write with unchanged postconditions are proven. | Capture the write audit evidence, then approve and pass the final rollback/removal procedure before the broad Security Tooling assignment is separately removed. |
| `BillingAdmin` | Keep as a design-only management-account permission set. | Account-level budget thresholds and notification ownership remain open; assigning billing authority before those decisions would create a broad role without an operating boundary. | Budget threshold, alert recipient, policy scope, and rollback decisions are documented and approved. |
| `LakehouseOperator` and `LakehouseReadOnly` | Keep as design-only workload-account permission sets. | The current record does not identify a distinct routine operator or portfolio-reviewer lifecycle, and the workload resource boundary must be validated before access is granted. | Named persona, workload resource scope, policy validation, test case, rollback, and explicit approval are recorded. |

## First Live Assignment Gate

Before any future assignment, recapture a fresh read-only inventory of the IAM
Identity Center instance, permission-set names and policy attachments, and
assignment counts for each in-scope account. The 2026-07-10 baseline does not
substitute for the immediately pre-change check. The control-specific change
note must then include:

- named permission set, principal or group, target account, and rationale;
- exact policy scope and validation of the intended permissions;
- prechange assignment count and rollback action;
- expected audit, notification, and cost impact;
- public-evidence redaction validation; and
- separate explicit approval for that exact assignment.

The first assignment completed under those gates on 2026-07-11 in Security
Tooling only. Its evidence is recorded in
`docs/evidence/domain1-governance-identity-center-security-audit-assignment-change-note-20260711.md`.

Execute one permission set in one account per approved change. Do not combine a
routine assignment with BreakGlassAdmin hardening, an SCP change, or account
movement.

## Deferred Work

This decision intentionally leaves the following open:

- custom least-privilege policy documents for `OrganizationAdmin` and
  `BreakGlassAdmin`;
- missing audit evidence for the successful representative write and stage-2
  removal of the Security Tooling `AdministratorAccess` assignment after the
  live `SecurityToolingAdmin` path is fully proven;
- account-level budget thresholds and BillingAdmin notification ownership;
- routine reviewer/operator personas for the lakehouse workload account; and
- Security Hub and OAM adoption.

## SAP-C02 Relevance

This supports Domain 1 by separating normal and emergency access, using
least-privilege readiness gates, and requiring rollout/rollback evidence before
human-access changes cross account boundaries.
