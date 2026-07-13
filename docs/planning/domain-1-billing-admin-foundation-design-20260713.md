# Domain 1 BillingAdmin Foundation Design - 2026-07-13

## Status

Documentation-only IAM-foundation slice. This note begins the tracker week of
2026-07-13 without creating a permission set, changing an assignment, reading
live AWS state, or changing a budget, notification, tag, or account.

It narrows the next design task to the management-account `BillingAdmin`
persona. Account-level budget thresholds and notification ownership remain
unresolved, so this note does not authorize a policy document or live rollout.

## Tracker Mapping

The tracker schedules **IAM foundation** for the week beginning 2026-07-13.
This artifact supports SAP-C02 Domain 1, the partial IAM Identity Center access
model, and the open account-level budget-threshold governance gap. It preserves
the preflight's documentation-first scope and the required separate approval
gate for every AWS-changing task.

## Established Baseline

- The management account is the intended home for billing and cost-governance
  access; it is not a workload-operation role.
- The permission-set matrix records `BillingAdmin` as a design-only,
  management-account persona for Billing, Budgets, Cost Explorer, and cost
  allocation tags.
- A managed-workflow budget baseline and selected Billing Cost Allocation Tags
  already exist, but broader account-level budget coverage remains partial.
- `SecurityAudit` and `SecurityToolingAdmin` are the live, bounded routine
  paths in Security Tooling. This slice does not broaden either path or change
  the management-only emergency path.

Sources: `docs/planning/identity-center-permission-set-matrix-20260619.md`,
`docs/planning/domain-1-identity-center-assignment-decision-20260710.md`,
`docs/planning/domain-1-governance-preflight-20260618.md`, and
`docs/evidence/phase17at-budget-guardrail-apply-summary-20260610.md`.

## Accepted Boundary

`BillingAdmin` is a future, task-specific management-account permission set
for the dedicated `billing-admins` Workforce Identity group. It has a `PT1H`
session duration and the update-only policy in
`docs/policies/iam-identity-center-billing-admin.inline-policy.example.json`.
The policy supports Billing and Cost Explorer review plus approved updates to
future management-account budgets named `BillingAdmin-*` that carry
`GovernanceControl=BillingAdmin`. A privileged setup path must create and tag a
budget before this role can modify it.

It must not include Organizations account movement, SCP administration,
cost-allocation-tag administration, workload
deployment or operation, security-service administration, IAM lifecycle
management, payment or tax administration, or emergency recovery.
`OrganizationAdmin`, `SecurityToolingAdmin`, and `BreakGlassAdmin` remain
separate personas.

AWS Budgets maps creation and modification to the same IAM action,
`budgets:ModifyBudget`. This design constrains that action to a named ARN
pattern and an existing resource tag, and withholds `budgets:TagResource` and
the dependent `iam:CreateServiceLinkedRole` permission. It therefore does not
provide a general budget-creation path. Budget deletion behavior must be
included in the final live-policy simulation and precheck; do not assume it is
separately controllable when AWS exposes only the combined permission.

## Required Decisions Before a Policy or Assignment

The following decisions are intentionally not inferred from the existing
budget baseline:

| Required decision | Why it is a gate |
|---|---|
| Named owner and backup owner | Group membership must have accountable human ownership before any live assignment. |
| Management, lakehouse, and sandbox budget thresholds | A policy cannot make an arbitrary threshold operationally safe. |
| Notification recipients and escalation path | Budget alerts need a monitored, approved destination. |
| Cost-allocation-tag administration scope | Tag activation and workload tag policy are separate governance decisions. |
| Evidence retention and redaction approach | Budget evidence must remain public-safe while proving the control. |

## Next Safe Work Unit

The initial threshold and ownership decision is now recorded in
`docs/planning/domain-1-budget-threshold-notification-ownership-decision-20260712.md`.
It retains the current live budget unchanged, rejects an unsupported new
threshold, and keeps `BillingAdmin` design-only. The first April-June history
is recorded in `docs/evidence/domain1-governance-budget-cost-history-20260712.md`,
but contains only one meaningful finalized month. The next safe unit is private
owner confirmation plus sufficient finalized cost and tag-coverage evidence
before proposing any threshold or write boundary.

No AWS command is needed for this design note. If later work proposes a live
budget or IAM Identity Center change, it must first meet the governance
runbook's named-account, exact-change, cost, rollback, validation, redaction,
and separate-explicit-approval requirements.

## Explicit Deferrals

- Create or assign `BillingAdmin`.
- Create, update, or delete budgets, alerts, tags, or notification targets.
- Define an `OrganizationAdmin` or `BreakGlassAdmin` replacement policy.
- Change Security Hub, OAM, GuardDuty, AWS Config, Organizations, SCPs, or any
  lakehouse resource.

## SAP-C02 Relevance

This separates billing governance from organization administration and
workload operation, requires accountable ownership before cost-control writes,
and treats federated least privilege as an operating design rather than a
generic administrator role.
