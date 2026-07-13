# Domain 1 Budget Threshold and Notification Ownership Decision - 2026-07-12

## Status

Accepted documentation-only decision for the 2026-07-13 IAM-foundation week.
It uses fresh read-only cost evidence and does not authorize a budget,
notification, tag, IAM policy, permission-set, or account change.

## Decision

Do not adopt a new account-level budget threshold or create `BillingAdmin` from
the current single `$10` all-services budget. Retain the existing live budget
and its notifications unchanged, while treating it as a managed-workflow-era
control with an unresolved ownership and scope gap rather than an organization
standard.

For the future model, separate these responsibilities:

| Responsibility | Decision |
|---|---|
| Control approval owner | The normal management-account administrator approves a threshold or notification change only after the required evidence and separate live-change approval exist. |
| Notification recipients | Retain the current two-recipient set unchanged. The public repository does not identify recipients or assume they are accountable control owners. |
| Private accountability record | Confirmed on 2026-07-12: a primary owner and backup owner are retained outside this public repository, with email as the notification path. Before a live change, re-confirm that the approved recipients are monitored. |
| `BillingAdmin` | Keep design-only and review-only. Do not create or assign it until thresholds, recipient ownership, policy scope, rollback, and validation are approved. |
| Emergency access | Do not use `BreakGlassAdmin` as a routine billing-notification or cost-review path. |

## Evidence and Rationale

Fresh public-safe evidence in
`docs/evidence/domain1-governance-budget-cost-baseline-20260712.md` shows:

- the sole management-account budget has a `$10` monthly limit but actual
  spend of `$12.927` and forecast spend of `$23.402`;
- it has no cost filters, so it cannot distinguish management, lakehouse,
  security, or sandbox spend;
- the verified notifications are 85% actual, 100% actual, and 100% forecast,
  each with two existing subscribers; and
- current cost is concentrated in the lakehouse workload and Security Log
  Archive, not the management account.

A new numeric threshold would therefore be arbitrary. Copying the current
all-services budget to every account would also create false signals and blur
accountability. A `BillingAdmin` write policy before those choices would widen
access without an approved operating boundary.

## Alternatives Considered

| Alternative | Decision | Why not chosen now |
|---|---|---|
| Copy the existing `$10` budget to every active account | Rejected | It is already breached, unfiltered, and does not reflect the observed account roles or spend. |
| Raise the current budget immediately | Rejected | A higher figure without history, owner approval, and notification review would be arbitrary and is an AWS mutation. |
| Create and assign `BillingAdmin` as a broad billing administrator | Rejected | Policy scope, accountable owners, and write versus review authority are not yet decided. |
| Retain the current control and establish evidence-led gates | Accepted | It preserves alerting while avoiding unsupported thresholds and privilege expansion. |

## Required Inputs for the Next Decision

Before proposing any threshold or live change, collect and document:

1. At least three complete monthly Cost Explorer periods, grouped by linked
   account and relevant activated tags where data is available.
2. A workload-level explanation for the current lakehouse and log-archive cost
   drivers, including whether costs are steady, transitional, or exceptional.
3. Re-confirm immediately before execution that the privately recorded primary
   and backup budget owners and the approved email recipients are monitored.
4. A written choice between review-only `BillingAdmin` access and tightly
   scoped budget-update access.
5. Per-account threshold rationale, alert levels, expected recurring cost,
   rollback, validation, and public-evidence redaction plan.

## Historical-Cost Assessment

The requested April-June Cost Explorer history is now recorded in
`docs/evidence/domain1-governance-budget-cost-history-20260712.md`. Although
it covers three calendar periods, April remains estimated at zero, May is
effectively zero, and June is the only meaningful finalized observation.
`Workload` tag coverage is also predominantly untagged. The 2026-07-13
account-and-service refresh now attributes the June Lakehouse amount and its
main EBS driver without relying on the tag, but July remains estimated and
incomplete. See `docs/evidence/domain1-governance-cost-attribution-20260713.md`.

The finalized subset is published separately in
`docs/evidence/domain1-governance-budget-finalized-monthly-data-20260712.md`.
The 12-month lookback confirms that only May and June are finalized; May is
effectively zero and June is the first meaningful month.

**Threshold proposal:** no numeric management, lakehouse, security, or sandbox
threshold is proposed or accepted. That is the evidence-supported outcome: a
number derived from a single meaningful month would be arbitrary and could
either desensitize a control or create avoidable alert noise.

The next proposal may proceed only after three meaningful finalized months,
adequate tag coverage for any tag-based control, and private confirmation of a
primary owner, backup owner, and monitored notification destination. The
private owners have approved the design; fresh monitored-recipient
confirmation is still required immediately before any live change.

## Explicit Deferrals

- Creating, changing, deleting, or copying a budget or notification.
- Retrieving or publishing subscriber identities.
- Creating or assigning `BillingAdmin`.
- Changing cost allocation tags or adding a tag policy.
- Any Organizations, SCP, Identity Center, Security Hub, OAM, Config,
  GuardDuty, or lakehouse change.

## Revisit Condition

The cost-threshold review is scheduled for **2026-07-27**. It is a
documentation and read-only evidence review that will assess July month-to-date
spend, attribution changes, and whether an interim control is warranted; it is
not expected to create the missing finalized-month history or authorize an AWS
change.

The missing history is **not a blocker to the current low-volume Lakehouse
platform**. It is only a gate on adopting new evidence-based per-account budget
thresholds or expanding `BillingAdmin` into live update access. The existing
budget and notifications remain unchanged in the meantime.

Revisit a live-change decision when the required evidence and private ownership
record are complete. Any live budget or IAM Identity Center change then remains
one separate approval-bound change unit under the governance runbook.

## SAP-C02 Relevance

This supports Domain 1 by separating billing governance from organization and
workload administration, using evidence rather than arbitrary thresholds, and
requiring least-privilege and ownership gates before cross-account governance
controls are changed.
