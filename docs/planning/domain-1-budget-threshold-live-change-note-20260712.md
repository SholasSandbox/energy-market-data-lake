# Domain 1 Governance Change Note - Account Budget Thresholds - Pending

## Status

Approval-readiness note only. It is **not approved for execution** and does not
create, update, copy, or delete a budget, alert, subscriber, tag, or billing
configuration.

This is deliberately separate from the future `BillingAdmin` permission-set
assignment. Budget mutation remains its own approval-bound change unit.

## Target Account and Scope

| Field | Intended boundary |
|---|---|
| Control plane | Organizations management account |
| Proposed targets | Management, lakehouse workload, Security Tooling, Security Log Archive, and sandbox only when each threshold is evidence-supported |
| Budget type | Monthly cost budgets only |
| Notification scope | Approved monitored destinations only; subscriber identities stay in private operational records |
| Explicit exclusions | Organizations, SCP, IAM Identity Center, tag-policy, workload deployment, and security-service changes |

## Proposed Change

After all gates below are met, apply only the approved per-account monthly
budget thresholds and their approved actual/forecast notification levels. The
change request must name every target account, budget name, cost filter,
threshold, alert level, subscriber count, and rollback value.

No numeric threshold is populated in this note. The current evidence contains
only one meaningful finalized month and cannot support an accountable limit.
This note must not be used to copy the existing all-services `$10` budget.

## Current Evidence

- The sole current management-account budget is unfiltered, has a `$10` limit,
  is already breached, and has forecast spend above twice that limit:
  `docs/evidence/domain1-governance-budget-cost-baseline-20260712.md`.
- April-June history provides only one meaningful finalized month and the
  `Workload` tag is mostly untagged:
  `docs/evidence/domain1-governance-budget-cost-history-20260712.md`.
- The available finalized monthly dataset contains only May and June, with
  June as the first meaningful month:
  `docs/evidence/domain1-governance-budget-finalized-monthly-data-20260712.md`.
- The threshold and notification ownership decision therefore accepts no new
  threshold yet:
  `docs/planning/domain-1-budget-threshold-notification-ownership-decision-20260712.md`.

## Required Immediate Precheck

Immediately before any execution, save public-safe read-only evidence of:

1. management-account identity and the exact target account inventory;
2. all current budgets, budget limits, time units, filters, actual and forecast
   spend, notification thresholds, and subscriber counts;
3. the approved private primary and backup owner plus monitored email
   notification destination, without storing identities in this repository;
4. the approved three-or-more-month cost history and tag-coverage assessment;
5. the exact prechange configuration for every target budget; and
6. the expected alert, cost, and user-impact boundary.

Stop if any target account, threshold, filter, notification, ownership record,
or observed budget configuration differs from the approved request.

## Expected Blast Radius

Budget changes can create repeated actual or forecast alerts and alter the
financial signal seen by each approved recipient. Cost filters can exclude
spend unexpectedly if they do not match current account or tag coverage. No
workload runtime or data-plane change is intended.

## Rollback

For an updated budget, restore exactly its saved prechange limit, filters, and
notifications. For a newly created budget, delete only that named budget after
confirming the approved recipients and no unrelated budget are affected.
Capture a post-rollback read-only budget and notification comparison.

## Validation

- Each intended budget exists only in its approved account with the exact
  monthly limit and cost filter.
- Actual and forecast notification thresholds match the approved values.
- Each notification has the approved subscriber count; do not publish
  subscriber identities.
- Cost Explorer account and tag coverage matches the design assumptions.
- Existing unrelated budgets and notification configurations are unchanged.

## Cost and Evidence Impact

The change may create or increase alert volume and operational response work.
Its expected financial-control effect, subscription assumptions, and any
recurring AWS cost must be stated from the final threshold design before
approval. Public evidence must remain redacted and pass
`scripts/check_public_evidence_redaction.sh`.

## Approval

**Pending.** Private primary/backup ownership and the email notification path
were confirmed on 2026-07-12 and are retained outside this public repository.
Before execution, this section must still identify the approver, date, target
accounts, exact budget names, limits, filters, alert levels, fresh monitored
recipient confirmation, cost expectation, rollback values, and validation
commands. User authorization to create this note is not approval to execute a
budget change.

## SAP-C02 Relevance

This supports Domain 1 by treating financial governance as an account-scoped,
evidence-led control with explicit notification ownership, rollback, and
least-privilege separation from IAM administration.
