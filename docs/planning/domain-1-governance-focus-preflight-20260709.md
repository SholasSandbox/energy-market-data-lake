# Domain 1 Governance Focus Preflight - 2026-07-09

## Purpose

This repository-only preflight prepares the formal Domain 1 governance focus
scheduled for 2026-07-13. It supports SAP-C02 Domain 1, the tracker governance
checklist, and near-term cloud-architect positioning.

This document does not request, authorize, or perform an AWS change.

## Current Evidence Boundary

- The Lakehouse June-July closure milestone is complete; this preflight does
  not reopen lakehouse implementation work.
- `so-aws-admin` (`054394900225`) was closed on 2026-07-09 after a zero-blocker
  pre-close check. See
  `docs/evidence/domain1-governance-so-aws-admin-account-closure-20260709.md`.
- AWS Config delegated administration, aggregation, and the Security Tooling
  recorder are established in `Security Tooling` in `eu-west-2`. See
  `docs/evidence/domain1-governance-config-security-tooling-migration-change-note-20260706.md`
  and
  `docs/evidence/domain1-governance-config-security-tooling-recorder-change-note-20260707.md`.
- GuardDuty delegated administration and foundational coverage are established
  in `Security Tooling` in `eu-west-2`; optional protection plans remain
  disabled. See
  `docs/evidence/domain1-governance-guardduty-delegated-admin-change-note-20260707.md`.

## Scope For The 13 July Focus

1. Maintain current governance evidence and collect a read-only GuardDuty
   usage/cost observation before considering another Region or an optional
   protection plan.
2. Continue documentation-only work on the broader IAM Identity Center
   assignment model and account-level budget thresholds.
3. Keep Security Hub and OAM as later adoption decisions. Do not start either
   service merely because the governance focus has begun.

## Hard-Deferral Review

The following remain out of scope unless the tracker is explicitly changed:

- Docker/container implementation.
- New AI orchestration expansion.
- UI/dashboard expansion.
- Deep EKS, complex microservices, deep REMIT workflows, and non-essential
  portfolio polish.

## Live-Change Gate

No live change is proposed by this preflight. Before any future AWS-changing
task, create a control-specific change note using
`docs/runbooks/domain-1-governance-live-readiness-runbook.md` that includes:

- a named account, OU, Region, and exact API or configuration change;
- current read-only evidence and the expected blast radius;
- an explicit rollback and validation path;
- expected cost and notification impact;
- public-evidence redaction validation; and
- separate explicit approval for that exact scope.

## Completion Criteria

This preflight is complete when it is referenced by the tracker. The first
GuardDuty usage/cost observation is recorded in
`docs/evidence/domain1-governance-guardduty-usage-cost-observation-20260709.md`.
The documentation-only IAM Identity Center assignment decision is recorded in
`docs/planning/domain-1-identity-center-assignment-decision-20260710.md`.
The following outcomes remain open and require separate evidence: any Security
Hub or OAM adoption decision, live IAM Identity Center assignment changes, and
account-level budget thresholds.
