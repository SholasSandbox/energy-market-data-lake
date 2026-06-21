# Domain 1 Governance Preflight - 2026-06-18

<!-- markdownlint-disable MD013 -->

## Scope

This complementary planning document records the recommended repo-only steps
for moving from the completed lakehouse closure into the scheduled Domain 1
governance phase.

It aligns with:

- `docs/planning/sap-c02-readiness-tracker.md`, which keeps the SAP-C02 tracker
  as the controlling source for this repository.
- `/Users/[redacted-user]/Kiro-Workspace/handlers/LEARNING-PLAN.md`, which keeps the
  Python/serverless tutorial workspace separate from lakehouse implementation
  evidence.

This is a documentation-only preflight. It does not authorize live AWS changes,
Control Tower deployment, SCP attachment, Identity Center changes, CloudTrail
changes, AWS Config changes, GuardDuty/Security Hub enablement, or Terraform
apply.

## Confirmed Alignment

The recommended repo-only sequence supports the tracker because it advances:

1. SAP-C02 Domain 1: organizational complexity, IAM, governance, logging, and
   account-boundary reasoning.
2. The Energy Data Lakehouse as a governed portfolio case study.
3. Near-term cloud architect positioning through clear multi-account and
   control-plane documentation.
4. Cost, logging, security, and governance controls without expanding parked
   dashboard, AI orchestration, container, or Kubernetes work.

It does not conflict with the learning plan because it does not copy tutorial
code into this repository or claim tutorial work as lakehouse evidence. Tutorial
sessions remain separate study/lab evidence unless a pattern is explicitly
adapted, tested, and evidenced in this repository.

## Step-By-Step Repo Plan

1. Create the repo-only branch `domain1-governance-preflight`.
2. Add this Domain 1 governance preflight note.
3. Document the current account structure.
4. Propose the target governance structure.
5. Draft first-pass governance artifacts.
6. Update the tracker without overclaiming implementation.
7. Update `README.md` and `PLANS.md` only where they need to point to the
   preflight.
8. Commit, open a PR, merge to `main`, and synchronize `origin/main`.

## Current Account Structure

| Account | Current role | Repo interpretation |
|---|---|---|
| `management-account-alias` | AWS Organizations management account | Owns organization-level billing and governance administration. It is not the lakehouse workload account. |
| `lakehouse-workload-account` | Member account | Energy Data Lakehouse workload account and the account used for lakehouse evidence. |
| `containers-lab.com` | Member account | Separate container lab account. It is not lakehouse implementation evidence. |

The completed Cost Allocation Tag activation evidence verifies the management
account and member accounts:
`docs/evidence/cost-allocation-tag-activation-20260617.md`.

The later read-only organization inventory evidence is now recorded in
`docs/evidence/domain1-governance-org-inventory-summary-20260621.md`.

## Proposed Target Governance Structure

The target structure should be simple enough for a lab organization but shaped
like a professional AWS landing zone:

```text
AWS Organization
│
├── Management account
│   ├── Organizations administration
│   ├── Billing and Cost Management
│   ├── IAM Identity Center administration
│   └── SCP administration
│
├── Security OU
│   └── Future security/log archive account
│
├── Lakehouse Workloads OU
│   └── Energy Data Lakehouse workload account
│
├── Sandbox OU
│   └── Container labs account
│
└── Suspended OU
    └── Quarantined or closed accounts
```

This is a proposed design, not a live Organizations change. The current account
placement should not be changed until the governance phase explicitly approves
OU changes, rollback, and cost/safety boundaries.

The current-to-target placement decision based on later organization inventory
evidence is now recorded in
`docs/planning/domain-1-ou-account-placement-decision-20260621.md`.

## Management Account Rules

The management account should be treated as a control-plane account:

- Use it for Organizations, billing, Cost Allocation Tags, IAM Identity Center,
  and SCP administration.
- Do not run ordinary lakehouse workloads in it.
- Keep root-user access emergency-only with MFA and stored recovery details.
- Use named roles or Identity Center permission sets for administration.
- Avoid long-lived IAM users except where a documented bootstrap or break-glass
  process requires them.
- Capture organization-level governance changes as evidence before claiming
  tracker completion.

## Workload And Sandbox Boundaries

The lakehouse workload account should own:

- S3 data bucket and dashboard boundary evidence.
- Lambda, Glue, Athena, Step Functions, EventBridge, SQS, DynamoDB, and
  lakehouse IAM boundaries.
- Workload-specific budgets, tags, logging, and operational runbooks.

The container labs account should remain separate from the lakehouse case
study. Container evidence can support future SAP-C02 study only if the tracker
explicitly grants a short exception or the post-exam parking-lot deferral ends.

## Future Security And Logging Boundary

The security/log archive boundary should be designed before it is implemented.
The target responsibilities are:

- Organization CloudTrail trail ownership.
- Central log archive S3 bucket design.
- AWS Config aggregation.
- GuardDuty and Security Hub administration decisions.
- KMS ownership decisions for central logs if customer managed keys become
  required.

This repository can document the design, but live central logging enablement
requires explicit approval and a separate change boundary.

## IAM Identity Center Outline

Initial permission-set candidates:

| Permission set | Purpose | Notes |
|---|---|---|
| `OrganizationAdmin` | Manage AWS Organizations, SCPs, and account placement | Management account only; tightly controlled. |
| `BillingAdmin` | Manage budgets, Cost Allocation Tags, and billing views | Management account only. |
| `SecurityAudit` | Read-only security, logging, and audit visibility | Prefer read-only plus Security Hub/GuardDuty visibility. |
| `LakehouseOperator` | Operate lakehouse workload services | Workload account only; no Organizations administration. |
| `LakehouseReadOnly` | Review lakehouse evidence and runtime posture | Workload account read-only. |
| `BreakGlassAdmin` | Emergency administrative recovery | MFA, documented owner, alerting, and review required. |

Identity Center permission sets grant access through IAM roles in target
accounts. SCPs still do not grant permissions; they only set maximum allowed
permissions.

## First-Pass SCP Catalogue

| SCP | Purpose | Draft scope |
|---|---|---|
| Deny disabling CloudTrail | Protect audit evidence | Apply after organization trail design exists. |
| Deny deleting log archive buckets | Protect central logs | Apply only after log archive account and bucket naming are finalized. |
| Deny public S3 exposure | Reduce accidental data exposure | Start with guardrail design; avoid blocking approved public dashboard bucket patterns without testing. |
| Deny unapproved Regions | Limit cost and operational spread | Allow required global services and `eu-west-2`; test service exceptions. |
| Deny root-user actions except emergencies | Reduce blast radius | Requires a clear break-glass process before attachment. |
| Require encryption where feasible | Improve compliance posture | Must account for services that do not support the same condition keys. |
| Deny leaving AWS Organization | Prevent governance bypass | Apply to member accounts after management-account recovery model is documented. |

These are catalogue candidates. They are not ready for attachment until each
has service exceptions, test cases, rollback guidance, and a target OU.

## CloudTrail, AWS Config, And Security Services Outline

| Control | Preflight decision | Open work |
|---|---|---|
| CloudTrail organization trail | Design one organization trail with management events, log-file validation, and a central log archive target. Detailed design is now recorded in `docs/planning/domain-1-cloudtrail-log-archive-design-20260621.md`. | Live account creation, final bucket/prefix names, exact bucket/KMS policies, retention implementation, and validation evidence remain open. |
| AWS Config aggregation | Use organization aggregation to support account and region visibility. Detailed design is now recorded in `docs/planning/domain-1-config-guardduty-design-20260621.md`. | Live recorder enablement, final exclusions, final ruleset, and validation evidence remain open. |
| GuardDuty | Treat as a security-service candidate for organization-level finding visibility. Detailed delegated-admin and cost-control design is now recorded in `docs/planning/domain-1-config-guardduty-design-20260621.md`. | Final Region set, optional protection-plan choices, and live enablement evidence remain open. |
| Security Hub | Treat as a later aggregation and standards-review layer. Current decision is to defer broad adoption until Config and GuardDuty scope is settled; see `docs/planning/domain-1-config-guardduty-design-20260621.md`. | Revisit delegated-admin, standards, and cost posture after Config and GuardDuty implementation scope is finalized. |

## Budget And Tagging Governance

Current verified cost controls:

- Lakehouse data bucket tags are applied.
- Selected Billing Cost Allocation Tags are active from the management account.
- A managed-workflow budget baseline exists.

Open governance work:

- Define account-level budget thresholds for management, lakehouse workload, and
  container labs.
- Decide whether budget notifications should be centralized.
- Define required tag keys for workload accounts.
- Decide whether tag policies are worth adding before the SAP-C02 attempt.

## Non-Goals

This preflight does not:

- deploy or configure AWS resources;
- move accounts between OUs;
- attach or test SCPs;
- create Identity Center permission sets;
- enable organization CloudTrail, AWS Config, GuardDuty, or Security Hub;
- change Terraform;
- use tutorial code as lakehouse evidence;
- expand containers, AI orchestration, dashboard, DNS, or ACM work.

## Tracker Update Guidance

Rows touched by this preflight should move at most to `Partial`, because the
artifact creates planning evidence but does not complete implementation or live
verification. Full completion belongs to the scheduled Domain 1 governance
phase unless the tracker is explicitly updated. The next repo-only bridge
artifact for that phase is
`docs/runbooks/domain-1-governance-live-readiness-runbook.md`, which packages
the per-change evidence, rollback, validation, and approval boundary without
authorizing live AWS changes.
