# ADR 0005: AWS Organizations Governance Design

<!-- markdownlint-disable MD013 -->

- Status: Accepted for design; implementation not approved
- Date: 2026-06-19
- Related tracker section: Domain 1 governance readiness
- Related preflight: `docs/planning/domain-1-governance-preflight-20260618.md`

## Context

The repository has completed the June-July lakehouse closure work and now needs
a professional-shaped Domain 1 governance design before any live governance
changes are attempted.

The current AWS Organization has:

- `management-account-alias` as the management account;
- `lakehouse-workload-account` as the Energy Data Lakehouse member account;
- `containers-lab.com` as a separate container-lab member account.

The Energy Data Lakehouse remains the applied case study. The container-lab
account is separate study/lab scope and must not be treated as lakehouse
implementation evidence.

This ADR records the target governance design only. It does not authorize AWS
Organizations changes, OU moves, SCP attachment, IAM Identity Center changes,
CloudTrail changes, AWS Config changes, GuardDuty or Security Hub enablement,
Terraform apply, or any other live AWS modification.

## Decision

Adopt a small landing-zone-shaped governance model:

```text
AWS Organization
|
|-- Management account
|   |-- Organizations administration
|   |-- Billing and Cost Management
|   |-- IAM Identity Center administration
|   `-- SCP administration
|
|-- Security OU
|   `-- Future security/log archive account
|
|-- Lakehouse Workloads OU
|   `-- Energy Data Lakehouse workload account
|
|-- Sandbox OU
|   `-- Container labs account
|
`-- Suspended OU
    `-- Quarantined or closed accounts
```

Use the management account only as the control plane. Keep lakehouse runtime
services in the workload member account. Keep the container-lab account in
separate sandbox scope.

Although AWS Organizations allows the management account to be placed anywhere
in the organization hierarchy, this design intentionally keeps the management
account attached directly to the root.

Reason:

- it matches the standard small-landing-zone control-plane pattern used in AWS
  guidance and commonly seen in SAP-C02-style designs;
- it keeps the payer and organization-administration account visually distinct
  from policy-target OUs that primarily exist for member accounts;
- it avoids implying that OU placement would give meaningful SCP restriction on
  the management account, which it does not;
- it keeps the management account easy to explain as a special-case root-level
  control plane rather than an ordinary workload-bearing account.

For the current organization shape, `Lakehouse Workloads OU` is the preferred
name over the more generic `Workloads OU`.

Reason:

- it is more descriptive of the single active workload boundary that exists now;
- it aligns directly with the Energy Data Lakehouse case study, which is the
  primary applied architecture in this repository;
- it is still broad enough to hold closely related lakehouse and analytics
  runtime services without implying a much larger enterprise taxonomy.

The first two approved workload-boundary changes are now recorded separately:

1. create `Lakehouse Workloads OU`;
2. move `lakehouse-workload-account` into that OU from the root.

Evidence:

- `docs/evidence/domain1-governance-lakehouse-workloads-ou-change-note-20260621.md`
- `docs/evidence/domain1-governance-lakehouse-account-move-change-note-20260622.md`

Use IAM Identity Center as the preferred human-access model, with permission
sets aligned to administrative duty boundaries:

| Permission set | Target account | Purpose |
| --- | --- | --- |
| `OrganizationAdmin` | Management account | Organizations, OU placement, and SCP administration. |
| `BillingAdmin` | Management account | Billing, budgets, Cost Allocation Tags, and cost reports. |
| `SecurityAudit` | Management and workload accounts | Read-only security, logging, and audit visibility. |
| `LakehouseOperator` | Lakehouse workload account | Operate lakehouse workload services without organization administration. |
| `LakehouseReadOnly` | Lakehouse workload account | Review runtime posture and evidence without write access. |
| `BreakGlassAdmin` | As required | Emergency recovery only, with MFA, owner, logging, and review. |

Use SCPs as guardrails, not as permission grants. IAM policies and permission
sets grant access; SCPs define maximum allowed permissions for accounts or OUs.

Adopt this initial SCP catalogue as design intent only:

| SCP | Target | Design intent |
| --- | --- | --- |
| Deny disabling CloudTrail | Workloads, Sandbox, future Security OU | Protect audit evidence once an organization trail exists. |
| Deny deleting central log buckets | Future Security OU and member accounts | Protect central logs after log archive naming and ownership are finalized. |
| Deny public S3 exposure by default | Workloads and Sandbox | Reduce data leakage while allowing explicitly approved public dashboard patterns. |
| Deny unapproved Regions | Workloads and Sandbox | Limit cost and operational spread, with global-service and `eu-west-2` exceptions. |
| Deny root-user actions except emergencies | Member accounts | Reduce blast radius after break-glass procedure is documented. |
| Require encryption where feasible | Workloads and Sandbox | Improve baseline posture while accounting for service-specific condition-key support. |
| Deny leaving the AWS Organization | Member accounts | Prevent governance bypass after management-account recovery is documented. |

Design organization logging as:

- one organization CloudTrail trail with management events and log-file
  validation;
- central log archive ownership in a future security/log archive account;
- AWS Config organization aggregation after recorder scope and cost controls are
  defined;
- GuardDuty as the first security-service aggregation candidate;
- Security Hub as a later standards and finding-aggregation layer if the cost
  and study value justify it.

Retain the currently activated cost-allocation tag keys as the baseline
governance tag set:

- `Project`
- `Workload`
- `Environment`
- `Purpose`
- `ManagedBy`
- `DataClassification`
- `CostCenter`

## Alternatives Considered

| Option | Decision | Why |
| --- | --- | --- |
| Small landing-zone-shaped organization with management, security, workloads, sandbox, and suspended boundaries | Accepted | Gives SAP-C02-relevant account separation and guardrail reasoning while staying small enough for a personal lab organization. |
| Use the generic name `Workloads OU` for the lakehouse workload boundary | Rejected for now | Understandable, but too vague for the current three-account organization; `Lakehouse Workloads OU` better explains what actually lives there today. |
| Keep all accounts under root with no OU model | Rejected | Simple, but fails to demonstrate OU-based governance, scoped SCP attachment, account lifecycle thinking, and professional landing-zone reasoning. |
| Deploy AWS Control Tower immediately | Rejected for now | Strong managed landing-zone option, but too broad for the current repo step and would introduce live account, guardrail, and lifecycle changes before design review. |
| Create many environment-specific OUs such as Dev, Test, Prod, Shared Services, Network, and Data | Rejected for now | More enterprise-like, but over-engineered for two active member accounts and would blur learning goals with unnecessary account taxonomy. |
| Treat the lakehouse and container accounts as the same workload class | Rejected | Weakens portfolio clarity; the lakehouse is the applied case study, while container labs remain parked/sandbox study scope. |
| Put all human access in IAM users | Rejected | Long-lived users are harder to govern and audit than Identity Center permission sets and should not be the target model. |
| Attach restrictive SCPs before testing service exceptions | Rejected | Could block legitimate lab operations or recovery paths; each SCP needs policy simulation, exceptions, target OU, rollback, and evidence. |
| Enable every security service immediately | Rejected for now | GuardDuty, Security Hub, Config, and CloudTrail can create cost and operational noise; sequencing should start with audit/logging design and add services intentionally. |

## Trade-Offs

This design favors clarity and exam relevance over complete enterprise
coverage. It introduces real governance concepts without pretending the lab is
a full production landing zone.

The accepted design is stronger than a flat organization because it gives clear
places to reason about account purpose, blast radius, SCP scope, logging, and
cost ownership. It is lighter than Control Tower because it avoids a live
platform rollout before the repo has finalized policies, rollback paths, and
evidence requirements.

Keeping the management account at the root is slightly less symmetrical than
placing every account inside an OU, but it is a worthwhile trade-off here. The
management account is a control-plane exception, not a normal workload target,
and keeping it at the root makes that distinction clearer while avoiding false
confidence that OU-level SCP structure would constrain it like a member
account.

Naming the workload OU `Lakehouse Workloads OU` adds a little specificity
compared with the shorter `Workloads OU`, but that is a worthwhile trade-off
for the current repo because it makes the intended lakehouse boundary explicit
and easier to explain in review, exam, and portfolio contexts.

The main cost of the design is additional documentation and policy discipline:
each OU, permission set, and SCP needs an owner, target, exception model, test
case, and rollback plan. That overhead is intentional because Domain 1 requires
trade-off reasoning, not just service naming.

## SAP-C02 Implications

This ADR supports SAP-C02 Domain 1 by making the following exam-relevant
distinctions explicit:

- management accounts are control-plane accounts, not workload accounts;
- member accounts isolate blast radius and ownership boundaries;
- OUs provide administrative grouping and SCP attachment scope;
- SCPs do not grant permissions;
- IAM Identity Center permission sets are human-access grants through roles;
- central logging and audit protection require account, bucket, KMS, retention,
  and deletion-control decisions;
- security services need delegated-administrator, aggregation, region, and cost
  decisions;
- cost-allocation tags are useful only when tagging standards and billing
  activation are aligned.

The ADR also supports Domain 3 because it improves an existing architecture by
adding governance boundaries, operational control points, and implementation
gates without destabilizing the working lakehouse.

## Implementation Boundary

No live change is approved by this ADR.

Before any implementation, create a separate runbook or evidence note that
states:

1. the exact target account and OU;
2. the current state from read-only AWS CLI commands;
3. the proposed change;
4. the expected blast radius;
5. the rollback path;
6. the validation command or console check;
7. the cost impact;
8. the explicit approval for that one change.

Apply sequencing should be:

1. Read-only Organizations and account inventory evidence.
2. Final OU names and account-placement decision.
3. Identity Center permission-set design and assignment plan.
4. Break-glass procedure.
5. Organization CloudTrail/log archive design.
6. SCP policy examples with exceptions and tests.
7. AWS Config and GuardDuty design.
8. Optional Security Hub design.
9. Live changes only after explicit approval per change boundary.

## Follow-Up Actions

- SCP policy example files are recorded in `docs/policies/scp/`.
- The IAM Identity Center permission-set matrix is recorded in
  `docs/planning/identity-center-permission-set-matrix-20260619.md`.
- The break-glass procedure is recorded in
  `docs/runbooks/break-glass-access-procedure.md`.
- CloudTrail and log archive design is now recorded in
  `docs/planning/domain-1-cloudtrail-log-archive-design-20260621.md`, covering
  bucket ownership, KMS posture, retention, and delete protection.
- AWS Config and GuardDuty design with cost controls, plus the current Security
  Hub defer/adopt decision, is now recorded in
  `docs/planning/domain-1-config-guardduty-design-20260621.md`.
- The governance live-readiness runbook that operationalizes this ADR's
  per-change boundary and evidence requirements is now recorded in
  `docs/runbooks/domain-1-governance-live-readiness-runbook.md`.
- The first read-only organization inventory evidence is now recorded in
  `docs/evidence/domain1-governance-org-inventory-summary-20260621.md`.
- The current-to-target OU and account-placement decision is now recorded in
  `docs/planning/domain-1-ou-account-placement-decision-20260621.md`.
- The first approved live Organizations change, creating `Lakehouse Workloads
  OU`, is now recorded in
  `docs/evidence/domain1-governance-lakehouse-workloads-ou-change-note-20260621.md`.
- The second approved live Organizations change, moving `lakehouse-workload-account` into
  `Lakehouse Workloads OU`, is now recorded in
  `docs/evidence/domain1-governance-lakehouse-account-move-change-note-20260622.md`.
- The first bounded OU-targeted SCP change note, choosing
  `DenyLeavingOrganization` ahead of a root-user restriction because it has the
  cleaner blast radius and lower recovery-path risk, is now recorded in
  `docs/evidence/domain1-governance-deny-leaving-organization-change-note-20260622.md`.
- The first live OU-targeted SCP attachment attempt was rolled back after
  `AttachPolicy` returned `PolicyTypeNotEnabledException`, which exposed a
  separate root-level policy-type enablement prerequisite before OU-targeted
  SCP rollout can begin.
- That prerequisite has now been resolved by enabling
  `SERVICE_CONTROL_POLICY` for root `r-gbyf`, and the retried
  `DenyLeavingOrganization-LakehouseWorkloads` attachment to
  `Lakehouse Workloads OU` has now succeeded under separate change evidence.
- Define account-level budget thresholds for management, lakehouse workload,
  and sandbox accounts.
- Update the tracker only as design artifacts are created; keep implementation
  rows partial until live verification exists.

## Revisit Conditions

Revisit this ADR if a new member account is added, the lakehouse becomes
production or regulated, Control Tower is intentionally adopted, the user
chooses to create a dedicated security/log archive account, container work is
unparked, or live governance changes reveal service exceptions that materially
change the OU or SCP model.
