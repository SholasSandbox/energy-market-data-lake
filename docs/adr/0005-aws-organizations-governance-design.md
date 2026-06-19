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
|-- Workloads OU
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
- Create a CloudTrail and log archive design note, including bucket ownership,
  KMS posture, retention, and delete protection.
- Create an AWS Config and GuardDuty design note with cost controls.
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
