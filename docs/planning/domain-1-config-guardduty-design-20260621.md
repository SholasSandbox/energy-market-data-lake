# Domain 1 AWS Config And GuardDuty Design - 2026-06-21

<!-- markdownlint-disable MD013 -->

## Scope

This note records the repo-only design for AWS Config aggregation,
GuardDuty delegated administration, and the related Security Hub
defer/adopt decision for the Energy Data Lakehouse governance phase.

It aligns with:

- `docs/planning/sap-c02-readiness-tracker.md`, which keeps governance work in
  documentation/preparation mode unless a specific live change is explicitly
  approved.
- `docs/planning/domain-1-governance-preflight-20260618.md`, which called for
  AWS Config and GuardDuty design with cost controls.
- `docs/planning/domain-1-cloudtrail-log-archive-design-20260621.md`, which
  established the logging and log archive boundary first.
- `docs/adr/0005-aws-organizations-governance-design.md`, which set the
  accepted multi-account governance direction.
- `docs/runbooks/domain-1-governance-live-readiness-runbook.md`, which
  packages the later per-change evidence, rollback, and validation boundary.

This note is an early design artifact created under explicit user approval
before the originally scheduled governance start. It does not pull any live AWS
change forward, authorize Terraform, or approve service enablement.

## Confirmed Alignment

This note supports the tracker because it advances:

1. SAP-C02 Domain 1 through organization-wide configuration visibility,
   delegated security administration, and centralized governance reasoning.
2. The Energy Data Lakehouse case study as a controlled multi-account AWS
   portfolio rather than a single-account lab.
3. Near-term cloud architect positioning through concrete cost-aware governance
   choices rather than vague service lists.
4. A cleaner later decision on Security Hub by first settling Config and
   GuardDuty ownership, scope, and cost controls.

## Current Design Inputs

| Input | Current state | Design implication |
|---|---|---|
| Organization structure | Management account plus lakehouse and sandbox member accounts | Small enough to keep the design simple, but still worth modeling as a real multi-account governance pattern |
| Logging boundary | CloudTrail/log archive design now keeps `Security Log Archive` as the storage-only boundary and points delegated security operations to the separate live `Security Tooling` account | AWS Config and GuardDuty should align to `Security Tooling` rather than permanently stay in the log archive account |
| Account-readiness boundary | `Security Tooling` now exists in `Security OU`, its `SECURITY`, `OPERATIONS`, and `BILLING` alternate contacts are configured, AWS Config delegated administration plus aggregation have migrated to it, and its AWS Config recorder is live in `eu-west-2` | GuardDuty delegated-admin setup is now the next security-service candidate if adopted |
| Config recorder boundary | The migrated organization CloudTrail Config rule now includes `Security Tooling` and excludes only `so-aws-admin` | Keep `so-aws-admin` excluded on the decommission path; do not expand recorder scope into retired or placeholder accounts |
| GuardDuty delegated-admin boundary | GuardDuty delegated-admin planning now targets `Security Tooling` and explicitly rejects creating another account | Use `Security Tooling` as the delegated administrator if GuardDuty is adopted; no new GuardDuty account is needed |
| Observability boundary | External governance study now distinguishes OAM, CloudTrail log archive, and AWS Config aggregator | OAM is a future operational visibility option for `Security Tooling`, not a replacement for audit logging or Config compliance posture |
| Primary active Region | `eu-west-2` | Use one home Region assumption for the design unless a later networking or resilience decision changes that |
| Current workload profile | Lakehouse services plus a separate sandbox account | Focus on governance-relevant resources first; avoid broad cost-heavy security rollouts by default |

## Design Decisions

### 1. AWS Config aggregator ownership

Target the live `Security Tooling` account as the long-term AWS Config
aggregator account, with `eu-west-2` as the design home Region.

Rationale:

- The aggregator gives a read-only cross-account, cross-Region view rather than
  mutating control, which fits a delegated security-operations boundary well.
- Keeping aggregation in a separate `Security Tooling` account preserves
  separation of duties between write-mostly archive storage and active
  organization-level security visibility.
- AWS documents that organization-based aggregation works across accounts and
  Regions and that aggregators themselves do not provide mutating access.

Transition note:

- the current live delegated-admin and aggregator placement in `Security Log
  Archive` was accepted as a transitional implementation boundary for this lab;
- `Security Tooling` now exists in `Security OU` as account `668848431187`;
- `SECURITY`, `OPERATIONS`, and `BILLING` alternate contacts for `Security
  Tooling` were configured as a separate bounded account-readiness step on
  2026-07-06;
- AWS Config delegated administration and aggregation were migrated to
  `Security Tooling` as a separate approved service-migration step on
  2026-07-06;
- the migrated organization CloudTrail rule remains focused on recorder-bearing
  accounts; the recorder-scope decision now keeps `054394900225` excluded on
  the decommission path and records `668848431187` as the next bounded
  recorder-onboarding candidate.

OAM distinction:

- AWS Config aggregation answers configuration and compliance posture questions;
- OAM answers live operational visibility and troubleshooting questions;
- both belong in the future `Security Tooling` or central monitoring boundary,
  not in the storage-only `Security Log Archive` account.

### 2. AWS Config recorder scope

Use one customer-managed configuration recorder per enabled account and Region,
with a narrow but defensible baseline scope.

Baseline design:

- enable recorders only in Regions that are intentionally in scope;
- start with `eu-west-2` as the primary Region;
- record all supported regional resource types in the management and lakehouse
  workload accounts unless a specific exclusion is justified by cost or noise;
- avoid broad enablement in Regions that are not part of the current study or
  workload scope.

Global-resource rule:

- if global resources are needed for later Security Hub or governance controls,
  record them in one home Region only to avoid unnecessary duplication and cost.

### 3. AWS Config recording frequency

Use continuous recording as the target baseline for the management and lakehouse
workload accounts.

Rationale:

- governance and change-tracking use cases benefit from near-real-time
  configuration visibility;
- AWS documents that daily recording can delay change-triggered security
  findings;
- continuous recording better supports later audit and compliance reasoning.

Cost-control fallback:

- if the sandbox account or a later ephemeral workload pattern produces
  unexpectedly high configuration item volume, revisit exclusions first;
- consider daily recording only for clearly low-risk, low-change areas where
  delayed visibility is acceptable.

### 4. AWS Config cost controls

Use these cost controls in the design from day one:

- keep Region scope intentional instead of enabling every Region by default;
- exclude high-churn ephemeral resource types if they generate noisy,
  low-value configuration changes;
- start with a small rule set focused on governance-relevant signals;
- if Security Hub is adopted later and `AWS::Config::ResourceCompliance` is not
  needed for any other purpose, turn off recording for that resource type to
  reduce AWS Config recorder cost;
- remember that aggregation itself does not add extra AWS Config cost, but
  recorders and rules do.

### 5. Starter AWS Config rule approach

Do not begin with a large standards rollout. Start with a compact governance
starter set after recorder scope is approved.

First rule categories:

- CloudTrail presence and hygiene
- S3 public-exposure prevention
- required tag governance
- later Region or encryption guardrails only where the prerequisite design is
  already clear

This keeps the initial compliance surface small enough to understand and defend.

### 6. GuardDuty delegated-administrator design

Target `Security Tooling` as the future GuardDuty delegated administrator
account across all enabled Regions.

Rationale:

- AWS recommends not using the Organizations management account as the
  delegated GuardDuty administrator account;
- GuardDuty delegated administration is Regional, so consistency across Regions
  matters;
- using the same `Security Tooling` account across security services gives a
  cleaner operating model without expanding archive-account privileges.

Design rules:

- use the same delegated administrator account in every enabled GuardDuty Region;
- ensure the delegated administrator account itself has GuardDuty enabled;
- keep the management account as the organization control plane, not as the
  daily GuardDuty operations account.
- keep `Security Log Archive` storage-only once the later split happens, rather
  than using it as the permanent delegated GuardDuty admin.
- do not create a new account for GuardDuty; `Security Tooling` is the accepted
  target delegated administrator.

### 7. GuardDuty enablement scope

Use a layered GuardDuty design rather than enabling every protection plan by
default.

Target baseline:

- foundational GuardDuty enabled for active organization accounts in the active
  Region set, excluding `so-aws-admin` while it remains on the decommission path;
- organization configuration should ultimately prefer `ALL` for foundational
  account coverage once approved, because the current organization is small and
  complete coverage is easier to reason about than per-account drift.

Optional protection-plan posture:

- do not auto-enable every optional protection plan by default;
- evaluate optional plans one by one based on workload relevance and cost;
- prioritize S3-related and workload-relevant protections only when the
  lakehouse or later sandbox patterns justify them.

### 8. GuardDuty cost controls

Use explicit cost controls before any live enablement:

- estimate baseline cost with the AWS Pricing Calculator;
- use the GuardDuty usage/cost view after enablement to validate actual spend;
- keep optional protection plans off by default until there is a workload-driven
  reason to enable them;
- review Regional scope carefully because delegated administration and plan
  enablement are Regional.

Helpful framing:

- foundational GuardDuty gives the cleanest security baseline;
- optional protection plans are the main place where costs can expand quickly;
- therefore, default to targeted protection-plan adoption, not blanket adoption.

### 9. Security Hub decision

Decision: **defer broad Security Hub adoption for now**.

Reason:

- Security Hub becomes cleaner and easier to justify after AWS Config recorder
  scope and GuardDuty delegated administration are settled;
- Security Hub depends on AWS Config recording for many controls and can affect
  AWS Config recorder cost through `AWS::Config::ResourceCompliance`;
- the repo does not yet need a broad standards-and-findings aggregation layer
  badly enough to justify adopting it before the underlying Config and
  GuardDuty design is tighter.

Adoption trigger:

- revisit Security Hub after recorder scope, GuardDuty delegated-admin design,
  and baseline cost controls are approved;
- if adopted later, use the same delegated administrator across security
  services for cleaner governance;
- if adopted later, place it in `Security Tooling` rather than widening the log
  archive boundary or using `so-aws-admin`.

## Practical Design Summary

```text
Security Log Archive account
  ├─ CloudTrail log archive bucket + KMS key
  ├─ AWS Config archive bucket + KMS key
  └─ preferred storage-only audit boundary

Security Tooling account
  ├─ account-readiness contacts configured before service migration
  ├─ AWS Config aggregator in eu-west-2
  ├─ organization CloudTrail Config rule for recorder-bearing accounts
  ├─ GuardDuty delegated administrator in each enabled Region
  ├─ OAM / cross-account observability if adopted
  └─ preferred future security-service operations boundary

Management account
  ├─ retains Organizations control plane
  └─ owns explicit enablement decisions

Lakehouse workload account
  ├─ AWS Config recorder in active Region(s)
  ├─ foundational GuardDuty target coverage
  └─ optional protection plans only if workload relevance and cost justify them

Sandbox account
  ├─ AWS Config recorder only where in scope
  ├─ foundational GuardDuty target coverage
  └─ exclusions or reduced scope if churn makes Config cost disproportionate
```

## Alternatives Considered

| Option | Decision | Why |
|---|---|---|
| Put AWS Config aggregation and GuardDuty administration in a separate `Security Tooling` account while keeping `Security Log Archive` storage-only | Accepted target design | Best preserves separation of duties between write-mostly audit storage and active delegated security administration. AWS Config has now moved; GuardDuty remains a later adoption decision. |
| Keep AWS Config aggregation and GuardDuty administration permanently in the current `Security Log Archive` account | Rejected as the preferred target | Works as a transitional live boundary, but it couples audit storage and delegated security tooling too tightly as the environment grows. |
| Keep AWS Config aggregation permanently in the management account | Rejected as target | Simpler initially, but mixes control-plane duties with ongoing security visibility and weakens separation of duties. |
| Use the management account as the GuardDuty delegated administrator | Rejected | AWS documentation does not recommend this, and it weakens least-privilege operations. |
| Enable all GuardDuty protection plans by default | Rejected | Faster to turn on, but cost and relevance vary by workload; targeted adoption is more defensible. |
| Enable Security Hub now as part of this design step | Deferred | Useful later, but cleaner after Config and GuardDuty scope are settled and cost controls are understood. |
| Record every supported AWS Config resource in every possible Region | Rejected | Over-broad for the current lab footprint and likely to create avoidable cost and noise. |

## Open Implementation Work

This note does not complete implementation. The following remain open:

- keep `Security Log Archive` as the storage-only account while active AWS
  Config delegated administration and aggregation run from `Security Tooling`;
- keep the `Security Tooling` recorder and organization-rule inclusion stable
  after the 2026-07-07 bounded implementation;
- keep `so-aws-admin` excluded while read-only dependency checks and dependency
  resolution prepare it for retirement;
- implement GuardDuty delegated administration in `Security Tooling` under
  separate explicit approval if GuardDuty is adopted;
- evaluate OAM separately if centralized CloudWatch metrics, logs, and traces
  become a useful operational visibility requirement;
- adopt Security Hub only if later justified, and place it in `Security
  Tooling` rather than the archive account or `so-aws-admin`;

- use `docs/runbooks/domain-1-governance-live-readiness-runbook.md` to package
  the exact prechange evidence, blast radius, rollback, validation, cost, and
  approval boundary before any live execution;
- decide whether to codify `eu-west-2` as the permanent home Region for
  governance aggregation artifacts;
- choose the exact initial AWS Config resource exclusions, if any;
- define the starter AWS Config managed ruleset explicitly;
- keep the sandbox account in the same continuous-recording baseline for now
  because it is becoming a real container/microservices workload boundary, then
  revisit scoped exclusions only after cost observation;
- choose the exact GuardDuty Region set for first enablement;
- decide which optional GuardDuty protection plan, if any, is the first
  workload-justified candidate;
- document account-level budget thresholds for management, lakehouse workload,
  and sandbox governance services;
- revisit Security Hub after Config and GuardDuty scope is finalized.

## References

- Recording AWS resources with AWS Config:
  `https://docs.aws.amazon.com/config/latest/developerguide/select-resources.html`
- Working with the configuration recorder:
  `https://docs.aws.amazon.com/config/latest/developerguide/stop-start-recorder.html`
- Evaluating resources with AWS Config rules:
  `https://docs.aws.amazon.com/config/latest/developerguide/evaluate-config.html`
- Multi-account, multi-Region data aggregation for AWS Config:
  `https://docs.aws.amazon.com/config/latest/developerguide/aggregate-data.html`
- Security Hub CSPM and AWS Config recording cost considerations:
  `https://docs.aws.amazon.com/securityhub/latest/userguide/securityhub-setup-prereqs.html`
- Designating a delegated GuardDuty administrator:
  `https://docs.aws.amazon.com/guardduty/latest/ug/delegated-admin-designate.html`
- Managing GuardDuty accounts with AWS Organizations:
  `https://docs.aws.amazon.com/guardduty/latest/ug/guardduty_organizations.html`
- GuardDuty administrator/member relationships:
  `https://docs.aws.amazon.com/guardduty/latest/ug/administrator_member_relationships.html`
- Monitoring GuardDuty usage and estimating costs:
  `https://docs.aws.amazon.com/guardduty/latest/ug/monitoring_costs.html`
- Designating a delegated administrator in Security Hub:
  `https://docs.aws.amazon.com/securityhub/latest/userguide/securityhub-v2-set-da.html`
