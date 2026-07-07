# ADR 0005: AWS Organizations Governance Design

<!-- markdownlint-disable MD013 -->

- Status: Accepted for design; live implementation occurs only through separate approved change notes
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
|   |-- Log Archive account
|   `-- Security Tooling account
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

For the longer-term security boundary, prefer two separate security accounts
over one permanently combined security/log archive account:

- a write-mostly `Log Archive` account that owns the organization CloudTrail
  archive bucket, AWS Config archive bucket, the related KMS keys, and
  retention or delete-protection controls; and
- a separate `Security Tooling` account that owns delegated-administrator and
  security-operations functions such as the AWS Config aggregator, GuardDuty
  delegated administration, OAM/cross-account observability, possible later
  Security Hub administration, and read-only investigation tooling.

Reason:

- it separates tamper-resistant audit storage from day-to-day security-service
  administration;
- it keeps the archive account quieter, narrower, and easier to protect with
  write-mostly access assumptions;
- it gives GuardDuty, Security Hub, Config aggregation, and future automation a
  cleaner delegated-administration home without mixing them into evidence
  storage;
- it maps more cleanly to the SAP-C02 mental model of centralized logging,
  delegated security tooling, and separation of duties;
- it reduces future policy coupling, because archive-bucket protection and
  security-operations permissions can evolve independently.

The earlier live `Security Log Archive` AWS Config delegated-admin placement was
a valid transitional implementation boundary for this lab. It kept the first
CloudTrail and Config rollout small. As of 2026-07-06, AWS Config delegated
administration and organization aggregation have moved to `Security Tooling`,
while `Security Log Archive` remains the storage-only boundary.

Accepted transition and sequencing:

- the root-user-emergency-SCP closure completed before this split moved forward;
- a separate `Security Tooling` account now exists in `Security OU`;
- keep the existing `Security Log Archive` account as the storage-only
  boundary for central audit buckets, KMS keys, and related retention or
  delete-protection controls;
- migrate delegated-administrator and security-operations functions in this
  order: AWS Config first, GuardDuty next, and Security Hub only if it is later
  intentionally adopted. AWS Config migration is complete; GuardDuty delegated
  administration and foundational coverage are live in `Security Tooling` for
  `eu-west-2`;
- treat OAM as a later `Security Tooling` or central monitoring concern, not as
  part of the storage-only log archive boundary.

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
| `SecurityAudit` | Management, workload, log archive, and security tooling accounts | Read-only security, logging, and audit visibility. |
| `LakehouseOperator` | Lakehouse workload account | Operate lakehouse workload services without organization administration. |
| `LakehouseReadOnly` | Lakehouse workload account | Review runtime posture and evidence without write access. |
| `BreakGlassAdmin` | As required | Emergency recovery only, with MFA, owner, logging, and review. |

Use SCPs as guardrails, not as permission grants. IAM policies and permission
sets grant access; SCPs define maximum allowed permissions for accounts or OUs.

Adopt this initial SCP catalogue as design intent only:

| SCP | Target | Design intent |
| --- | --- | --- |
| Deny disabling CloudTrail | Workloads, Sandbox, future Security OU | Protect audit evidence once an organization trail exists. |
| Deny deleting central log buckets | Log Archive account and member accounts | Protect central logs after log archive naming and ownership are finalized. |
| Deny public S3 exposure by default | Workloads and Sandbox | Reduce data leakage while allowing explicitly approved public dashboard patterns. |
| Deny unapproved Regions | Workloads and Sandbox | Limit cost and operational spread, with global-service and `eu-west-2` exceptions. |
| Deny root-user actions except emergencies | Member accounts | Reduce blast radius after break-glass procedure is documented. |
| Require encryption where feasible | Workloads and Sandbox | Improve baseline posture while accounting for service-specific condition-key support. |
| Deny leaving the AWS Organization | Member accounts | Prevent governance bypass after management-account recovery is documented. |

Design organization logging as:

- one organization CloudTrail trail with management events and log-file
  validation;
- central log archive ownership in a dedicated `Log Archive` account;
- AWS Config organization aggregation in the separate `Security Tooling`
  account, with the `Security Tooling` recorder now live in `eu-west-2` and
  `so-aws-admin` excluded on the decommission path;
- GuardDuty delegated administration and foundational coverage in the
  `Security Tooling` account, with no additional GuardDuty account;
- OAM as a later cross-account observability option in the `Security Tooling`
  or central monitoring boundary;
- Security Hub as a later standards and finding-aggregation layer if the cost
  and study value justify it, also anchored in the `Security Tooling` account
  if adopted.

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
| Use one permanently combined security/log archive account for both archive storage and delegated security tooling | Rejected as the preferred target state | Simpler for first implementation, and acceptable as a temporary lab step, but it mixes write-mostly audit storage with active delegated-administrator and security-operations duties. |
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

Splitting the security boundary into a `Log Archive` account and a `Security
Tooling` account adds one more account and later migration work, but it is a
worthwhile trade-off for the target state. The cleaner split keeps audit
storage write-mostly and easier to harden, while delegated administration,
findings aggregation, and future response automation can evolve separately
without broadening archive-account permissions.

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
- separating write-mostly log retention from delegated security tooling can be
  the cleaner answer when tamper resistance and operational separation both
  matter;
- OAM, CloudTrail log archive, and AWS Config aggregation answer different
  exam prompts: live operational visibility, API audit evidence, and
  configuration/compliance posture respectively;
- security services need delegated-administrator, aggregation, region, and cost
  decisions;
- cost-allocation tags are useful only when tagging standards and billing
  activation are aligned.

The ADR also supports Domain 3 because it improves an existing architecture by
adding governance boundaries, operational control points, and implementation
gates without destabilizing the working lakehouse.

## Revision Notes From Live Rollout

The separate change notes remain the source of detailed operational evidence,
but several condensed live lessons are now worth carrying directly in this ADR
for later SAP-C02 revision:

- organization AWS Config rules deployed from a delegated administrator depend
  on `AWSServiceRoleForConfigMultiAccountSetup` not only in the delegated-admin
  account, but also in the organization management account if that management
  account is itself part of the in-scope deployment set;
- an organization AWS Config rule does not deploy cleanly into an account that
  lacks an in-scope AWS Config recorder; in this repo, the first rule attempt
  exposed that exact gap in sandbox account `974893866311`;
- once that sandbox account became an intended container and microservices
  workload boundary, the correct fix was to give it its own recorder baseline
  rather than leave it permanently excluded from governance coverage;
- when sandbox scope is intentionally left open for later cost observation, it
  is acceptable to exclude that sandbox account from the first organization
  rule rather than widening the current change boundary into full sandbox
  recorder rollout;
- once the excluded account has a working recorder baseline and the central
  Config archive path is authorized for that account, the correct follow-on is
  to remove the exclusion from the same organization rule rather than leave a
  lingering governance gap or create a duplicate account-local rule;
- adding a new account to the central AWS Config archive path also requires the
  central S3 bucket policy and Config KMS key policy to be extended for that
  account before delivery-channel creation can succeed;
- using the same account for archive storage and delegated Config operations was
  a reasonable early implementation shortcut, but it also made the longer-term
  two-account target easier to justify: archive storage should stay narrow and
  write-mostly, while delegated administration can expand separately in a
  dedicated `Security Tooling` account;
- for this repository, `MULTI_REGION_CLOUD_TRAIL_ENABLED` was a better first
  detective control than `CLOUD_TRAIL_ENABLED` because the accepted CloudTrail
  design already centers on one organization multi-Region trail with
  management events and a central archive bucket;
- organization-managed AWS Config rules materialize as local
  `OrgConfigRule-*` resources in each in-scope account, so both organization
  deployment status and local account compliance views matter during
  validation;
- the clean rollout path was:
  recorder baseline first, then one narrow organization rule, then blocker
  resolution, then retry, rather than starting with multiple rule families at
  once.
- the GuardDuty rollout followed the same narrow-boundary pattern: enable the
  delegated administrator in `Security Tooling`, enroll only approved active
  accounts, keep `so-aws-admin` excluded, and leave optional protection plans
  disabled until a separate cost/value decision exists.

These live lessons reinforce a recurring exam pattern: the correct answer is
often not just "use Organizations" or "use Config," but "sequence delegated
administration, recorder coverage, service-linked-role prerequisites, and
scope boundaries in the right order."

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
- The combined `Security Log Archive` live boundary was split through separate
  account-boundary and delegated-administration migration notes rather than by
  silently rewriting earlier evidence.
- The AWS Config recorder-scope decision is now recorded in
  `docs/planning/domain-1-config-recorder-scope-decision-20260706.md`:
  `Security Tooling` recorder onboarding was implemented on 2026-07-07, while
  `so-aws-admin` remains excluded on the decommission path.
- The `so-aws-admin` account-purpose decision is now recorded in
  `docs/planning/domain-1-so-aws-admin-decommission-decision-20260706.md`:
  retire it only after read-only dependency checks and dependency resolution,
  and keep Security Hub in `Security Tooling` if adopted.
- GuardDuty delegated-admin planning is now recorded in
  `docs/planning/domain-1-guardduty-delegated-admin-planning-20260706.md`:
  use `Security Tooling` as the delegated administrator; do not create another
  account. The live delegated-admin implementation is recorded in
  `docs/evidence/domain1-governance-guardduty-delegated-admin-change-note-20260707.md`.
- The external governance study note
  `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/SAP-C02_Security_Observability_Comparison.md`
  records the OAM vs CloudTrail log archive vs AWS Config aggregator exam
  distinction that informed this split.
- The first live organization AWS Config CloudTrail rule, the sandbox-recorder
  deployment gap, the management-account multi-account-setup service-linked
  role blocker, the sandbox recorder follow-on, and the final successful
  four-account rollout are now recorded in
  `docs/evidence/domain1-governance-config-org-cloudtrail-rule-change-note-20260625.md`.
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
chooses to split the current combined security boundary into separate `Log
Archive` and `Security Tooling` accounts, container work is unparked, or live
governance changes reveal service exceptions that materially change the OU or
SCP model.
