# Domain 1 OU And Account Placement Decision - 2026-06-21

<!-- markdownlint-disable MD013 -->

## Scope

This note records the repo-only current-to-target OU and account-placement
decision for the Energy Data Lakehouse governance phase.

It aligns with:

- `docs/adr/0005-aws-organizations-governance-design.md`
- `docs/planning/domain-1-governance-preflight-20260618.md`
- `docs/evidence/domain1-governance-org-inventory-summary-20260621.md`
- `docs/planning/sap-c02-readiness-tracker.md`

This note does not authorize live OU creation, account moves, SCP attachment, or
any other AWS Organizations change.

## Confirmed Alignment

This note supports the tracker because it advances:

1. SAP-C02 Domain 1 reasoning around management-account boundaries, OU purpose,
   account isolation, and SCP scope.
2. The Energy Data Lakehouse case study as a governed workload in a
   multi-account organization.
3. Near-term cloud architect positioning through explicit current-to-target
   placement reasoning rather than vague landing-zone terminology.

## Current Observed Organization State

Based on the recorded inventory evidence:

- the organization has one root: `r-gbyf`;
- the management account is `349687196588` / `management-account-alias`;
- the lakehouse workload account is `464975959576` / `lakehouse-workload-account`;
- the separate sandbox/container-lab account is `974893866311` /
  `containers-lab.com`;
- trusted service access currently evidenced at the organization level is
  `sso.amazonaws.com`;
- the current root OU list contains two OUs:
  `Lakehouse Workloads OU` (`ou-gbyf-m6ppfmpq`) and
  `Container Sandbox` (`ou-gbyf-zs0f26b5`).

Evidence:

- `docs/evidence/domain1-governance-org-roots-prechange-20260621.json`
- `docs/evidence/domain1-governance-org-accounts-prechange-20260621.json`
- `docs/evidence/domain1-governance-org-service-access-prechange-20260621.json`
- `docs/evidence/domain1-governance-org-parents-management-prechange-20260621.json`
- `docs/evidence/domain1-governance-org-parents-lakehouse-prechange-20260621.json`
- `docs/evidence/domain1-governance-org-parents-containers-prechange-20260621.json`
- `docs/evidence/domain1-governance-org-root-ous-prechange-20260621.json`
- `docs/evidence/domain1-governance-lakehouse-workloads-ou-change-note-20260621.md`
- `docs/evidence/domain1-governance-lakehouse-account-move-change-note-20260622.md`

Current observed parent mapping:

- `management-account-alias` is attached directly to root `r-gbyf`
- `lakehouse-workload-account` is now attached to `ou-gbyf-m6ppfmpq` / `Lakehouse Workloads OU`
- `Lakehouse Workloads OU` now exists as `ou-gbyf-m6ppfmpq`
- `containers-lab.com` is already attached to OU
  `ou-gbyf-zs0f26b5` / `Container Sandbox`

Important limitation:

- this note now proves the current parent mapping needed for repo-only planning;
- the target workload OU now exists live and the lakehouse account is already
  attached to it;
- any later live placement change still needs its own approved change note with
  source parent, destination parent, rollback, and validation details.

## Placement Decision

### 1. Management account

Keep `management-account-alias` as the control-plane account for:

- AWS Organizations administration
- billing and cost administration
- IAM Identity Center administration
- SCP administration

Do not treat it as the lakehouse runtime account.

### 2. Lakehouse workload account

Target `lakehouse-workload-account` for the existing `Lakehouse Workloads OU`
(`ou-gbyf-m6ppfmpq`).

Reason:

- it is the account that owns the Energy Data Lakehouse runtime and evidence;
- it is the clearest target for workload-oriented guardrails later;
- separating it from sandbox study work improves blast-radius reasoning and
  portfolio clarity;
- the more descriptive `Lakehouse Workloads OU` name is clearer than the generic
  `Workloads OU` for the current three-account organization;
- the OU and the lakehouse account move are now both live, so the next
  implementation steps can focus on OU-targeted governance controls rather than
  further workload-boundary setup.

### 3. Container-lab account

Keep `containers-lab.com` in the existing sandbox-oriented OU for now.

Reason:

- it remains separate study scope rather than lakehouse implementation;
- sandbox placement keeps experimentation distinct from the main case-study
  workload;
- later restrictive SCPs can be evaluated differently there if needed;
- the current live organization already has a sandbox-shaped OU for this
  account, so there is no immediate design reason to move it again before
  higher-priority workload and security boundaries are settled.

Naming note:

- the live OU is currently named `Container Sandbox`;
- that is acceptable as the interim sandbox boundary;
- a later rename or normalization to a broader `Sandbox` name is optional, not
  required for the current governance-readiness path.

### 4. Future security/log archive account

Reserve the `Security OU` for a future security/log archive account, but leave
it empty for now.

Reason:

- the security/log archive boundary is already accepted in the governance ADR;
- CloudTrail, AWS Config, GuardDuty, and possible Security Hub design now point
  toward that future account boundary;
- creating the account before the logging/security implementation boundary is
  approved would be premature.

### 5. Suspended OU

Reserve the `Suspended OU` for quarantine or account-closure workflows, but
leave it empty for now.

Reason:

- it provides a clean future lifecycle boundary;
- it demonstrates SAP-C02-relevant account-governance thinking without forcing
  unnecessary live structure today.

## Trade-Off Summary

This decision keeps the organization small but intentionally shaped.

Accepted trade-offs:

- use a compact OU model instead of leaving all member accounts under the root,
  because root-only placement is simpler but weaker for scoped governance and
  SCP reasoning;
- keep separate `Workloads` and `Sandbox` placement, because combining the two
  would reduce operational overhead but blur the lakehouse case study with
  unrelated lab work;
- use `Lakehouse Workloads OU` instead of the more generic `Workloads OU`,
  because the extra specificity improves clarity now and still leaves room to
  revisit the name later if the organization grows beyond the lakehouse-focused
  workload boundary;
- accept the existing `Container Sandbox` OU as a good-enough interim sandbox
  boundary instead of prioritizing OU renaming now, because a cleaner generic
  OU name would be slightly nicer but does not unlock as much value as creating
  the missing workload and security boundaries later;
- reserve `Security` and `Suspended` OUs before they have live accounts, because
  that adds a little conceptual overhead now but creates a cleaner path for
  logging, audit, and lifecycle controls later;
- avoid a larger enterprise taxonomy such as `Dev`, `Test`, `Prod`, `Network`,
  and `Shared Services`, because it would look more enterprise-like but is not
  justified by the current three-account footprint.

## Implementation Boundary

No live change is approved by this note.

Before any live OU creation or account-move request:

1. capture current parent mapping with `list-parents` or equivalent;
2. identify the exact source parent and destination parent IDs;
3. record rollback and validation steps in a separate live change note;
4. confirm the target OU design still matches ADR 0005 and the tracker.

## Practical Next Use

This note unlocks the next governance preparation slices:

1. a bounded OU-targeted SCP or guardrail change note if explicit approval is
   later granted for the lakehouse account boundary;
2. later SCP target mapping by OU once policy choices and exceptions are ready;
3. a security/log archive account and OU implementation decision when later
   governance sequencing reaches that boundary;
4. a later optional naming cleanup only if `Container Sandbox` becomes too
   narrow for the intended sandbox scope.
