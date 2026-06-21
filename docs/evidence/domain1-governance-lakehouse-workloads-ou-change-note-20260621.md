# Domain 1 Governance Change Note - Create Lakehouse Workloads OU - 2026-06-21

<!-- markdownlint-disable MD013 -->

## Target Account And OU

- Management account: `349687196588` / `management-account-alias`
- Source parent: root `r-gbyf`
- OU created in this change: `Lakehouse Workloads OU`
- New OU ID: `ou-gbyf-m6ppfmpq`
- Account move in this change boundary: none

## Current State

Before this change:

- the root OU list contained only `Container Sandbox`
  (`ou-gbyf-zs0f26b5`);
- `lakehouse-workload-account` (`464975959576`) was attached directly to root `r-gbyf`;
- organization inventory and parent mapping were already captured read-only.

Evidence files:

- `docs/evidence/domain1-governance-sts-prechange-20260621.json`
- `docs/evidence/domain1-governance-org-description-prechange-20260621.json`
- `docs/evidence/domain1-governance-org-root-ous-prechange-20260621.json`
- `docs/evidence/domain1-governance-org-parents-lakehouse-prechange-20260621.json`
- `docs/evidence/domain1-governance-org-inventory-summary-20260621.md`

## Proposed Change

Create one new root-level OU named `Lakehouse Workloads OU`.

This change intentionally does **not** move `lakehouse-workload-account` yet. The account move
stays as a separate approval boundary.

## Expected Blast Radius

- Affects AWS Organizations structure in the management account only.
- Does not change SCP attachments.
- Does not change IAM Identity Center assignments.
- Does not move any account.
- Does not affect the running lakehouse workload directly.

## Rollback Path

If no account has been moved into the OU and no later dependency is created,
delete the empty OU:

```bash
aws organizations delete-organizational-unit \
  --profile org-admin \
  --organizational-unit-id ou-gbyf-m6ppfmpq
```

Validation of rollback:

- `describe-organizational-unit` for `ou-gbyf-m6ppfmpq` should fail;
- `list-organizational-units-for-parent --parent-id r-gbyf` should no longer
  return `Lakehouse Workloads OU`.

## Validation

Commands used:

```bash
aws organizations create-organizational-unit \
  --profile org-admin \
  --parent-id r-gbyf \
  --name 'Lakehouse Workloads OU' \
  --output json

aws organizations describe-organizational-unit \
  --profile org-admin \
  --organizational-unit-id ou-gbyf-m6ppfmpq \
  --output json

aws organizations list-organizational-units-for-parent \
  --profile org-admin \
  --parent-id r-gbyf \
  --output json

aws organizations list-parents \
  --profile org-admin \
  --child-id 464975959576 \
  --output json
```

Success criteria:

- `Lakehouse Workloads OU` exists under root `r-gbyf`;
- the new OU ID is stable and queryable;
- `lakehouse-workload-account` remains attached to root after this change;
- `Container Sandbox` still exists unchanged.

## Cost Impact

- No direct AWS service charge is expected from OU creation itself.
- Minor governance/administrative complexity increases because an additional OU
  now exists and must be kept aligned with later SCP and account-placement work.

## Approval

- Approval source: direct user instruction
- Approval text: `Good, move forward with Lakehouse Workloads OU`
- Approval date: 2026-06-21
- Scope of approval: create the OU only; do not move `lakehouse-workload-account` in the same
  change boundary

## Result

The OU creation succeeded.

Post-change evidence:

- `docs/evidence/domain1-governance-lakehouse-workloads-ou-create-20260621.json`
- `docs/evidence/domain1-governance-lakehouse-workloads-ou-postchange-20260621.json`
- `docs/evidence/domain1-governance-lakehouse-parent-post-ou-create-20260621.json`

Current interpretation:

- `Lakehouse Workloads OU` now exists live;
- `lakehouse-workload-account` is still attached to root `r-gbyf`;
- the next live governance step, if later approved, is the separate account-move
  change note for moving `lakehouse-workload-account` into `ou-gbyf-m6ppfmpq`.
