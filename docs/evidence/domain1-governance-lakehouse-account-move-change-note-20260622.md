# Domain 1 Governance Change Note - Move lakehouse-workload-account To Lakehouse Workloads OU - 2026-06-22

<!-- markdownlint-disable MD013 -->

## Target Account And OU

- Management account: `349687196588` / `management-account-alias`
- Account moved: `464975959576` / `lakehouse-workload-account`
- Source parent: root `r-gbyf`
- Destination parent: `ou-gbyf-m6ppfmpq` / `Lakehouse Workloads OU`

## Current State

Before this change:

- `Lakehouse Workloads OU` already existed as `ou-gbyf-m6ppfmpq`;
- `lakehouse-workload-account` was still attached directly to root `r-gbyf`;
- the workload OU contained no accounts;
- the root still contained both the management account and `lakehouse-workload-account`.

Evidence files:

- `docs/evidence/domain1-governance-lakehouse-workloads-ou-change-note-20260621.md`
- `docs/evidence/domain1-governance-lakehouse-parent-pre-move-20260622.json`
- `docs/evidence/domain1-governance-root-accounts-pre-move-20260622.json`
- `docs/evidence/domain1-governance-lakehouse-workloads-ou-accounts-pre-move-20260622.json`

## Proposed Change

Move `lakehouse-workload-account` from root `r-gbyf` into
`ou-gbyf-m6ppfmpq` / `Lakehouse Workloads OU`.

This change intentionally does **not** attach any SCP and does **not** change
IAM Identity Center assignments. It only aligns account placement with the
accepted governance design.

## Expected Blast Radius

- Affects AWS Organizations account placement for one member account.
- Does not change the running lakehouse resources inside the account directly.
- Does not by itself change permissions unless a policy was already attached to
  the destination OU.
- Changes the future attachment point for OU-targeted SCPs and governance
  controls.

## Rollback Path

Move the account back to root if a later validation or operational concern
requires it:

```bash
aws organizations move-account \
  --profile org-admin \
  --account-id 464975959576 \
  --source-parent-id ou-gbyf-m6ppfmpq \
  --destination-parent-id r-gbyf
```

Validation of rollback:

- `list-parents --child-id 464975959576` should return root `r-gbyf`;
- `list-accounts-for-parent --parent-id r-gbyf` should include `lakehouse-workload-account`;
- `list-accounts-for-parent --parent-id ou-gbyf-m6ppfmpq` should no longer
  include `lakehouse-workload-account`.

## Validation

Commands used:

```bash
aws organizations move-account \
  --profile org-admin \
  --account-id 464975959576 \
  --source-parent-id r-gbyf \
  --destination-parent-id ou-gbyf-m6ppfmpq

aws organizations list-parents \
  --profile org-admin \
  --child-id 464975959576 \
  --output json

aws organizations list-accounts-for-parent \
  --profile org-admin \
  --parent-id r-gbyf \
  --output json

aws organizations list-accounts-for-parent \
  --profile org-admin \
  --parent-id ou-gbyf-m6ppfmpq \
  --output json
```

Observed nuance:

- the `move-account` command returned successfully with no CLI output;
- the first immediate read-after-write checks still showed the old root parent;
- after a short retry delay, the parent and account-list checks reflected the
  new OU placement.

Success criteria:

- `list-parents` shows `lakehouse-workload-account` under `ou-gbyf-m6ppfmpq`;
- root `r-gbyf` no longer lists `lakehouse-workload-account`;
- `Lakehouse Workloads OU` lists `lakehouse-workload-account`;
- `Container Sandbox` remains unchanged.

## Cost Impact

- No direct AWS service charge is expected from moving the account.
- Governance scope becomes clearer because the lakehouse account now has its own
  workload OU attachment point for future guardrails.

## Approval

- Approval source: direct user instruction
- Approval text: `I want it`
- Approval date: 2026-06-22
- Scope of approval: move `lakehouse-workload-account` into `ou-gbyf-m6ppfmpq` as a separate
  live change boundary

## Result

The account move succeeded after a short Organizations propagation delay.

Post-change evidence:

- `docs/evidence/domain1-governance-lakehouse-parent-post-move-20260622.json`
- `docs/evidence/domain1-governance-root-accounts-post-move-20260622.json`
- `docs/evidence/domain1-governance-lakehouse-workloads-ou-accounts-post-move-20260622.json`

Current interpretation:

- `lakehouse-workload-account` is now attached to `Lakehouse Workloads OU`;
- the organization now has distinct live workload and sandbox OU boundaries;
- the next governance step, if later approved, is likely OU-targeted SCP
  design/attachment or later security OU/account work rather than more account
  placement cleanup.
