# Domain 1 Governance Change Note - Enable SERVICE_CONTROL_POLICY For Root - 2026-06-22

<!-- markdownlint-disable MD013 -->

## Target Account And OU

- Management account: `349687196588` / `management-account-alias`
- Target root: `r-gbyf`
- Related workload OU unlocked by this change: `ou-gbyf-m6ppfmpq` /
  `Lakehouse Workloads OU`
- Account move in this change boundary: none
- SCP attachment in this change boundary: none

## Current State

Before this change:

- the shell was authenticated to the management account through
  `org-admin`;
- the organization described `SERVICE_CONTROL_POLICY` as available and
  `ENABLED` at the organization level;
- root `r-gbyf` still returned `PolicyTypes: []`;
- the earlier OU-targeted `DenyLeavingOrganization` attach attempt had already
  failed with `PolicyTypeNotEnabledException` and had been rolled back cleanly.

Evidence files:

- `docs/evidence/domain1-governance-enable-scp-root-sts-prechange-20260622.json`
- `docs/evidence/domain1-governance-enable-scp-root-org-description-prechange-20260622.json`
- `docs/evidence/domain1-governance-enable-scp-root-roots-prechange-20260622.json`
- `docs/evidence/domain1-governance-deny-leaving-org-attach-error-20260622.txt`
- `docs/evidence/domain1-governance-deny-leaving-organization-change-note-20260622.md`

## Proposed Change

Enable `SERVICE_CONTROL_POLICY` for root `r-gbyf`.

This change intentionally does **not**:

- create or attach a customer-managed SCP in the same boundary;
- move any account or OU;
- change IAM Identity Center assignments;
- change CloudTrail, Config, GuardDuty, Security Hub, or budgets.

## Expected Blast Radius

- Affects the organization-wide ability to attach SCPs to the root, OUs, and
  accounts under this root.
- Does not by itself deny any action.
- Creates the prerequisite needed for later OU-targeted and account-targeted
  SCP attachment.
- Changes governance capability at the root boundary, so it is broader than an
  OU-local policy attach even though it is still a small AWS Organizations
  control-plane change.

## Rollback Path

Disable the root policy type if a later review decides SCP capability should
not remain enabled:

```bash
aws organizations disable-policy-type \
  --profile org-admin \
  --root-id r-gbyf \
  --policy-type SERVICE_CONTROL_POLICY
```

Rollback precondition:

- any customer-managed SCP attachment created after this enablement must be
  detached first.

Validation of rollback:

- `list-roots` should no longer show `SERVICE_CONTROL_POLICY` in
  `PolicyTypes`;
- later OU-targeted SCP attach attempts should again be blocked until the
  policy type is re-enabled.

## Validation

Commands used:

```bash
aws sts get-caller-identity \
  --profile org-admin \
  --output json

aws organizations describe-organization \
  --profile org-admin \
  --output json

aws organizations list-roots \
  --profile org-admin \
  --output json

aws organizations enable-policy-type \
  --profile org-admin \
  --root-id r-gbyf \
  --policy-type SERVICE_CONTROL_POLICY

aws organizations list-roots \
  --profile org-admin \
  --output json

aws organizations list-policies-for-target \
  --profile org-admin \
  --target-id r-gbyf \
  --filter SERVICE_CONTROL_POLICY \
  --output json
```

Observed nuance:

- the immediate `enable-policy-type` response showed
  `SERVICE_CONTROL_POLICY` as `PENDING_ENABLE`;
- a follow-up `list-roots` check then showed the status as `ENABLED`.

Success criteria:

- `list-roots` shows `SERVICE_CONTROL_POLICY` under root `r-gbyf`;
- the status becomes `ENABLED`, not just `PENDING_ENABLE`;
- the change unblocks later OU-targeted SCP attachment work.

## Cost Impact

- No direct AWS service charge is expected from enabling the root policy type.
- Governance capability increases because SCP attachment is now available at
  the root and OU hierarchy.

## Approval

- Approval source: direct user instruction
- Approval text: `Proceed with next live step: enable SERVICE_CONTROL_POLICY for root r-gbyf, validate that the root exposes the enabled policy type, then retry the OU-targeted DenyLeavingOrganization attach.`
- Approval date: 2026-06-22
- Scope of approval: enable `SERVICE_CONTROL_POLICY` for root `r-gbyf` as the
  prerequisite change unit

## Result

The root policy type enablement succeeded.

Post-change evidence:

- `docs/evidence/domain1-governance-enable-scp-root-roots-postchange-20260622.json`
- `docs/evidence/domain1-governance-enable-scp-root-policies-postchange-20260622.json`

Current interpretation:

- root `r-gbyf` now exposes `SERVICE_CONTROL_POLICY` as `ENABLED`;
- the earlier `PolicyTypeNotEnabledException` blocker is resolved;
- the repo can now support narrow OU-targeted SCP attachment under separate
  execution evidence.
