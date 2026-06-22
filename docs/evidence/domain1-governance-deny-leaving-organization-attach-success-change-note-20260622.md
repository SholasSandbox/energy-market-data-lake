# Domain 1 Governance Change Note - Attach DenyLeavingOrganization SCP To Lakehouse Workloads OU After Root Enablement - 2026-06-22

<!-- markdownlint-disable MD013 -->

## Target Account And OU

- Management account: `349687196588` / `management-account-alias`
- Target OU: `ou-gbyf-m6ppfmpq` / `Lakehouse Workloads OU`
- Current in-scope member account: `464975959576` / `lakehouse-workload-account`
- Customer-managed SCP created in this change:
  `p-4stxl0u2` / `DenyLeavingOrganization-LakehouseWorkloads`

## Current State

Before this retry:

- root `r-gbyf` already exposed `SERVICE_CONTROL_POLICY` as `ENABLED`;
- `Lakehouse Workloads OU` still had no customer-managed SCP attached;
- the earlier attach attempt had been rolled back cleanly and left no custom
  SCP attached to the OU.

Evidence files:

- `docs/evidence/domain1-governance-enable-scp-root-change-note-20260622.md`
- `docs/evidence/domain1-governance-enable-scp-root-roots-postchange-20260622.json`
- `docs/evidence/domain1-governance-deny-leaving-org-lakehouse-ou-policies-preretry-20260622.json`
- `docs/evidence/domain1-governance-deny-leaving-org-policies-preretry-20260622.json`
- `docs/evidence/domain1-governance-deny-leaving-organization-change-note-20260622.md`

## Proposed Change

Create one customer-managed SCP from
`docs/policies/scp/deny-leaving-organization.example.json` and attach it only
to `ou-gbyf-m6ppfmpq` / `Lakehouse Workloads OU`.

Policy intent:

- deny `organizations:LeaveOrganization`;
- apply that deny to the current lakehouse workload account and any later
  member account placed in the same OU;
- leave all broader SCP catalogue items out of scope for this change unit.

## Expected Blast Radius

- Affects only accounts under `Lakehouse Workloads OU`.
- Prevents those member accounts from leaving the AWS Organization while the
  SCP remains attached.
- Does not affect the management account.
- Does not directly change running lakehouse services.
- Changes future account-separation behavior for this OU until the SCP is
  detached.

## Rollback Path

Detach the policy from the OU:

```bash
aws organizations detach-policy \
  --profile org-admin \
  --policy-id p-4stxl0u2 \
  --target-id ou-gbyf-m6ppfmpq
```

If this SCP is no longer wanted after detaching, delete it:

```bash
aws organizations delete-policy \
  --profile org-admin \
  --policy-id p-4stxl0u2
```

Validation of rollback:

- `list-targets-for-policy --policy-id p-4stxl0u2` should no longer include
  `ou-gbyf-m6ppfmpq`;
- `list-policies-for-target --target-id ou-gbyf-m6ppfmpq` should no longer show
  `p-4stxl0u2`;
- `lakehouse-workload-account` should remain in `Lakehouse Workloads OU`.

## Validation

Commands used:

```bash
aws organizations list-policies-for-target \
  --profile org-admin \
  --target-id ou-gbyf-m6ppfmpq \
  --filter SERVICE_CONTROL_POLICY \
  --output json

aws organizations create-policy \
  --profile org-admin \
  --content file://docs/policies/scp/deny-leaving-organization.example.json \
  --name DenyLeavingOrganization-LakehouseWorkloads \
  --description "Prevent member accounts in Lakehouse Workloads OU from leaving the organization" \
  --type SERVICE_CONTROL_POLICY

aws organizations attach-policy \
  --profile org-admin \
  --policy-id p-4stxl0u2 \
  --target-id ou-gbyf-m6ppfmpq

aws organizations describe-policy \
  --profile org-admin \
  --policy-id p-4stxl0u2 \
  --output json

aws organizations list-targets-for-policy \
  --profile org-admin \
  --policy-id p-4stxl0u2 \
  --output json

aws organizations list-policies-for-target \
  --profile org-admin \
  --target-id ou-gbyf-m6ppfmpq \
  --filter SERVICE_CONTROL_POLICY \
  --output json

aws organizations list-accounts-for-parent \
  --profile org-admin \
  --parent-id ou-gbyf-m6ppfmpq \
  --output json
```

Success criteria:

- the policy document remains the intended single-action deny;
- the policy attaches to `ou-gbyf-m6ppfmpq` without error;
- `list-targets-for-policy` shows only `Lakehouse Workloads OU`;
- `list-policies-for-target` for the workload OU shows both
  `DenyLeavingOrganization-LakehouseWorkloads` and inherited
  `FullAWSAccess`.

## Cost Impact

- No direct AWS service charge is expected from creating and attaching this
  narrow SCP.
- Small governance overhead increases because future OU/account-separation work
  must account for the new guardrail.

## Approval

- Approval source: direct user instruction
- Approval text: `Proceed with next live step: enable SERVICE_CONTROL_POLICY for root r-gbyf, validate that the root exposes the enabled policy type, then retry the OU-targeted DenyLeavingOrganization attach.`
- Approval date: 2026-06-22
- Scope of approval: retry the OU-targeted `DenyLeavingOrganization` attach
  after root enablement is validated

## Result

The OU-targeted SCP attachment succeeded.

Post-change evidence:

- `docs/evidence/domain1-governance-deny-leaving-org-policy-id-retry-20260622.txt`
- `docs/evidence/domain1-governance-deny-leaving-org-policy-postretry-20260622.json`
- `docs/evidence/domain1-governance-deny-leaving-org-policy-targets-postretry-20260622.json`
- `docs/evidence/domain1-governance-deny-leaving-org-lakehouse-ou-policies-postretry-20260622.json`
- `docs/evidence/domain1-governance-deny-leaving-org-lakehouse-ou-accounts-postretry-20260622.json`

Current interpretation:

- `DenyLeavingOrganization-LakehouseWorkloads` is now attached to
  `Lakehouse Workloads OU`;
- the first live OU-targeted SCP guardrail is now in place for the lakehouse
  workload boundary;
- the next SCP decision can stay narrow and intentional rather than jumping
  immediately to broader Region or S3 restrictions.
