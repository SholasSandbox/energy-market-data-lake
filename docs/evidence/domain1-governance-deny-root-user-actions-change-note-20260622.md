# Domain 1 Governance Change Note - Attach Root-User Emergency-Only SCP To Lakehouse Workloads OU - 2026-06-22

<!-- markdownlint-disable MD013 -->

## Status

Prepared as the next bounded SCP change candidate.

Live attachment was **not** executed from this note because the required
break-glass live-readiness checks are not yet fully evidenced.

## Target Account And OU

- Management account: `349687196588` / `management-account-alias`
- Target OU: `ou-gbyf-m6ppfmpq` / `Lakehouse Workloads OU`
- Current in-scope member account: `464975959576` / `lakehouse-workload-account`
- SCP candidate file:
  `docs/policies/scp/deny-root-user-actions.example.json`

## Current State

Fresh 2026-06-22 read-only evidence shows:

- the shell is authenticated to the management account through `org-admin`;
- root `r-gbyf` already exposes `SERVICE_CONTROL_POLICY` as `ENABLED`;
- `Lakehouse Workloads OU` currently has `FullAWSAccess` and
  `DenyLeavingOrganization-LakehouseWorkloads` attached;
- `lakehouse-workload-account` remains the only account in `Lakehouse Workloads OU`.

Break-glass design state:

- the break-glass procedure exists and defines the target emergency model;
- the live values are now defined as:
  target emergency owner identity `[redacted-email]`,
  current active emergency contact mailbox
  `[redacted-email]`,
  retirement of `[redacted-email]` from the active contact path,
  emergency email notifications now centered on the emergency-owner mailbox,
  emergency SMS to `[redacted-phone]`,
  Google Password Manager as the primary cross-platform recovery store,
  an out-of-band backup requirement outside the repository and the primary
  machine path,
  the evidence location
  `docs/evidence/domain1-governance-break-glass-usage-YYYYMMDD.md`,
  workload-account-first scope for the current OU-targeted guardrail,
  and a defined post-use reduction path;
- follow-on 2026-06-25 same-day IAM Identity Center evidence now clarifies
  that the current live management-account admin principal is
  `org-admin-principal` / `[redacted-email]`, that a
  dedicated break-glass IAM Identity Center user now exists as
  `breakglass-principal` / `[redacted-email]` with one enrolled
  MFA device, and that a dedicated `BreakGlassAdmin` permission set now exists
  with a direct management-account assignment for that user;
- the management account is not the SCP target for this OU-level guardrail, but
  it remains in the Organizations attach/detach rollback path unless explicit
  delegated Organizations policy management is later implemented;
- the remaining live-readiness checks are now narrower:
  confirm root-user and management-account-path MFA evidence,
  generate and store the out-of-band backup material,
  confirm notification reachability,
  and record a light procedural validation;
- the SCP policy README also states that
  `deny-root-user-actions.example.json` should be attached only after the
  emergency root-use process is documented and tested.

Evidence:

- `docs/evidence/domain1-governance-deny-root-user-actions-sts-prechange-20260622.json`
- `docs/evidence/domain1-governance-deny-root-user-actions-roots-prechange-20260622.json`
- `docs/evidence/domain1-governance-deny-root-user-actions-lakehouse-ou-policies-prechange-20260622.json`
- `docs/evidence/domain1-governance-deny-root-user-actions-lakehouse-ou-accounts-prechange-20260622.json`
- `docs/evidence/domain1-governance-identity-center-current-state-20260625.md`
- `docs/runbooks/break-glass-access-procedure.md`
- `docs/policies/scp/README.md`

## Proposed Change

Create one customer-managed SCP from
`docs/policies/scp/deny-root-user-actions.example.json` and attach it only to
`ou-gbyf-m6ppfmpq` / `Lakehouse Workloads OU`.

Policy intent:

- deny most root-user actions for member accounts in the OU;
- leave a narrow set of account-recovery and MFA-management actions available
  to the root principal;
- reduce root-user blast radius while preserving last-resort recovery paths.

## Expected Blast Radius

- Affects only root principals in member accounts under `Lakehouse Workloads OU`.
- Does not affect the management account.
- Could block legitimate emergency recovery if the documented break-glass path
  is incomplete, stale, or untested.
- Has a higher recovery-path risk than `DenyLeavingOrganization`, even though
  the policy itself is still narrow compared with broader Region or S3 SCPs.

## Rollback Path

If later attached and found too restrictive, detach it from the OU:

```bash
aws organizations detach-policy \
  --profile org-admin \
  --policy-id <policy-id> \
  --target-id ou-gbyf-m6ppfmpq
```

If the policy is no longer needed after detaching, delete it:

```bash
aws organizations delete-policy \
  --profile org-admin \
  --policy-id <policy-id>
```

Validation of rollback:

- `list-targets-for-policy --policy-id <policy-id>` should no longer include
  `ou-gbyf-m6ppfmpq`;
- `list-policies-for-target --target-id ou-gbyf-m6ppfmpq` should no longer show
  the policy;
- `lakehouse-workload-account` should remain in `Lakehouse Workloads OU`.

## Validation

Read-only commands used for this preparation step:

```bash
aws sts get-caller-identity \
  --profile org-admin \
  --output json

aws organizations list-roots \
  --profile org-admin \
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

Planned live command sequence after the blocker is resolved:

```bash
aws organizations create-policy \
  --profile org-admin \
  --content file://docs/policies/scp/deny-root-user-actions.example.json \
  --name DenyRootUserActions-LakehouseWorkloads \
  --description "Restrict root-user actions for member accounts in Lakehouse Workloads OU except emergency recovery actions" \
  --type SERVICE_CONTROL_POLICY

aws organizations attach-policy \
  --profile org-admin \
  --policy-id <policy-id> \
  --target-id ou-gbyf-m6ppfmpq

aws organizations list-targets-for-policy \
  --profile org-admin \
  --policy-id <policy-id> \
  --output json

aws organizations list-policies-for-target \
  --profile org-admin \
  --target-id ou-gbyf-m6ppfmpq \
  --filter SERVICE_CONTROL_POLICY \
  --output json
```

Blocking condition:

- live attachment is not yet responsible because the emergency root-use process
  now has defined live values, but the remaining MFA, out-of-band backup, and
  procedural test checks are not yet evidenced for this repository context.

## Cost Impact

- No direct AWS service charge is expected from this SCP if later attached.
- Operational risk is the main cost here, because an unready emergency path
  would make a restrictive root-user guardrail harder to recover from than the
  current `DenyLeavingOrganization` policy.

## Approval

- Approval source: direct user instruction
- Approval text: `Do the next narrow SCP, likely root-user emergency-only.`
- Approval date: 2026-06-22
- Scope of approval: evaluate and advance the next narrow SCP candidate

## Result

The next narrow SCP candidate is confirmed as the root-user emergency-only
guardrail for `Lakehouse Workloads OU`, but live execution is blocked by
remaining break-glass live-readiness evidence.

Current interpretation:

- this is still the right next SCP in sequence after
  `DenyLeavingOrganization`;
- the blocker is not OU structure or SCP mechanics;
- the blocker is now narrower than before:
  the target owner, contact, notification, storage, evidence, scope, and
  reduction values are defined, and the current live principal inventory is now
  clearer, the dedicated break-glass principal now exists with MFA and a
  management-account emergency assignment, but out-of-band backup generation,
  notification-path validation, root-user MFA evidence, and procedural
  validation are still required before a root-user restriction can be attached
  responsibly.
