# Domain 1 Governance Change Note - Attach DenyLeavingOrganization SCP To Lakehouse Workloads OU - 2026-06-22

<!-- markdownlint-disable MD013 -->

## Status

Live execution was attempted on 2026-06-22.

The OU attachment did **not** complete because `AttachPolicy` failed with
`PolicyTypeNotEnabledException`.

A temporary customer-managed SCP was created during the attempt and then
deleted as rollback when the attachment blocker became clear.

Net live outcome: no customer-managed SCP is currently attached to
`Lakehouse Workloads OU`.

## Target Account And OU

- Management account: `349687196588` / `management-account-alias`
- Target OU: `ou-gbyf-m6ppfmpq` / `Lakehouse Workloads OU`
- Current in-scope member account: `464975959576` / `lakehouse-workload-account`
- SCP candidate file:
  `docs/policies/scp/deny-leaving-organization.example.json`

## Current State

Fresh 2026-06-22 read-only evidence shows:

- the shell is authenticated to the management account
  `349687196588` through the `org-admin` profile;
- the organization describes `SERVICE_CONTROL_POLICY` as available and
  `ENABLED` at the organization level;
- the root `r-gbyf` still returns `PolicyTypes: []` from `list-roots`;
- `Lakehouse Workloads OU` exists as `ou-gbyf-m6ppfmpq`;
- `lakehouse-workload-account` is attached to that OU;
- the workload OU had no attached SCPs before the live attempt;
- the management-account recovery and emergency access path is already
  documented in `docs/runbooks/break-glass-access-procedure.md`, which
  satisfies the design-level prerequisite for this SCP candidate.

Evidence:

- `docs/evidence/domain1-governance-deny-leaving-org-sts-prechange-20260622.json`
- `docs/evidence/domain1-governance-deny-leaving-org-description-prechange-20260622.json`
- `docs/evidence/domain1-governance-deny-leaving-org-roots-prechange-20260622.json`
- `docs/evidence/domain1-governance-deny-leaving-org-lakehouse-ou-policies-prechange-20260622.json`
- `docs/evidence/domain1-governance-deny-leaving-org-lakehouse-ou-accounts-prechange-20260622.json`
- `docs/evidence/domain1-governance-lakehouse-workloads-ou-change-note-20260621.md`
- `docs/evidence/domain1-governance-lakehouse-account-move-change-note-20260622.md`
- `docs/evidence/domain1-governance-org-inventory-summary-20260621.md`

Important interpretation:

- the root-level response and the `AttachPolicy` error are the operationally
  important signals for this change boundary;
- even though `describe-organization` reports SCP availability at the
  organization level, the actual attachment path is still blocked until the
  root policy type is separately enabled and validated.

## Why This Control First

`DenyLeavingOrganization` is the safest first OU-targeted SCP candidate for
`Lakehouse Workloads OU`.

Reason:

- it is narrow and easy to explain;
- it directly protects the new OU/account boundary that was just created;
- it does not depend on service-specific condition-key exceptions;
- it does not affect the management account;
- it is less likely to interfere with legitimate runtime operations than a
  broader Region, S3, or encryption guardrail.

The meaningful alternative for the first bounded control was a narrow
root-user-emergency-only guardrail. That option remains valuable, but it was
not chosen first because it carries more recovery-path risk if break-glass and
root-use exceptions are not validated carefully in each member account.

## Proposed Change

Create one SCP from
`docs/policies/scp/deny-leaving-organization.example.json` and attach it only
to `ou-gbyf-m6ppfmpq` / `Lakehouse Workloads OU`.

Policy intent:

- deny `organizations:LeaveOrganization`;
- apply that deny to the current lakehouse workload account and any later
  member account placed in the same OU;
- leave all other SCP candidates out of scope for this change unit.

This change intentionally does **not**:

- attach a root-user restriction SCP in the same change;
- attach broader S3, Region, CloudTrail, or encryption guardrails;
- change IAM Identity Center assignments;
- move any account between OUs.

## Expected Blast Radius

- Affects AWS Organizations governance posture for member accounts in
  `Lakehouse Workloads OU`.
- Prevents those member accounts from leaving the AWS Organization while the
  SCP remains attached, even if IAM policy would otherwise allow the action.
- Does not affect the management account, because SCPs do not apply there.
- Does not directly change running lakehouse services such as S3, Lambda, Glue,
  Athena, Step Functions, or budgets.
- Can slow an intentional future account-separation or account-closure flow
  until the SCP is detached or the account is moved to a different parent.

## Rollback Path

If the SCP creates an unexpected operational problem, detach it from the OU:

```bash
aws organizations detach-policy \
  --profile org-admin \
  --policy-id <policy-id> \
  --target-id ou-gbyf-m6ppfmpq
```

If the SCP was created only for this change and is no longer needed, delete it
after detaching:

```bash
aws organizations delete-policy \
  --profile org-admin \
  --policy-id <policy-id>
```

Validation of rollback:

- `list-policies-for-target --target-id ou-gbyf-m6ppfmpq` should no longer show
  the policy;
- `list-targets-for-policy --policy-id <policy-id>` should no longer include
  `ou-gbyf-m6ppfmpq`;
- the OU/account placement should remain unchanged.

Actual rollback used in this session:

- the temporary customer-managed policy was created as `p-f1wyqasi`;
- the attach step failed before any target attachment existed;
- `list-targets-for-policy --policy-id p-f1wyqasi` returned no targets;
- the unattached policy was deleted with
  `aws organizations delete-policy --profile org-admin --policy-id p-f1wyqasi`.

## Validation

Commands executed for the 2026-06-22 live attempt:

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

aws organizations list-policies-for-target \
  --profile org-admin \
  --target-id ou-gbyf-m6ppfmpq \
  --filter SERVICE_CONTROL_POLICY \
  --output json

aws organizations list-accounts-for-parent \
  --profile org-admin \
  --parent-id ou-gbyf-m6ppfmpq \
  --output json

aws organizations create-policy \
  --profile org-admin \
  --content file://docs/policies/scp/deny-leaving-organization.example.json \
  --name DenyLeavingOrganization-LakehouseWorkloads \
  --description "Prevent member accounts in Lakehouse Workloads OU from leaving the organization" \
  --type SERVICE_CONTROL_POLICY

aws organizations attach-policy \
  --profile org-admin \
  --policy-id p-f1wyqasi \
  --target-id ou-gbyf-m6ppfmpq

aws organizations list-targets-for-policy \
  --profile org-admin \
  --policy-id p-f1wyqasi \
  --output json

aws organizations delete-policy \
  --profile org-admin \
  --policy-id p-f1wyqasi
```

Observed blocker:

```text
aws: [ERROR]: An error occurred (PolicyTypeNotEnabledException) when calling the AttachPolicy operation: This operation can be performed only for enabled policy types.
```

Recorded blocker evidence:

- `docs/evidence/domain1-governance-deny-leaving-org-attach-error-20260622.txt`
- `docs/evidence/domain1-governance-deny-leaving-org-policy-id-20260622.txt`
- `docs/evidence/domain1-governance-deny-leaving-org-policy-targets-predelete-20260622.json`
- `docs/evidence/domain1-governance-deny-leaving-org-policies-postrollback-20260622.json`
- `docs/evidence/domain1-governance-deny-leaving-org-policies-current-20260622.json`

Success criteria status:

- the policy content choice remains valid and intentionally narrow;
- the actual OU attachment success criteria were **not met**;
- no unintended SCP attachment remained after rollback;
- the organization is still waiting on a separate root-level policy-type
  enablement change before this SCP can be attached responsibly.

Intentional validation limit:

- do **not** use a real `leave-organization` call as a casual test, because a
  mis-scoped or missing SCP could turn the validation attempt into a real
  member-account separation event.
- For this narrow first guardrail, structural validation through policy content
  and attachment evidence is the safer default.

## Cost Impact

- No durable AWS service charge is expected from this attempted create-and-
  rollback cycle, and no ongoing charge is expected when this narrow SCP is
  later attached successfully.
- Small governance overhead increases because later account-separation or OU
  re-homing actions would need to account for this guardrail first.

## Approval

- Approval source: direct user instruction
- Approval text: `proceed with the next live step step`
- Approval date: 2026-06-22
- Scope of approval: attempt the bounded live create-and-attach step for
  `DenyLeavingOrganization` against `Lakehouse Workloads OU` only

## Result

The first bounded OU-targeted SCP live change was attempted and rolled back
cleanly.

Current interpretation:

- `DenyLeavingOrganization` is the cleanest first live guardrail candidate for
  `Lakehouse Workloads OU`;
- the blocker is now narrower and clearer than before: the next prerequisite is
  not more OU design work, but a separate explicit change to enable
  `SERVICE_CONTROL_POLICY` for root `r-gbyf`;
- because that prerequisite changes organization-wide policy behavior, it
  should be treated as its own change unit before retrying any OU-targeted SCP
  attachment.
