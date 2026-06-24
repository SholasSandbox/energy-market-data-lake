# Domain 1 Governance Change Note - Create Security OU - 2026-06-22

<!-- markdownlint-disable MD013 -->

## Target Account And OU

- Management account: `349687196588` / `management-account-alias`
- Source parent: root `r-gbyf`
- OU created in this change: `Security OU`
- New OU ID: `ou-gbyf-mug20ym0`
- Account move in this change boundary: none

## Current State

Before this change:

- the root OU list contains `Lakehouse Workloads OU`
  (`ou-gbyf-m6ppfmpq`) and `Container Sandbox` (`ou-gbyf-zs0f26b5`);
- no `Security OU` exists yet under root `r-gbyf`;
- the management account remains attached directly to root `r-gbyf`;
- there are still only three accounts in the organization, so the dedicated
  security/log archive boundary does not yet exist live;
- the SAP-C02-preferred logging path is already accepted, and `Security OU` is
  now the next narrow live implementation unit for that path.

Evidence files:

- `docs/evidence/domain1-governance-security-ou-sts-prechange-20260622.json`
- `docs/evidence/domain1-governance-security-ou-root-ous-prechange-20260622.json`
- `docs/evidence/domain1-governance-security-ou-accounts-prechange-20260622.json`
- `docs/evidence/domain1-governance-security-ou-management-parent-prechange-20260622.json`
- `docs/planning/domain-1-security-log-archive-account-implementation-boundary-20260622.md`

## Proposed Change

Create one new root-level OU named `Security OU`.

This change intentionally does **not**:

- create the dedicated security/log archive member account;
- move any account into the new OU;
- enable CloudTrail, S3, KMS, AWS Config, GuardDuty, or Security Hub;
- attach any SCP to the new OU.

Why now:

- `Security OU` is the smallest live step that advances the accepted
  centralized-logging path;
- it creates the destination boundary needed before a durable account-creation
  step;
- it preserves the SAP-C02-preferred separation between control-plane
  administration and the future audit/security boundary.

## Expected Blast Radius

- Affects AWS Organizations structure in the management account only.
- Does not move any account.
- Does not change SCP attachments.
- Does not change Identity Center assignments.
- Does not change running lakehouse or sandbox workloads directly.
- Creates one additional empty OU that later account-creation and logging steps
  may depend on.

## Rollback Path

If no account has been moved into the OU and no later dependency is created,
delete the empty OU:

```bash
aws organizations delete-organizational-unit \
  --profile org-admin \
  --organizational-unit-id ou-gbyf-mug20ym0
```

Validation of rollback:

- `describe-organizational-unit` for the new OU should fail;
- `list-organizational-units-for-parent --parent-id r-gbyf` should no longer
  return `Security OU`;
- `list-parents --child-id 349687196588` should still show the management
  account attached directly to root.

## Validation

Commands used:

```bash
aws organizations create-organizational-unit \
  --profile org-admin \
  --parent-id r-gbyf \
  --name 'Security OU' \
  --output json

aws organizations describe-organizational-unit \
  --profile org-admin \
  --organizational-unit-id ou-gbyf-mug20ym0 \
  --output json

aws organizations list-organizational-units-for-parent \
  --profile org-admin \
  --parent-id r-gbyf \
  --output json

aws organizations list-parents \
  --profile org-admin \
  --child-id 349687196588 \
  --output json
```

Success criteria:

- `Security OU` exists under root `r-gbyf`;
- the new OU ID is stable and queryable;
- `Lakehouse Workloads OU` and `Container Sandbox` still exist unchanged;
- the management account remains attached directly to root after this change;
- no account is moved as part of this change unit.

## Cost Impact

- No direct AWS service charge is expected from OU creation itself.
- Minor governance and documentation overhead increases because an additional OU
  now exists and must stay aligned with later account-placement and SCP work.

## Approval

- Approval source: direct user instruction
- Approval text: `We are close to 5hr usage limit (5% remaining), if it can fit, let's proceed, otherwise, I am okay to wait till after the reset.`
- Approval date: 2026-06-22
- Scope of approval: proceed with the narrow `Security OU` live step if it fits
  in the remaining session budget

## Result

The `Security OU` creation succeeded.

Post-change evidence:

- `docs/evidence/domain1-governance-security-ou-create-20260622.json`
- `docs/evidence/domain1-governance-security-ou-postchange-20260622.json`
- `docs/evidence/domain1-governance-security-ou-root-ous-postchange-20260622.json`
- `docs/evidence/domain1-governance-security-ou-management-parent-postchange-20260622.json`

Validation nuance:

- the first root-OU postchange read did not include `Security OU`, likely
  because of parallel timing or short Organizations propagation;
- the follow-up `describe-organizational-unit` and repeated root-OU list both
  showed the settled state correctly.

Current interpretation:

- the SAP-C02-preferred dedicated security/log archive path is still the chosen
  direction;
- `Security OU` now exists live under root `r-gbyf`;
- the later dedicated account-creation step remains separate because it needs
  explicit email/owner inputs and has a less reversible blast radius.
