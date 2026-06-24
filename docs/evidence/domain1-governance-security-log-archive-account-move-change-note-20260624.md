# Domain 1 Governance Change Note - Move Security Log Archive Account Into Security OU - 2026-06-24

<!-- markdownlint-disable MD013 -->

## Target Account And OU

- Management account: `349687196588` / `management-account-alias`
- Account moved in this change: `955659429518` / `Security Log Archive`
- Source parent: root `r-gbyf`
- Destination parent: `ou-gbyf-mug20ym0` / `Security OU`

## Current State

Before this change:

- account `955659429518` existed and was attached directly to root `r-gbyf`;
- `Security OU` existed and was empty;
- the dedicated security/log archive account had already been created as a
  separate durable boundary;
- alternate contacts had already been handled separately.

Evidence files:

- `docs/evidence/domain1-governance-security-log-archive-account-parent-pre-move-20260624.json`
- `docs/evidence/domain1-governance-security-ou-accounts-pre-move-20260624.json`
- `docs/evidence/domain1-governance-root-accounts-pre-security-move-20260624.json`
- `docs/evidence/domain1-governance-security-log-archive-account-change-note-20260624.md`

## Proposed Change

Move account `955659429518` from root `r-gbyf` into `Security OU`
`ou-gbyf-mug20ym0`.

Why now:

- AWS Organizations account creation places new accounts under root first;
- the SAP-C02-preferred target design calls for the security/log archive
  account to live inside `Security OU`;
- moving it now completes the intended governance boundary before CloudTrail,
  KMS, AWS Config, or GuardDuty setup begins.

## Expected Blast Radius

- Changes Organizations parent placement for account `955659429518` only.
- Does not move any other account.
- Does not directly change IAM, Identity Center, CloudTrail, S3, KMS, AWS
  Config, or GuardDuty resources.
- Changes which OU-level SCPs may apply later to the account.

## Rollback Path

Move the account back to root:

```bash
aws organizations move-account \
  --profile org-admin \
  --account-id 955659429518 \
  --source-parent-id ou-gbyf-mug20ym0 \
  --destination-parent-id r-gbyf
```

Validation of rollback:

- `list-parents --child-id 955659429518` should show root `r-gbyf`;
- `list-accounts-for-parent --parent-id ou-gbyf-mug20ym0` should no longer
  include account `955659429518`;
- `list-accounts-for-parent --parent-id r-gbyf` should include it again.

## Validation

Commands used:

```bash
aws organizations move-account \
  --profile org-admin \
  --account-id 955659429518 \
  --source-parent-id r-gbyf \
  --destination-parent-id ou-gbyf-mug20ym0

aws organizations list-parents \
  --profile org-admin \
  --child-id 955659429518 \
  --output json

aws organizations list-accounts-for-parent \
  --profile org-admin \
  --parent-id ou-gbyf-mug20ym0 \
  --output json

aws organizations list-accounts-for-parent \
  --profile org-admin \
  --parent-id r-gbyf \
  --output json
```

Success criteria:

- `list-parents` shows `ou-gbyf-mug20ym0` for account `955659429518`;
- `Security OU` account listing shows `Security Log Archive`;
- root account listing no longer shows account `955659429518`.

## Cost Impact

- No direct AWS service charge is expected from the parent move itself.
- Small governance overhead changes because later OU-scoped controls now have a
  clear target for the security/log archive boundary.

## Approval

- Approval source: direct user instruction
- Approval text: `Explicit approval granted, proceed with Move account 955659429518 from root r-gbyf into Security OU ou-gbyf-mug20ym0.`
- Approval date: 2026-06-24
- Scope of approval: move only account `955659429518` from root into
  `Security OU`

## Result

The account move succeeded.

Post-change evidence:

- `docs/evidence/domain1-governance-security-log-archive-account-parent-post-move-20260624.json`
- `docs/evidence/domain1-governance-security-ou-accounts-post-move-20260624.json`
- `docs/evidence/domain1-governance-root-accounts-post-security-move-20260624.json`

Current interpretation:

- `Security Log Archive` account `955659429518` now sits in `Security OU`;
- the intended security/log archive account boundary is now live;
- the next major governance slice can move from account-boundary setup toward
  CloudTrail bucket/KMS and organization-trail implementation.
