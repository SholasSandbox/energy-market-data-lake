# Domain 1 Governance Change Note - Create Security Log Archive Account - 2026-06-24

<!-- markdownlint-disable MD013 -->

## Target Account And OU

- Management account: `349687196588` / `management-account-alias`
- New member account email: `[redacted-email]`
- New member account name: `Security Log Archive`
- Expected initial parent after creation: root `r-gbyf`
- Intended later target OU: `ou-gbyf-mug20ym0` / `Security OU`

## Current State

Before this change:

- the organization contains three active accounts;
- root `r-gbyf` contains `Lakehouse Workloads OU`, `Security OU`, and
  `Container Sandbox`;
- `Security OU` exists but is still empty;
- the SAP-C02-preferred path has already selected a dedicated security/log
  archive account as the future home for central CloudTrail log storage, the
  future AWS Config aggregator, and later GuardDuty delegated administration;
- account email and owner/recovery direction are now explicit enough to create
  the account without guessing.

Evidence files:

- `docs/evidence/domain1-governance-security-account-sts-prechange-20260624.json`
- `docs/evidence/domain1-governance-security-account-accounts-prechange-20260624.json`
- `docs/evidence/domain1-governance-security-account-root-ous-prechange-20260624.json`
- `docs/evidence/domain1-governance-security-account-security-ou-accounts-prechange-20260624.json`
- `docs/planning/domain-1-security-log-archive-account-implementation-boundary-20260622.md`

## Proposed Change

Create one new AWS Organizations member account with:

- email `[redacted-email]`
- account name `Security Log Archive`

This change intentionally does **not**:

- move the new account into `Security OU` yet;
- enable `account.amazonaws.com` trusted access yet;
- configure alternate contacts yet;
- create any CloudTrail, S3, KMS, AWS Config, GuardDuty, or Security Hub
  resources;
- attach any SCP directly to the new account.

Why now:

- the dedicated security/log archive account is the next SAP-C02-aligned
  boundary after `Security OU` creation;
- AWS Organizations account creation is a durable step and deserves its own
  evidence package;
- keeping move, alternate contacts, and logging setup separate preserves a
  reviewable change history and rollback thinking.

## Expected Blast Radius

- Adds one new member account to the AWS Organization.
- The new account is created under root first, as AWS Organizations account
  creation works, and will need a later move into `Security OU`.
- No existing workload or sandbox account is moved or modified by this change.
- Billing, governance, and account-inventory surfaces now include one more
  account.
- The new account introduces a durable recovery and contact boundary that must
  be handled carefully after creation.

## Rollback Path

Rollback is not symmetrical with account creation.

There is no lightweight immediate undo equivalent to deleting an empty OU.

If this account is created incorrectly:

- do not proceed with workload or security-service setup in it;
- if needed, move it later into an appropriate holding boundary such as
  `Security OU` or a future suspended/quarantine boundary under a separate
  change note;
- follow a deliberate account-closure or decommissioning path rather than
  assuming the create can simply be reversed in place.

Validation of rollback posture:

- ensure no later CloudTrail, KMS, S3, Config, or GuardDuty setup has been
  performed in the new account before deciding the recovery path;
- record any later containment or closure decision under a separate bounded
  note.

## Validation

Commands used:

```bash
aws organizations create-account \
  --profile org-admin \
  --email [redacted-email] \
  --account-name 'Security Log Archive' \
  --output json

aws organizations describe-create-account-status \
  --profile org-admin \
  --create-account-request-id "<request-id>" \
  --output json

aws organizations list-accounts \
  --profile org-admin \
  --output json

aws organizations list-accounts-for-parent \
  --profile org-admin \
  --parent-id ou-gbyf-mug20ym0 \
  --output json
```

Success criteria:

- `describe-create-account-status` reaches `SUCCEEDED`;
- `list-accounts` shows a new active member account named
  `Security Log Archive`;
- the new account is not yet moved into `Security OU` in this same change;
- `Security OU` remains empty after this change, confirming that the move stays
  a separate boundary.

## Cost Impact

- Creating the account itself has no direct Organizations charge.
- The organization now has one more account to govern.
- Future cost starts only when resources are enabled later in the new account.

## Approval

- Approval source: direct user instruction
- Approval text: `I confirm [redacted-email] as the new AWS account email, and reserve [redacted-email] for alternate-contact/log-archive use.`
- Approval date: 2026-06-24
- Scope of approval: create the new member account only; keep alternate
  contacts, trusted access for Account Management, OU move, and logging setup as
  separate later steps

## Result

The account creation succeeded.

Create-account request result:

- Request ID: `car-3d882c1859954699a2bd8f741175b331`
- Final state: `SUCCEEDED`
- New account ID: `955659429518`
- New account name: `Security Log Archive`
- New account email: `[redacted-email]`

Post-change evidence:

- `docs/evidence/domain1-governance-security-log-archive-account-create-20260624.json`
- `docs/evidence/domain1-governance-security-log-archive-account-status-20260624.json`
- `docs/evidence/domain1-governance-security-log-archive-account-accounts-postchange-20260624.json`
- `docs/evidence/domain1-governance-security-log-archive-account-root-accounts-postchange-20260624.json`
- `docs/evidence/domain1-governance-security-log-archive-account-security-ou-accounts-postchange-20260624.json`
- `docs/evidence/domain1-governance-security-log-archive-account-parent-postchange-20260624.json`

Current interpretation:

- the dedicated security/log archive account now exists live;
- it was created under root `r-gbyf`, which matches normal Organizations
  account-creation behavior;
- `Security OU` remains empty, so the account move is still a separate bounded
  step;
- alternate contacts are still not configured, and `account.amazonaws.com`
  trusted access still needs to be enabled before that central-management step
  can be executed.
