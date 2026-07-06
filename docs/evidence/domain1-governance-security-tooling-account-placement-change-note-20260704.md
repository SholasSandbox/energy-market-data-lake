# Domain 1 Governance Change Note - Create Security Tooling Account And Move Into Security OU - 2026-07-04

<!-- markdownlint-disable MD013 -->

## Status

Completed successfully.

This was a bounded live AWS Organizations change. One new member account was
created, polled to success, and moved from root `r-gbyf` into `Security OU`
`ou-gbyf-mug20ym0`.

No alternate contacts were configured, no SCP was attached or detached, no
trusted service access was changed, and no delegated-administrator setting was
migrated.

## Scope

- Management account: `349687196588` / `management-account-alias`
- New member account name: `Security Tooling`
- New member account ID: `668848431187`
- New member account root email: `[redacted-email]`
- Initial parent after creation: root `r-gbyf`
- Final parent after move: `ou-gbyf-mug20ym0` / `Security OU`

## Approval

- Approval source: direct user instruction
- Approval text: `Use [redacted-email] as the root email for Security Tooling. I explicitly approve creating the Security Tooling AWS Organizations account, polling creation to success, and moving it from root r-gbyf into Security OU ou-gbyf-mug20ym0. Do not configure alternate contacts or migrate delegated administration.`
- Approval date: 2026-07-04
- Scope of approval: create and place only the `Security Tooling` account;
  do not configure alternate contacts or migrate AWS Config, GuardDuty,
  Security Hub, OAM, or any other delegated administration.

## Prechange Evidence

Immediate read-only prechange evidence was collected before account creation:

- caller identity:
  `docs/evidence/domain1-governance-security-tooling-account-placement-sts-prechange-20260704.json`
- root policy-type status:
  `docs/evidence/domain1-governance-security-tooling-account-placement-roots-prechange-20260704.json`
- root OU inventory:
  `docs/evidence/domain1-governance-security-tooling-account-placement-root-ous-prechange-20260704.json`
- all-account inventory:
  `docs/evidence/domain1-governance-security-tooling-account-placement-accounts-prechange-20260704.json`
- root account inventory:
  `docs/evidence/domain1-governance-security-tooling-account-placement-root-accounts-prechange-20260704.json`
- `Security OU` account inventory:
  `docs/evidence/domain1-governance-security-tooling-account-placement-security-ou-accounts-prechange-20260704.json`
- `Security OU` SCP attachments:
  `docs/evidence/domain1-governance-security-tooling-account-placement-security-ou-policies-prechange-20260704.json`
- delegated administrator inventory:
  `docs/evidence/domain1-governance-security-tooling-account-placement-delegated-admins-prechange-20260704.json`
- delegated services for `Security Log Archive`:
  `docs/evidence/domain1-governance-security-tooling-account-placement-security-log-archive-delegated-services-prechange-20260704.json`

Prechange summary:

- caller was the Organizations management account through `org-admin`;
- no existing account named `Security Tooling` appeared in the organization
  account inventory;
- `Security OU` already contained `Security Log Archive` and `so-aws-admin`;
- `Security OU` had only the AWS-managed `FullAWSAccess` SCP attached;
- `Security Log Archive` was the only delegated administrator returned by
  Organizations.

## Change Executed

Create the member account:

```bash
aws organizations create-account \
  --profile org-admin \
  --email '[redacted-email]' \
  --account-name 'Security Tooling' \
  --output json
```

Create-account evidence:

- `docs/evidence/domain1-governance-security-tooling-account-placement-create-account-20260704.json`
- `docs/evidence/domain1-governance-security-tooling-account-placement-create-account-request-id-20260704.txt`
- `docs/evidence/domain1-governance-security-tooling-account-placement-create-account-poll-20260704.txt`
- `docs/evidence/domain1-governance-security-tooling-account-placement-create-account-status-20260704.json`
- `docs/evidence/domain1-governance-security-tooling-account-placement-account-id-20260704.txt`

Create-account result:

- Request ID: `car-a3d99e94ff4c4267924a51132cb8dd27`
- Final state: `SUCCEEDED`
- New account ID: `668848431187`
- New account name: `Security Tooling`

Verify the new account was initially under root:

```bash
aws organizations list-parents \
  --profile org-admin \
  --child-id 668848431187 \
  --output json
```

Parent-before-move evidence:

- `docs/evidence/domain1-governance-security-tooling-account-placement-parent-before-move-20260704.json`

Move the new account into `Security OU`:

```bash
aws organizations move-account \
  --profile org-admin \
  --account-id 668848431187 \
  --source-parent-id r-gbyf \
  --destination-parent-id ou-gbyf-mug20ym0
```

Move evidence:

- `docs/evidence/domain1-governance-security-tooling-account-placement-move-account-20260704.txt`

## Postchange Verification

Postchange evidence:

- parent mapping for `Security Tooling`:
  `docs/evidence/domain1-governance-security-tooling-account-placement-parent-postchange-20260704.json`
- all-account inventory:
  `docs/evidence/domain1-governance-security-tooling-account-placement-accounts-postchange-20260704.json`
- root account inventory:
  `docs/evidence/domain1-governance-security-tooling-account-placement-root-accounts-postchange-20260704.json`
- `Security OU` account inventory:
  `docs/evidence/domain1-governance-security-tooling-account-placement-security-ou-accounts-postchange-20260704.json`
- `Security OU` SCP attachments:
  `docs/evidence/domain1-governance-security-tooling-account-placement-security-ou-policies-postchange-20260704.json`
- delegated administrator inventory:
  `docs/evidence/domain1-governance-security-tooling-account-placement-delegated-admins-postchange-20260704.json`
- delegated services for `Security Log Archive`:
  `docs/evidence/domain1-governance-security-tooling-account-placement-security-log-archive-delegated-services-postchange-20260704.json`
- delegated-services check for `Security Tooling`:
  `docs/evidence/domain1-governance-security-tooling-account-placement-security-tooling-delegated-services-postchange-20260704.err`
  and
  `docs/evidence/domain1-governance-security-tooling-account-placement-security-tooling-delegated-services-postchange-20260704.status`

Verification summary:

- `Security Tooling` account `668848431187` is active;
- `list-parents` shows `ou-gbyf-mug20ym0` / `Security OU` as its parent;
- `Security OU` now contains:
  - `Security Log Archive` / `955659429518`;
  - `so-aws-admin` / `054394900225`;
  - `Security Tooling` / `668848431187`;
- `Security OU` still has only the AWS-managed `FullAWSAccess` SCP attached;
- `Security Log Archive` remains the only delegated administrator returned by
  the organization-level delegated-admin inventory;
- querying delegated services for `Security Tooling` returns
  `AccountNotRegisteredException`, confirming it is not currently registered
  as a delegated administrator.

## Rollback And Containment

Account creation is not symmetrically reversible.

If the new account must be contained before service migration, move it back to
root or into a future quarantine/suspended OU under a separate approved change:

```bash
aws organizations move-account \
  --profile org-admin \
  --account-id 668848431187 \
  --source-parent-id ou-gbyf-mug20ym0 \
  --destination-parent-id r-gbyf
```

This rollback-style move should be treated as its own change because it changes
OU placement and future SCP inheritance.

## Current Interpretation

The first `Security Tooling` implementation boundary is complete: the account
exists and is placed in `Security OU`.

The next transition must stay separate:

1. AWS Config delegated administration and aggregation migration first.
2. GuardDuty delegated administration next if adopted.
3. Security Hub only if later intentionally adopted.
4. OAM only if centralized operational telemetry becomes useful.

## SAP-C02 Relevance

This supports Domain 1 by separating management account control-plane duties,
write-mostly audit storage, and active delegated security administration. It
also preserves the best-practice pattern of creating a dedicated security
operations account before migrating delegated security-service administration.
