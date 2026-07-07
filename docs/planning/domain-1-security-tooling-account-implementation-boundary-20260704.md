# Domain 1 Security Tooling Account Implementation Boundary - 2026-07-04

<!-- markdownlint-disable MD013 -->

## Status

Design-to-implementation boundary drafted; fresh read-only prechange evidence
collected; account-boundary decision closed; first live account-placement slice
completed; separate alternate-contact readiness slice completed; AWS Config
delegated-administration and aggregation migration completed; recorder-scope
decision closed; GuardDuty delegated-admin planning recorded.

The `Security Tooling` account was created and moved into `Security OU` on
2026-07-04. On 2026-07-06, only the `SECURITY`, `OPERATIONS`, and `BILLING`
alternate contacts were configured. Later on 2026-07-06, AWS Config delegated
administration and aggregation were migrated from `Security Log Archive` to
`Security Tooling`.

Fresh Organizations/account prechange evidence was collected on 2026-07-04
after the `org-admin` SSO token was refreshed:

- `docs/evidence/domain1-governance-security-tooling-account-prechange-summary-20260704.md`

That prechange evidence did not by itself authorize live account creation,
account movement, or delegated-admin migration.

The first implementation slice was narrowed to account creation and placement
only.

Live account-placement evidence:

- `docs/evidence/domain1-governance-security-tooling-account-placement-change-note-20260704.md`

Live alternate-contact evidence:

- `docs/evidence/domain1-governance-security-tooling-alt-contacts-change-note-20260706.md`

Live AWS Config migration evidence:

- `docs/evidence/domain1-governance-config-security-tooling-migration-change-note-20260706.md`

## Purpose

Create a separate `Security Tooling` account in `Security OU` so active
security-service administration is separated from write-mostly audit storage.

This implements the accepted long-term split:

- `Security Log Archive`: owns organization CloudTrail and AWS Config delivery
  buckets, KMS keys, retention controls, and log-storage evidence.
- `Security Tooling`: owns delegated-administrator and security-operations
  functions such as AWS Config aggregation, GuardDuty delegated administration,
  possible future Security Hub, OAM/cross-account observability, and read-only
  investigation tooling.

## Current Accepted Design

The accepted target design is recorded in:

- `docs/adr/0005-aws-organizations-governance-design.md`
- `docs/planning/domain-1-config-guardduty-design-20260621.md`
- `docs/planning/sap-c02-readiness-tracker.md`

Relevant sequencing now allows this slice because the root-user emergency-only
SCP readiness blocker has closed and
`DenyRootUserActions-LakehouseWorkloads` is live for `Lakehouse Workloads OU`.

## Adopted Account Boundary

Accepted account name:

- `Security Tooling`

Accepted target parent:

- `ou-gbyf-mug20ym0` / `Security OU`

Accepted purpose:

- AWS Config organization aggregation and delegated administration;
- GuardDuty delegated administration in `eu-west-2`;
- possible future Security Hub administration if later adopted;
- OAM/cross-account observability if operational telemetry centralization
  becomes useful;
- read-only security investigation tooling.

Explicitly out of scope for this account:

- owning durable CloudTrail or AWS Config log archive buckets;
- owning log archive KMS keys;
- replacing the management account as the Organizations control plane;
- routine lakehouse workload operation;
- broad security-service enablement without separate approval.

### Account-Boundary Decisions

| Decision | Outcome |
|---|---|
| Account name | Use `Security Tooling`. |
| Target OU | Place the account in `Security OU` / `ou-gbyf-mug20ym0`. |
| Account creation and placement | Create the account and move it from root into `Security OU` in the same approved working session. |
| Alternate contacts | Do not make alternate-contact configuration part of the first account-creation boundary. Completed later as a separate bounded change on 2026-07-06 after the contact values and account-access path were explicitly approved. |
| Delegated administration | Do not migrate AWS Config, GuardDuty, Security Hub, OAM, or any other delegated-admin function during account creation. |
| `Security Log Archive` role | Keep `Security Log Archive` storage-only for durable audit buckets, KMS keys, retention controls, and log-storage evidence. |
| `so-aws-admin` in `Security OU` | Place on the decommission path. Do not move active security tooling, Security Hub, AWS Config recording, GuardDuty, OAM, or workload ownership into it. Retire only after read-only dependency checks, dependency resolution, and separate explicit approval. |
| Root email | Supplied and approved before live account creation: `[redacted-email]`. |

### Live Account Values

| Field | Value |
|---|---|
| Account name | `Security Tooling` |
| Account ID | `668848431187` |
| Root email | `[redacted-email]` |
| Parent | `ou-gbyf-mug20ym0` / `Security OU` |
| Alternate contacts | `SECURITY`, `OPERATIONS`, and `BILLING` configured on 2026-07-06 |
| AWS Config delegated admin | `config.amazonaws.com` and `config-multiaccountsetup.amazonaws.com` migrated to `Security Tooling` on 2026-07-06 |
| AWS Config aggregator | `organization-config-aggregator-eu-west-2` in `eu-west-2` |
| AWS Config recorder scope | Recorder onboarding completed in `eu-west-2`; `so-aws-admin` remains excluded on the decommission path |
| GuardDuty delegated admin | Live in `eu-west-2` with foundational coverage only; no new account |

### Completed Separate Account-Readiness Step

Accepted recommendation completed: configure alternate contacts for `Security
Tooling` before migrating AWS Config delegated administration or any other
security-service administration into the account.

This was completed as a separate bounded implementation step from account
creation and remains separate from delegated-admin migration.

Completed scope for that step:

- verify `account.amazonaws.com` trusted access is still enabled;
- capture current alternate-contact state for `Security Tooling`;
- configure `SECURITY`, `OPERATIONS`, and `BILLING` alternate contacts for
  account `668848431187`;
- record postchange evidence for all three contact types.

Out of scope for that contact step, and not performed in that step:

- AWS Config delegated-administrator migration;
- GuardDuty, Security Hub, or OAM setup;
- SCP changes;
- account movement;
- workload resource changes.

Approved values used:

| Contact type | Email | Name | Title | Phone |
|---|---|---|---|---|
| `SECURITY` | `[redacted-email]` | `[redacted-contact-name]` | `AWS Governance Contact` | `[redacted-phone]` |
| `OPERATIONS` | `[redacted-email]` | `[redacted-contact-name]` | `AWS Governance Contact` | `[redacted-phone]` |
| `BILLING` | `[redacted-email]` | `[redacted-contact-name]` | `AWS Governance Contact` | `[redacted-phone]` |

For this personal lab, using one durable monitored mailbox for all three
alternate-contact types is acceptable if explicitly approved. Purpose-specific
aliases remain cleaner if they are available and monitored.

## Required Prechange Evidence

Before live account creation, collect fresh read-only evidence:

```bash
aws sts get-caller-identity --profile org-admin --output json

aws organizations list-roots --profile org-admin --output json

aws organizations list-organizational-units-for-parent \
  --profile org-admin \
  --parent-id r-gbyf \
  --output json

aws organizations list-accounts --profile org-admin --output json

aws organizations list-accounts-for-parent \
  --profile org-admin \
  --parent-id ou-gbyf-mug20ym0 \
  --output json

aws organizations list-policies-for-target \
  --profile org-admin \
  --target-id ou-gbyf-mug20ym0 \
  --filter SERVICE_CONTROL_POLICY \
  --output json

aws organizations list-aws-service-access-for-organization \
  --profile org-admin \
  --output json

aws organizations list-delegated-administrators \
  --profile org-admin \
  --output json

aws organizations list-delegated-services-for-account \
  --profile org-admin \
  --account-id 955659429518 \
  --output json
```

Expected evidence questions:

- Is the caller still the Organizations management account?
- Does `Security OU` still exist under root `r-gbyf`?
- Which accounts currently sit in `Security OU`?
- Does a `Security Tooling` account already exist?
- Which SCPs are attached to `Security OU`?
- Which trusted services are currently enabled?
- Which delegated administrators are currently configured?
- Which delegated services currently point at `Security Log Archive`
  (`955659429518`) and therefore need later migration planning?

## Completed Live Implementation Sequence

This sequence was completed under separate explicit approval.

1. Create the member account.
2. Poll account creation until it succeeds.
3. Move the account from root into `Security OU` in the same working session.
4. Record postchange account inventory and parent mapping evidence.
5. Stop before any delegated-admin migration.

The first implementation slice created and placed the account only. The next
separate account-readiness slice configured alternate contacts only. The next
separate service-migration slice migrated AWS Config delegated administration
and aggregation only.

## Approval Recorded For Live Account Creation

The following values were explicitly approved before the live change:

- account name: `Security Tooling`;
- account root email address: `[redacted-email]`;
- no alternate contacts during this boundary;
- move the account into `Security OU` in the same working session;
- exact approval for the live `create-account` and `move-account` commands.

## Transition Order After Account Placement

Accepted order:

1. Create and place `Security Tooling` in `Security OU` as a separate account
   placement step. Completed on 2026-07-04.
2. Configure `Security Tooling` alternate contacts as a separate
   account-readiness step. Completed on 2026-07-06.
3. Migrate AWS Config delegated administration and aggregation. Completed on
   2026-07-06.
4. Close recorder scope for `so-aws-admin` and `Security Tooling`. Completed on
   2026-07-06: `Security Tooling` was approved for bounded recorder onboarding;
   `so-aws-admin` remains excluded on the decommission path. See
   `docs/planning/domain-1-config-recorder-scope-decision-20260706.md`.
5. Under separate explicit approval, onboard the `Security Tooling` recorder in
   `eu-west-2`, then remove only `668848431187` from the migrated organization
   Config rule exclusions. Completed on 2026-07-07.
6. Collect read-only dependency evidence for `so-aws-admin`, resolve any
   dependencies, and retire it only under separate explicit approval.
7. Record GuardDuty delegated-admin planning with `Security Tooling` as the
   delegated-admin target and no new account. Completed on 2026-07-06.
8. Under separate explicit approval, configure GuardDuty delegated
   administration in `Security Tooling`. Completed on 2026-07-07 for
   foundational coverage in `eu-west-2`.
9. Adopt Security Hub only if later intentionally adopted in `Security Tooling`.
10. Adopt OAM only if centralized operational telemetry becomes useful.

Do not migrate all services at account-creation time.

## Rollback And Safety Notes

Account creation cannot be rolled back like an SCP attachment. If created with
the wrong name, email, or purpose, the practical response is to quarantine the
account, avoid enabling services, and decide whether to close or repurpose it
later.

The safer rollback boundary is therefore:

- collect prechange evidence;
- explicitly approve the account name and email;
- create the account once;
- move it to the intended OU;
- stop before service migrations.

## SAP-C02 Relevance

This supports Domain 1 by separating management account control-plane duties,
write-mostly audit storage, and active delegated security administration. It
also reinforces the SAP-C02 pattern of delegated administration for security
services without using the management account for day-to-day operations.
