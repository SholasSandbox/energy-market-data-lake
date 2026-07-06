# Domain 1 Security Tooling Alternate Contacts Change Note - 2026-07-06

<!-- markdownlint-disable MD013 -->

## Status

Completed.

`SECURITY`, `OPERATIONS`, and `BILLING` alternate contacts are configured for
the `Security Tooling` account `668848431187`.

## Approval Boundary

User approval for this bounded live change:

> I explicitly approve configuring only these alternate contacts. Do not migrate
> delegated administration, change SCPs, move accounts, or enable services.

Approved contact values:

| Contact type | Email | Name | Title | Phone |
|---|---|---|---|---|
| `SECURITY` | `[redacted-email]` | `[redacted-contact-name]` | `AWS Governance Contact` | `[redacted-phone]` |
| `OPERATIONS` | `[redacted-email]` | `[redacted-contact-name]` | `AWS Governance Contact` | `[redacted-phone]` |
| `BILLING` | `[redacted-email]` | `[redacted-contact-name]` | `AWS Governance Contact` | `[redacted-phone]` |

Explicitly out of scope:

- delegated-administrator migration;
- SCP changes;
- account moves;
- service enablement;
- GuardDuty, Security Hub, or OAM setup;
- workload resource changes.

## Prechange Evidence

Fresh read-only evidence captured before the contact updates:

| Evidence | File |
|---|---|
| Caller identity | `docs/evidence/domain1-governance-security-tooling-alt-contacts-sts-prechange-20260706.json` |
| Account Management trusted access and enabled services | `docs/evidence/domain1-governance-security-tooling-alt-contacts-service-access-prechange-20260706.json` |
| Primary contact snapshot | `docs/evidence/domain1-governance-security-tooling-alt-contacts-primary-contact-prechange-20260706.json` |
| `SECURITY` alternate-contact precheck | `docs/evidence/domain1-governance-security-tooling-alt-contacts-security-prechange-20260706.status`; `docs/evidence/domain1-governance-security-tooling-alt-contacts-security-prechange-20260706.err` |
| `OPERATIONS` alternate-contact precheck | `docs/evidence/domain1-governance-security-tooling-alt-contacts-operations-prechange-20260706.status`; `docs/evidence/domain1-governance-security-tooling-alt-contacts-operations-prechange-20260706.err` |
| `BILLING` alternate-contact precheck | `docs/evidence/domain1-governance-security-tooling-alt-contacts-billing-prechange-20260706.status`; `docs/evidence/domain1-governance-security-tooling-alt-contacts-billing-prechange-20260706.err` |
| Delegated administrators | `docs/evidence/domain1-governance-security-tooling-alt-contacts-delegated-admins-prechange-20260706.json` |
| Security Tooling parent | `docs/evidence/domain1-governance-security-tooling-alt-contacts-parent-prechange-20260706.json` |
| Security OU SCP attachments | `docs/evidence/domain1-governance-security-tooling-alt-contacts-security-ou-policies-prechange-20260706.json` |

Prechange summary:

- caller was the Organizations management account `349687196588` through
  `AWSReservedSSO_AdministratorAccess_be84055d6f72e1b2`;
- `account.amazonaws.com` trusted access was enabled;
- each `get-alternate-contact` precheck returned `ResourceNotFoundException`,
  confirming no existing alternate contact for the requested type;
- `Security Tooling` was already in `Security OU` / `ou-gbyf-mug20ym0`;
- delegated administrators listed only `Security Log Archive` (`955659429518`);
- Security OU SCP attachments listed only the AWS-managed `FullAWSAccess`
  policy.

## Commands Executed

The following live commands were executed with `AWS_PROFILE=org-admin` and
`AWS_REGION=eu-west-2`.

```bash
aws account put-alternate-contact \
  --account-id 668848431187 \
  --alternate-contact-type SECURITY \
  --email-address [redacted-email] \
  --name "[redacted-contact-name]" \
  --phone-number [redacted-phone] \
  --title "AWS Governance Contact"

aws account put-alternate-contact \
  --account-id 668848431187 \
  --alternate-contact-type OPERATIONS \
  --email-address [redacted-email] \
  --name "[redacted-contact-name]" \
  --phone-number [redacted-phone] \
  --title "AWS Governance Contact"

aws account put-alternate-contact \
  --account-id 668848431187 \
  --alternate-contact-type BILLING \
  --email-address [redacted-email] \
  --name "[redacted-contact-name]" \
  --phone-number [redacted-phone] \
  --title "AWS Governance Contact"
```

Execution status files:

- `docs/evidence/domain1-governance-security-tooling-alt-contacts-security-put-20260706.status`
- `docs/evidence/domain1-governance-security-tooling-alt-contacts-operations-put-20260706.status`
- `docs/evidence/domain1-governance-security-tooling-alt-contacts-billing-put-20260706.status`

Each status file contains `0`.

## Postchange Evidence

Postchange evidence captured after the contact updates:

| Evidence | File |
|---|---|
| `SECURITY` alternate contact | `docs/evidence/domain1-governance-security-tooling-alt-contacts-security-postchange-20260706.json` |
| `OPERATIONS` alternate contact | `docs/evidence/domain1-governance-security-tooling-alt-contacts-operations-postchange-20260706.json` |
| `BILLING` alternate contact | `docs/evidence/domain1-governance-security-tooling-alt-contacts-billing-postchange-20260706.json` |
| Primary contact snapshot | `docs/evidence/domain1-governance-security-tooling-alt-contacts-primary-contact-postchange-20260706.json` |
| Account Management trusted access and enabled services | `docs/evidence/domain1-governance-security-tooling-alt-contacts-service-access-postchange-20260706.json` |
| Delegated administrators | `docs/evidence/domain1-governance-security-tooling-alt-contacts-delegated-admins-postchange-20260706.json` |
| Security Tooling parent | `docs/evidence/domain1-governance-security-tooling-alt-contacts-parent-postchange-20260706.json` |
| Security OU SCP attachments | `docs/evidence/domain1-governance-security-tooling-alt-contacts-security-ou-policies-postchange-20260706.json` |

Postchange validation:

- `SECURITY` alternate contact matched the approved email, name, title, and
  phone number;
- `OPERATIONS` alternate contact matched the approved email, name, title, and
  phone number;
- `BILLING` alternate contact matched the approved email, name, title, and phone
  number;
- `Security Tooling` remained in `Security OU` / `ou-gbyf-mug20ym0`;
- delegated administrators were unchanged from prechange;
- Security OU SCP attachments were unchanged from prechange;
- `account.amazonaws.com` trusted access remained enabled;
- no delegated administration was migrated, no SCPs were changed, no accounts
  were moved, and no services were enabled as part of this change.

## Rollback

If these contact values need to be removed, use a separately approved bounded
rollback with fresh prechange evidence:

```bash
aws account delete-alternate-contact \
  --account-id 668848431187 \
  --alternate-contact-type SECURITY

aws account delete-alternate-contact \
  --account-id 668848431187 \
  --alternate-contact-type OPERATIONS

aws account delete-alternate-contact \
  --account-id 668848431187 \
  --alternate-contact-type BILLING
```

If the contacts should be changed rather than removed, run
`put-alternate-contact` again with the replacement values under a new explicit
approval boundary.

## SAP-C02 Relevance

This closes the account-readiness step before service migration for SAP-C02
Domain 1 governance. It keeps security operations in a dedicated `Security
Tooling` account while preserving the separation between:

- management-account Organizations control-plane duties;
- storage-only `Security Log Archive` duties;
- active delegated security tooling duties that will move to `Security Tooling`
  only through later separately approved changes.
