# Domain 1 Governance Change Note - Enable Account Management Trusted Access And Set Alternate Contacts - 2026-06-24

<!-- markdownlint-disable MD013 -->

## Target Account And OU

- Management account: `349687196588` / `management-account-alias`
- Target member account: `955659429518` / `Security Log Archive`
- Current parent during this change: root `r-gbyf`
- Trusted access service principal: `account.amazonaws.com`

## Current State

Before this change:

- the organization service-access list included `sso.amazonaws.com` only;
- `account.amazonaws.com` trusted access was not yet enabled;
- the new `Security Log Archive` account already existed under root
  `r-gbyf`;
- no SECURITY, OPERATIONS, or BILLING alternate contacts were set for account
  `955659429518`;
- the user had reserved
  `[redacted-email]` for alternate-contact/log-archive
  use.

Evidence files:

- `docs/evidence/domain1-governance-account-management-service-access-prechange-20260624.json`
- `docs/evidence/domain1-governance-security-log-archive-alt-contact-security-prechange-20260624.txt`
- `docs/evidence/domain1-governance-security-log-archive-alt-contact-operations-prechange-20260624.txt`
- `docs/evidence/domain1-governance-security-log-archive-alt-contact-billing-prechange-20260624.txt`
- `docs/evidence/domain1-governance-security-log-archive-account-contact-information-20260624.json`

## Proposed Change

1. Enable trusted access for AWS Account Management using
   `account.amazonaws.com`.
2. Set all three alternate contact types for account `955659429518` using
   `[redacted-email]`.

Contact values applied:

- name: `Security Log Archive`
- phone number: `[redacted-phone]`
- titles:
  - `Security Contact`
  - `Operations Contact`
  - `Billing Contact`

Why now:

- centralized alternate-contact management for organization accounts depends on
  trusted access for AWS Account Management;
- alternate contacts reduce recovery and notification risk before later
  CloudTrail, KMS, and service enablement work;
- this keeps contact governance separate from both account creation and OU
  placement.

## Expected Blast Radius

- enables one additional Organizations-integrated service principal:
  `account.amazonaws.com`;
- changes only account-contact metadata for the target member account;
- does not change IAM, SCP attachments, CloudTrail, S3, KMS, AWS Config, or
  GuardDuty;
- does not move the account.

## Rollback Path

For alternate contacts:

- replace or clear them later only through separate approved contact-management
  changes.

For trusted access:

- disabling trusted access would be a separate Organizations change and should
  only happen if no ongoing centralized Account Management features depend on
  it.

Validation of rollback posture:

- treat alternate-contact changes as durable account-governance metadata, not
  disposable test data;
- if a later contact update is needed, record that as a separate bounded
  change note.

## Validation

Commands used:

```bash
aws organizations enable-aws-service-access \
  --profile org-admin \
  --service-principal account.amazonaws.com

aws organizations list-aws-service-access-for-organization \
  --profile org-admin \
  --output json

aws account get-contact-information \
  --profile org-admin \
  --account-id 955659429518 \
  --output json

aws account put-alternate-contact \
  --profile org-admin \
  --account-id 955659429518 \
  --alternate-contact-type SECURITY \
  --email-address [redacted-email] \
  --name 'Security Log Archive' \
  --phone-number '[redacted-phone]' \
  --title 'Security Contact'

aws account put-alternate-contact \
  --profile org-admin \
  --account-id 955659429518 \
  --alternate-contact-type OPERATIONS \
  --email-address [redacted-email] \
  --name 'Security Log Archive' \
  --phone-number '[redacted-phone]' \
  --title 'Operations Contact'

aws account put-alternate-contact \
  --profile org-admin \
  --account-id 955659429518 \
  --alternate-contact-type BILLING \
  --email-address [redacted-email] \
  --name 'Security Log Archive' \
  --phone-number '[redacted-phone]' \
  --title 'Billing Contact'

aws account get-alternate-contact \
  --profile org-admin \
  --account-id 955659429518 \
  --alternate-contact-type SECURITY \
  --output json

aws account get-alternate-contact \
  --profile org-admin \
  --account-id 955659429518 \
  --alternate-contact-type OPERATIONS \
  --output json

aws account get-alternate-contact \
  --profile org-admin \
  --account-id 955659429518 \
  --alternate-contact-type BILLING \
  --output json
```

Success criteria:

- `account.amazonaws.com` appears in the enabled service-principal list;
- SECURITY, OPERATIONS, and BILLING alternate contacts all resolve
  successfully for account `955659429518`;
- the applied alternate-contact email is
  `[redacted-email]`.

## Cost Impact

- No direct Organizations charge is expected from enabling trusted access.
- No direct charge is expected from alternate-contact updates.
- Small governance overhead increases because Account Management trusted access
  is now part of the organization-integrated services surface.

## Approval

- Approval source: direct user instruction
- Approval text: `Explicit approval granted, proceed with Enable account.amazonaws.com trusted access, then set alternate contacts using [redacted-email].`
- Approval date: 2026-06-24
- Scope of approval: enable `account.amazonaws.com` trusted access and set the
  three alternate contact types for account `955659429518`

## Result

The trusted-access and alternate-contact change succeeded.

Post-change evidence:

- `docs/evidence/domain1-governance-account-management-service-access-postchange-20260624.json`
- `docs/evidence/domain1-governance-security-log-archive-alt-contact-security-postchange-20260624.json`
- `docs/evidence/domain1-governance-security-log-archive-alt-contact-operations-postchange-20260624.json`
- `docs/evidence/domain1-governance-security-log-archive-alt-contact-billing-postchange-20260624.json`

Current interpretation:

- `account.amazonaws.com` is now enabled for the organization;
- all three alternate contact types are now present on
  `Security Log Archive` account `955659429518`;
- the later account move and logging setup remained separate change units.
