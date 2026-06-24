# Domain 1 Security Log Archive Account Implementation Boundary - 2026-06-22

<!-- markdownlint-disable MD013 -->

## Scope

This note converts the accepted SAP-C02-preferred logging design into the
smallest implementation boundary that can responsibly lead to later live
changes.

It does not approve any AWS change by itself.

It exists to answer one question clearly:

What is the next correct step if the chosen path is a dedicated security/log
archive account rather than a temporary management-account fallback?

## Status Note

`Security OU` was later created live on 2026-06-22 and is recorded in
`docs/evidence/domain1-governance-security-ou-change-note-20260622.md`.

This note still preserves the pre-creation reasoning and sequencing that led to
that choice. After the OU creation, the next live step becomes the dedicated
security/log archive account creation boundary.

That later account-creation step has now also succeeded on 2026-06-24 and is
recorded in
`docs/evidence/domain1-governance-security-log-archive-account-change-note-20260624.md`.

The new live state is:

- `Security OU` exists under root `r-gbyf`;
- `Security Log Archive` account `955659429518` exists live;
- `account.amazonaws.com` trusted access is enabled for the organization;
- alternate contacts are configured on the new account;
- the new account now sits in `Security OU`;
- the remaining follow-on steps are CloudTrail bucket/KMS and organization-trail
  implementation, not more account-boundary setup.

## Alignment

This note aligns with:

- `docs/planning/sap-c02-readiness-tracker.md`
- `docs/adr/0005-aws-organizations-governance-design.md`
- `docs/planning/domain-1-ou-account-placement-decision-20260621.md`
- `docs/planning/domain-1-cloudtrail-log-archive-design-20260621.md`
- `docs/planning/domain-1-config-guardduty-design-20260621.md`
- `docs/planning/identity-center-permission-set-matrix-20260619.md`
- `docs/runbooks/domain-1-governance-live-readiness-runbook.md`

This supports:

1. SAP-C02 Domain 1 organizational-complexity decisions.
2. Centralized logging and audit-boundary design for the Energy Data Lakehouse.
3. Better revision value by choosing the architecture the exam is more likely
   to prefer.

## Current Live State Rechecked On 2026-06-22

The current management-account read-only checks show that the intended security
boundary is still missing in live AWS.

Command set:

```bash
aws organizations list-organizational-units-for-parent \
  --profile org-admin \
  --parent-id r-gbyf \
  --output json

aws organizations list-accounts \
  --profile org-admin \
  --output json
```

Observed root OU list:

```json
{
  "OrganizationalUnits": [
    {
      "Id": "ou-gbyf-m6ppfmpq",
      "Name": "Lakehouse Workloads OU"
    },
    {
      "Id": "ou-gbyf-zs0f26b5",
      "Name": "Container Sandbox"
    }
  ]
}
```

Observed account list:

```json
{
  "Accounts": [
    {
      "Id": "349687196588",
      "Name": "management-account-alias"
    },
    {
      "Id": "464975959576",
      "Name": "lakehouse-workload-account"
    },
    {
      "Id": "974893866311",
      "Name": "containers-lab.com"
    }
  ]
}
```

Relevant already-recorded live context:

- there is no current CloudTrail trail evidenced in `eu-west-2`;
- there is no current management-account S3 bucket available for a central log
  archive;
- `Lakehouse Workloads OU` exists and the lakehouse account is already placed
  there;
- the root does not yet contain a `Security OU`;
- there is no dedicated security/log archive member account yet.

Interpretation:

- the chosen centralized-logging target architecture is still missing its
  account boundary;
- the next useful implementation step is not CloudTrail itself yet;
- the next useful implementation step is the security boundary that CloudTrail,
  AWS Config, and GuardDuty are all designed to use later.

## Accepted Path

Choose the SAP-C02-preferred target path:

- keep the management account as the control plane;
- use one future dedicated security/log archive member account for the audit
  storage and security-operations boundary;
- avoid the management-account log-bucket fallback except as an intentionally
  approved speed shortcut.

Why this is the preferred path:

- it matches the accepted ADR direction for centralized logging and security
  aggregation;
- it reinforces separation of duties, which is a strong SAP-C02 pattern;
- it creates the natural future home for the CloudTrail bucket, KMS key, AWS
  Config aggregator, and GuardDuty delegated-administrator boundary;
- it aligns with the `SecurityAudit` read-only access model already documented.

Trade-off:

- this path is slower than a temporary management-account fallback because it
  requires more than one live change;
- however, it teaches the more exam-relevant architecture and produces cleaner
  long-term governance evidence.

## Exact Implementation Boundary

Do not jump straight from the current state to organization-trail enablement.

The correct sequence is:

1. create `Security OU` under root `r-gbyf`;
2. create one dedicated member account for security/log archive use;
3. move that new account into `Security OU`;
4. only then package the CloudTrail bucket/KMS and organization-trail live
   change units;
5. after the logging boundary exists, continue to AWS Config aggregation and
   GuardDuty delegated administration.

This respects the runbook's rule that OU changes, account-boundary changes,
CloudTrail setup, and later security-service enablement should not be bundled
into one apply.

## Recommended Next Live Change Unit

The next narrow live step should be:

`Create Security OU`

Reason:

- it has a smaller blast radius than account creation;
- it is easier to validate;
- it is easier to roll back while empty;
- it creates the intended destination boundary before the durable account
  creation step;
- it does not depend on the still-missing new-account email address.

This makes `Create Security OU` the best next implementation unit even though
the longer-term goal is the dedicated security/log archive account.

## Later Account-Creation Shape

Once `Security OU` exists, the next bounded change can create the dedicated
member account.

Recommended account purpose:

- display purpose: centralized audit logging and future security aggregation;
- target services later: CloudTrail log archive bucket, KMS key, AWS Config
  aggregator, GuardDuty delegated administration, and possibly Security Hub;
- human access model: `SecurityAudit`, limited log administration, and
  break-glass only.

Recommended account name:

- `Security Log Archive`

This name is descriptive enough for SAP-C02 reasoning and clear enough for
later evidence review.

## Explicit Blockers Before Account Creation

The architecture choice is now made, but account creation still has open
non-obvious inputs that should not be guessed:

1. the exact email address to use for the new AWS account;
2. the exact owner/recovery mailbox decision for that account;
3. whether alternate contacts should be configured immediately after account
   creation as a separate bounded step;
4. whether the account should first remain under root briefly after creation or
   be moved into `Security OU` in the same working session under separate
   approval.

These are not date blockers. They are real implementation inputs.

## Rollback And Risk Notes

### Security OU creation

Rollback is simple while the OU is empty:

- validate no accounts are inside it;
- delete the OU if the naming or boundary decision changes.

### Account creation

Rollback is not symmetrical with creation:

- a new account is a durable organizational object, not a lightweight toggle;
- immediate "undo" is not equivalent to deleting an SCP or deleting an empty
  OU;
- if created incorrectly, the safer recovery path is typically to stop using
  it, move it to a holding boundary such as `Suspended OU` if later available,
  or follow an account-closure path deliberately.

That rollback asymmetry is one more reason to create the `Security OU` first
and keep the actual account creation in its own later change note.

## Validation For The Next Two Live Notes

### Security OU creation

Validation should confirm:

- `list-organizational-units-for-parent --parent-id r-gbyf` shows `Security OU`;
- no existing OU is renamed or disturbed unintentionally;
- the management account remains attached directly to root.

### Security/log archive account creation

Validation should confirm:

- the new account appears in `list-accounts`;
- the account reaches `ACTIVE` state;
- the intended email and display name match the approved values;
- the account can then be moved into `Security OU` under a separate change
  boundary.

## Cost And Operational Impact

- There is no cost impact from this planning note itself.
- `Security OU` creation has no direct service cost.
- A new member account has governance and operational overhead even before
  CloudTrail, S3, KMS, Config, or GuardDuty charges begin.
- The dedicated-account path is still the more defensible SAP-C02 answer
  because it separates billing/control-plane administration from audit-log
  ownership.

## Practical Outcome

This note resolves the earlier ownership-path ambiguity.

The path is now:

- target a future dedicated security/log archive account;
- do not use the management-account fallback as the default path;
- treat `Create Security OU` as the next live implementation unit;
- treat dedicated account creation as the following live unit once the exact
  email and owner inputs are approved.
