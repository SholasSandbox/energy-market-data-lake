# Domain 1 Governance Live-Readiness Runbook

<!-- markdownlint-disable MD013 -->

## Purpose

This runbook turns the accepted Domain 1 governance design into the smallest
useful implementation-ready package without authorizing any live AWS change by
itself.

It exists to support:

1. SAP-C02 Domain 1 readiness for organizational complexity, centralized
   logging, audit protection, delegated administration, and governance
   sequencing.
2. The Energy Data Lakehouse case study as a credible multi-account AWS design.
3. Near-term cloud architect positioning through explicit blast-radius,
   rollback, validation, and cost thinking.

It aligns with:

- `docs/planning/sap-c02-readiness-tracker.md`
- `docs/planning/domain-1-governance-preflight-20260618.md`
- `docs/adr/0005-aws-organizations-governance-design.md`
- `docs/planning/domain-1-cloudtrail-log-archive-design-20260621.md`
- `docs/planning/domain-1-config-guardduty-design-20260621.md`

## Boundary

This runbook is still **repo-only preparation**.

It does not:

- approve Organizations, SCP, Identity Center, CloudTrail, AWS Config,
  GuardDuty, OAM, Security Hub, S3, KMS, or budget changes;
- replace explicit approval for one live change at a time;
- override the tracker or ADR implementation boundary;
- treat a future start date as the only blocker.

The blocker for the next state must be one of:

- unresolved architecture or account-boundary design;
- an explicit approval dependency for a design decision that carries
  non-obvious consequences, cannot be responsibly assumed, or has not yet been
  recorded in an ADR or equivalent design artifact with explicit trade-offs;
- missing rollback or validation path;
- missing cost or blast-radius understanding;
- missing prerequisite state from an earlier governance step.

If a design decision needs explicit approval, the related ADR should
acknowledge:

- the winning choice;
- the meaningful alternatives considered;
- the trade-offs that made the winning choice preferable here;
- why the rejected options were not chosen for this repository context; and
- the conditions that would cause the decision to be revisited.

## Change-Unit Rule

Use **one approval boundary per change unit**.

Do not bundle these into one apply:

- OU/account placement changes
- CloudTrail organization trail enablement
- central log-bucket and KMS setup
- SCP attachment
- AWS Config recorder or aggregator enablement
- GuardDuty delegated-admin designation
- OAM / cross-account observability link setup
- Security Hub adoption
- budget creation or threshold changes

Each change unit needs its own evidence note or approved execution section.

## Required Contents For Any Live Change Note

ADR 0005 requires every live governance change note to state:

1. the exact target account and OU;
2. the current state from read-only AWS CLI commands;
3. the proposed change;
4. the expected blast radius;
5. the rollback path;
6. the validation command or console check;
7. the cost impact;
8. the explicit approval for that one change.

Use the template in this runbook before any live governance execution.

If the live change depends on a design approval, cite the ADR or equivalent
design artifact that records those trade-offs before requesting execution
approval.

## Evidence File Naming

Use bounded evidence names so each change stays reviewable:

- `docs/evidence/domain1-governance-<control>-prechange-YYYYMMDD.json`
- `docs/evidence/domain1-governance-<control>-plan-YYYYMMDD.txt`
- `docs/evidence/domain1-governance-<control>-postchange-YYYYMMDD.json`
- `docs/evidence/domain1-governance-<control>-summary-YYYYMMDD.md`

Examples:

- `domain1-governance-org-inventory-prechange-20260713.json`
- `domain1-governance-cloudtrail-postchange-20260713.json`
- `domain1-governance-guardduty-summary-20260720.md`

## Read-Only Baseline Capture

Capture current state before any live change. Save outputs under
`docs/evidence/`.

### Shell Context Check

Before running management-account Organizations commands, confirm the shell is
using the intended management-account credentials rather than a workload-account
default profile.

Helpful pattern:

```bash
aws sts get-caller-identity --output json

aws sts get-caller-identity --profile org-admin --output json

export AWS_PROFILE=org-admin
aws sts get-caller-identity --output json
```

In this workspace, `org-admin` is the known working management-account SSO
profile. If that name changes later, use the equivalent management-account SSO
profile instead.

If the default shell identity is a member or workload account, management-only
Organizations commands such as `list-roots` and `list-accounts` can fail with
`AccessDeniedException` even when a valid management-account SSO profile already
exists locally.

If the intended SSO profile is expired, refresh it first:

```bash
aws sso login --profile org-admin
```

Set a common shell context first:

```bash
cd /Users/[redacted-user]/Workspace/cloud-projects/energy-market-data-lake

export AWS_REGION=eu-west-2
export EVIDENCE_DATE="$(date +%Y%m%d)"
export AWS_PROFILE=org-admin
```

### 1. Organizations And Account Inventory

Run from the Organizations management account:

```bash
aws sts get-caller-identity --output json \
  > "docs/evidence/domain1-governance-sts-prechange-${EVIDENCE_DATE}.json"

aws organizations describe-organization --output json \
  > "docs/evidence/domain1-governance-org-description-prechange-${EVIDENCE_DATE}.json"

aws organizations list-roots --output json \
  > "docs/evidence/domain1-governance-org-roots-prechange-${EVIDENCE_DATE}.json"

aws organizations list-accounts --output json \
  > "docs/evidence/domain1-governance-org-accounts-prechange-${EVIDENCE_DATE}.json"

aws organizations list-aws-service-access-for-organization --output json \
  > "docs/evidence/domain1-governance-org-service-access-prechange-${EVIDENCE_DATE}.json"
```

If OUs already exist, also capture:

```bash
aws organizations list-organizational-units-for-parent \
  --parent-id "<root-or-ou-id>" \
  --output json
```

### 2. CloudTrail And Central Log-Archive Baseline

Run from the management account in the trail home Region:

```bash
aws cloudtrail list-trails --region "${AWS_REGION}" --output json \
  > "docs/evidence/domain1-governance-cloudtrail-list-prechange-${EVIDENCE_DATE}.json"

aws cloudtrail get-trail \
  --name "<trail-name-or-arn>" \
  --region "${AWS_REGION}" \
  --output json

aws cloudtrail get-trail-status \
  --name "<trail-name-or-arn>" \
  --region "${AWS_REGION}" \
  --output json
```

If a target log bucket already exists, capture:

```bash
aws s3api get-bucket-versioning --bucket "<log-bucket-name>"
aws s3api get-bucket-encryption --bucket "<log-bucket-name>"
aws s3api get-public-access-block --bucket "<log-bucket-name>"
aws s3api get-bucket-policy --bucket "<log-bucket-name>"
```

If a target KMS key already exists, capture:

```bash
aws kms describe-key --key-id "<kms-key-arn>"
aws kms get-key-policy --key-id "<kms-key-arn>" --policy-name default
```

### 3. AWS Config Baseline

Run in each in-scope account and Region where recorder state matters:

```bash
aws configservice describe-configuration-recorders --output json
aws configservice describe-configuration-recorder-status --output json
aws configservice describe-delivery-channels --output json
aws configservice describe-config-rules --output json
aws configservice describe-configuration-aggregators --output json
```

The aggregator command matters most in the chosen aggregator account and Region.

### 4. GuardDuty Baseline

Run from the management account in each in-scope Region:

```bash
aws guardduty list-organization-admin-accounts --output json
aws guardduty list-detectors --output json
```

If a delegated administrator already exists, also capture from that
administrator account in each in-scope Region:

```bash
aws guardduty describe-organization-configuration \
  --detector-id "<detector-id>" \
  --output json

aws guardduty list-members \
  --detector-id "<detector-id>" \
  --output json
```

## Public Evidence Redaction Gate

This repository is public. Do not commit exact personal contact values, mailbox
addresses, phone numbers, postal addresses, or local user home paths as
governance evidence.

Use this split for every live governance change:

- private evidence store: exact AWS CLI JSON exports and exact contact values;
- public repository: sanitized evidence summaries, redacted JSON snapshots, and
  implementation outcomes.

Before staging governance evidence, run:

```bash
scripts/check_public_evidence_redaction.sh
```

If the check fails, move the exact file into a private evidence store and commit
only a redacted summary or redacted copy.

History cleanup caveat:

- A repository history rewrite removes the exposed values from refs controlled
  by this repository, but it does not guarantee removal from GitHub caches,
  forks, or other local clones.
- After any public exposure of personal contact data, treat the values as
  exposed and rotate or replace them where practical.
- If the exposed data must be purged beyond this repository's refs, follow the
  current GitHub sensitive-data removal process and contact GitHub Support for
  cache cleanup where needed.
- Keep this caveat visible in the tracker until the next governance evidence
  commit has passed the redaction check.

## Ordered Implementation Sequence

Move through these states in order. Do not skip prerequisites just because a
calendar date is later than today.

| State | Ready when | Stop if |
|---|---|---|
| Organizations inventory evidence | Current account, OU, and service-access state is captured read-only | The acting account is unclear or inventory evidence is missing |
| Public evidence redaction | Exact contact values and local user paths are absent from public evidence; private raw evidence is stored outside this repository | Raw email, phone, address, personal name, or local user path appears in the public tree |
| Final OU and account-placement decision | Target OU names and target accounts are explicit | Account placement is still ambiguous |
| Account alternate-contact readiness | SECURITY, OPERATIONS, and BILLING contacts are defined for newly active governance accounts before service migration | Contact values or notification ownership are unclear |
| Identity Center assignment plan | Target permission sets and account assignments are explicit | Role boundaries are still unclear |
| Break-glass implementation note | Named principal, alerting path, and review cadence are explicit | Emergency access is not auditable |
| CloudTrail and log archive change note | Target trail owner, bucket, KMS posture, retention, rollback, and validation are explicit | Bucket/KMS ownership or delete protection is unresolved |
| SCP attachment note | Exact SCP, target OU/account, service exceptions, rollback, and test case are explicit | Exceptions or rollback are unclear |
| AWS Config change note | Recorder scope, exclusions, Region set, aggregator account, and validation are explicit | Region scope or cost posture is unclear |
| GuardDuty change note | Delegated admin, Region set, foundational coverage, and optional-plan stance are explicit | Region consistency or admin account choice is unclear |
| OAM review note | Monitoring account, source accounts, telemetry types, Region scope, and access model are explicit | It is being confused with CloudTrail retention or Config compliance aggregation |
| Security Hub review note | Config and GuardDuty decisions are stable enough to revisit adoption | Underlying recorder/admin decisions are still moving |
| Budget and tag-policy change note | Account-level thresholds and notification targets are explicit | Thresholds are still arbitrary or unsupported |

## Per-Control Validation Focus

Use these as the minimum validation questions for each change note.

### OU Or Account Placement

- Does the account appear under the intended root or OU?
- Are no unrelated accounts moved?
- Can the prior placement be restored quickly if needed?
- If `move-account` returns successfully but the immediate follow-up reads still
  show the old parent, wait briefly and retry because AWS Organizations can show
  short propagation delay after a successful move.

### CloudTrail And Log Archive

- Is the trail an organization trail?
- Is logging active in the intended home Region?
- Does the bucket policy constrain CloudTrail access correctly?
- Are versioning, public-access block, and encryption in the expected state?

### SCP Attachment

- Is the SCP attached only to the intended root, OU, or account?
- Does the expected deny behavior occur for the test case?
- Are approved service exceptions still working?

### Account Alternate Contacts

- Are `SECURITY`, `OPERATIONS`, and `BILLING` contacts present for the target
  governance account?
- Are the contact values durable, monitored, explicitly approved, and stored
  only in private evidence?
- Are public evidence files redacted while still proving the contact type and
  verification outcome?
- Is `account.amazonaws.com` trusted access still enabled if contacts are
  managed from the Organizations management account?
- Is the contact step separate from delegated-admin migration and service
  enablement?

### AWS Config

- Is the recorder present and recording in the intended Region?
- Is the delivery channel healthy?
- Does the aggregator appear in the intended account and Region?
- Are only the intended rules or exclusions present?

### GuardDuty

- Is the delegated administrator the intended account in each enabled Region?
- Is foundational coverage enabled for the intended member accounts?
- Are optional protection plans still in the approved state?

### OAM

- Is the monitoring account the intended `Security Tooling` or central
  monitoring account?
- Are source accounts, telemetry types, and Region scope explicit?
- Is the design separate from CloudTrail log retention and AWS Config
  compliance aggregation?

### Budgets And Tag Policies

- Are the thresholds the approved ones?
- Do alerts route to the intended recipients?
- Do tag-policy decisions avoid breaking current workload tagging patterns?

## Change-Note Template

Use this exact structure in a future change note or approval section:

```markdown
# Domain 1 Governance Change Note - <control> - <date>

## Target Account And OU

- Account:
- OU:
- Region:

## Current State

- Evidence files:
- Read-only commands used:

## Proposed Change

- Exact change:
- Why now:

## Expected Blast Radius

- Services affected:
- Accounts affected:
- Possible user-visible impact:

## Rollback Path

- Immediate rollback command or console action:
- Evidence to confirm rollback:

## Validation

- Commands:
- Console checks:
- Success criteria:

## Cost Impact

- Expected recurring cost:
- One-time cost or operational cost:

## Approval

- Approver:
- Approval date:
- Scope of approval:
```

## Practical Next Use

The first live governance note should usually be one of these, in this order:

1. Organizations read-only inventory and final OU/account-placement decision.
2. Organization CloudTrail plus central log-archive setup.
3. One SCP attachment with narrow blast radius and explicit exceptions.
4. AWS Config recorder plus aggregator enablement.
5. GuardDuty delegated-admin designation and foundational coverage.

As of 2026-07-07, the current Security Tooling AWS Config delegated-admin,
aggregator, recorder, and organization-rule inclusion sequence is complete in
`eu-west-2`; GuardDuty live-readiness evidence has also been collected. The next
security-service transition is a separately approved GuardDuty implementation
boundary.

OAM and Security Hub should remain later review notes unless the account
boundary, Config scope, and GuardDuty scope are already stable.

## References

- AWS Organizations `list-accounts` CLI reference:
  `https://docs.aws.amazon.com/cli/latest/reference/organizations/list-accounts.html`
- AWS Organizations `list-aws-service-access-for-organization` CLI reference:
  `https://docs.aws.amazon.com/cli/latest/reference/organizations/list-aws-service-access-for-organization.html`
- Creating a trail for an organization:
  `https://docs.aws.amazon.com/awscloudtrail/latest/userguide/creating-trail-organization.html`
- Managing trails with the AWS CLI:
  `https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-additional-cli-commands.html`
- Multi-account, multi-Region data aggregation for AWS Config:
  `https://docs.aws.amazon.com/config/latest/developerguide/aggregate-data.html`
- Viewing aggregators for AWS Config:
  `https://docs.aws.amazon.com/config/latest/developerguide/aggregated-view.html`
- Authorizing aggregator accounts for AWS Config:
  `https://docs.aws.amazon.com/config/latest/developerguide/aggregated-add-authorization.html`
- Recording resources with the AWS CLI for AWS Config:
  `https://docs.aws.amazon.com/config/latest/developerguide/select-resources-cli.html`
- Designating a delegated GuardDuty administrator account:
  `https://docs.aws.amazon.com/guardduty/latest/ug/delegated-admin-designate.html`
- Changing the delegated GuardDuty administrator account:
  `https://docs.aws.amazon.com/guardduty/latest/ug/change-guardduty-delegated-admin.html`
- CloudWatch cross-account observability:
  `https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Unified-Cross-Account.html`
