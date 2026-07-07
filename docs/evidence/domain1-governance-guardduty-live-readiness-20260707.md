# Domain 1 GuardDuty Live-Readiness Evidence - 2026-07-07

<!-- markdownlint-disable MD013 -->

## Status

Read-only live-readiness evidence collected.

No GuardDuty live change was performed:

- GuardDuty was not enabled in any account.
- No GuardDuty delegated administrator was designated.
- No GuardDuty organization configuration was changed.
- No member accounts were associated.
- No optional GuardDuty protection plans were enabled.
- Security Hub, OAM, SCPs, account placement, AWS Config, and workload resources
  were not changed.

## Approval Boundary

User approval for this slice:

> Explicit Approval granted: Proceed to GuardDuty live-readiness

This was treated as approval to collect fresh read-only evidence and close the
implementation-readiness boundary. It was not treated as approval to enable
GuardDuty or configure delegated administration.

## Intended GuardDuty Target

The accepted target remains:

- delegated administrator: `Security Tooling` (`668848431187`);
- Region: `eu-west-2`;
- coverage: foundational GuardDuty for active organization accounts;
- exclusions: keep `so-aws-admin` (`054394900225`) excluded while it remains on
  the decommission path;
- optional protection plans: disabled unless a later workload-specific value and
  cost decision approves them.

## Fresh Prechange Evidence

| Evidence | File |
|---|---|
| Management caller identity | `docs/evidence/domain1-governance-guardduty-live-readiness-management-sts-prechange-20260707.json` |
| Sanitized Organizations account inventory | `docs/evidence/domain1-governance-guardduty-live-readiness-organizations-accounts-sanitized-prechange-20260707.json` |
| Account parent mapping | `docs/evidence/domain1-governance-guardduty-live-readiness-parent-349687196588-prechange-20260707.json`; `docs/evidence/domain1-governance-guardduty-live-readiness-parent-668848431187-prechange-20260707.json`; `docs/evidence/domain1-governance-guardduty-live-readiness-parent-955659429518-prechange-20260707.json`; `docs/evidence/domain1-governance-guardduty-live-readiness-parent-464975959576-prechange-20260707.json`; `docs/evidence/domain1-governance-guardduty-live-readiness-parent-974893866311-prechange-20260707.json`; `docs/evidence/domain1-governance-guardduty-live-readiness-parent-054394900225-prechange-20260707.json` |
| Enabled Organizations service access | `docs/evidence/domain1-governance-guardduty-live-readiness-org-service-access-prechange-20260707.json` |
| Current delegated administrators | `docs/evidence/domain1-governance-guardduty-live-readiness-org-delegated-admins-all-prechange-20260707.json`; `docs/evidence/domain1-governance-guardduty-live-readiness-guardduty-delegated-admins-prechange-20260707.json` |
| GuardDuty organization admin state | `docs/evidence/domain1-governance-guardduty-live-readiness-management-list-organization-admin-accounts-prechange-20260707.json` |
| GuardDuty detector state | `docs/evidence/domain1-governance-guardduty-live-readiness-management-detectors-prechange-20260707.json`; `docs/evidence/domain1-governance-guardduty-live-readiness-security-tooling-detectors-prechange-20260707.json`; `docs/evidence/domain1-governance-guardduty-live-readiness-security-log-archive-detectors-prechange-20260707.json`; `docs/evidence/domain1-governance-guardduty-live-readiness-lakehouse-workload-detectors-default-profile-prechange-20260707.json`; `docs/evidence/domain1-governance-guardduty-live-readiness-container-sandbox-detectors-prechange-20260707.json` |
| Access caveats | `docs/evidence/domain1-governance-guardduty-live-readiness-lakehouse-workload-assume-role-prechange-20260707.status`; `docs/evidence/domain1-governance-guardduty-live-readiness-lakehouse-workload-assume-role-prechange-20260707.err`; `docs/evidence/domain1-governance-guardduty-live-readiness-so-aws-admin-assume-role-prechange-20260707.status`; `docs/evidence/domain1-governance-guardduty-live-readiness-so-aws-admin-assume-role-prechange-20260707.err` |

## Readiness Findings

Fresh evidence shows:

- `guardduty.amazonaws.com` is not currently enabled as an Organizations service
  access principal.
- Organizations has no GuardDuty delegated administrator.
- `aws guardduty list-organization-admin-accounts` returns no admin accounts in
  `eu-west-2`.
- No GuardDuty detector exists in the checked `eu-west-2` accounts:
  management, Security Tooling, Security Log Archive, lakehouse workload, and
  container sandbox.
- Because no detector exists, there is no current GuardDuty organization
  configuration, member list, auto-enable posture, or optional protection-plan
  state to preserve.
- `Security Tooling` remains the correct target delegated administrator because
  it is now the AWS Config delegated administrator and recorder-bearing security
  tooling account.
- `so-aws-admin` remains excluded because it is on the decommission path.

Access caveats:

- The management-account `org-admin` profile could not assume
  `OrganizationAccountAccessRole` into the lakehouse workload account, so the
  lakehouse detector check was captured through the existing local lakehouse
  profile instead.
- The management-account `org-admin` profile could not assume
  `OrganizationAccountAccessRole` into `so-aws-admin`; this is not a blocker for
  GuardDuty live-readiness because that account remains excluded on the
  decommission path.

## Implementation Boundary If Later Approved

The next live GuardDuty change should be a separate implementation step with
explicit approval. The recommended bounded sequence is:

1. Confirm this evidence is still fresh.
2. Enable or verify a GuardDuty detector in `Security Tooling` in `eu-west-2`.
3. Designate `Security Tooling` as the GuardDuty delegated administrator.
4. Configure foundational organization coverage for the approved active accounts.
5. Keep `so-aws-admin` excluded while it remains on the decommission path.
6. Leave optional protection plans disabled.
7. Verify delegated administrator, detector, member, organization configuration,
   and cost-observation follow-up state.

The live implementation approval should explicitly exclude:

- Security Hub;
- OAM;
- SCP changes;
- AWS Config scope changes;
- account moves or retirement actions;
- workload resource changes;
- optional GuardDuty protection plans.

## Cost And Scope Posture

Cost posture for first enablement:

- one Region only: `eu-west-2`;
- foundational GuardDuty only;
- optional protection plans off by default;
- post-enable cost observation required before expanding Regions or protection
  plans.

This keeps the first live change focused on delegated-admin and foundational
threat-detection posture rather than a broad security-service rollout.

## SAP-C02 Relevance

This supports SAP-C02 Domain 1 by demonstrating the difference between:

- planning a delegated security administrator;
- collecting read-only prechange evidence;
- enabling a Regional security service;
- configuring organization-wide member coverage;
- separating foundational security coverage from optional cost-expanding
  protection plans.
