# Domain 1 Governance Change Note - First Organization AWS Config CloudTrail Rule - 2026-06-25

<!-- markdownlint-disable MD013 -->

## Status Note

This first organization-rule step was executed live on 2026-06-25 under
explicit user approval.

The resulting live state is:

- organization AWS Config rule `org-multi-region-cloudtrail-enabled` now
  exists in the delegated-admin security account in `eu-west-2`;
- the rule uses managed rule identifier `MULTI_REGION_CLOUD_TRAIL_ENABLED`
  with inputs tied to the accepted CloudTrail design:
  central archive bucket `org-cloudtrail-log-archive-955659429518-eu-west-2`,
  management events included, and `readWriteType=ALL`;
- the initial organization-wide create attempt exposed a real structural gap in
  sandbox account `974893866311`: `NoAvailableConfigurationRecorder`;
- the rule was then updated to exclude `974893866311`, which matches the
  current design decision that sandbox AWS Config recorder scope remains open
  and only applies where in scope;
- a follow-on re-check exposed a second structural blocker in the management
  account: delegated-admin deployment could not assume
  `AWSServiceRoleForConfigMultiAccountSetup` because that service-linked role
  did not yet exist in the organization management account;
- the missing management-account service-linked role was then created directly
  in account `349687196588`, and the same sandbox-excluded organization rule
  was retried successfully;
- after the sandbox recorder baseline and central archive policy extension were
  completed, the same organization rule was updated again so sandbox account
  `974893866311` was no longer excluded;
- final detailed status now shows `UPDATE_SUCCESSFUL` in all four intended
  in-scope accounts:
  `349687196588`, `464975959576`, `955659429518`, and `974893866311`;
- the security account's local Config rule is `ACTIVE` and currently
  `COMPLIANT`;
- the management account's local Config rule is now `ACTIVE` and currently
  `COMPLIANT`;
- the sandbox account's local organization-managed rule is now `ACTIVE` and
  currently `COMPLIANT`;
- overall organization-rule status is now `UPDATE_SUCCESSFUL`;
- no additional Config rule families were enabled in this change.

## Target Accounts And Scope

- Delegated-admin account: `955659429518` / `Security Log Archive`
- Organization rule home Region: `eu-west-2`
- Final in-scope accounts after bounded rollout:
  `349687196588`, `464975959576`, `955659429518`, `974893866311`

## Current State

Before this step:

- no organization AWS Config rules existed in the delegated-admin account;
- no organization rule statuses existed;
- the delegated-admin account already had
  `AWSServiceRoleForConfigMultiAccountSetup`;
- the security account already hosted the organization aggregator and its own
  local recorder;
- the management account already had an organization CloudTrail named
  `organization-management-events` that was multi-Region, logged management
  events, used the central archive bucket, and had log file validation enabled;
- the sandbox account was still outside the recorder baseline.
- the organization management account did not yet have
  `AWSServiceRoleForConfigMultiAccountSetup`.

Evidence files:

- `docs/evidence/domain1-governance-config-org-rule-security-sts-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-org-rules-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-org-rule-statuses-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-org-rule-multiaccount-slr-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-org-rule-cloudtrail-security-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-org-rule-cloudtrail-security-status-prechange-20260625.json`
- `docs/evidence/domain1-governance-config-security-recorder-change-note-20260625.md`

## Proposed Change

Create the smallest defensible first organization AWS Config rule:

1. use the delegated-admin security account to create one organization managed
   rule in `eu-west-2`;
2. start with a CloudTrail detective control rather than a broader standards
   rollout;
3. require at least one multi-Region CloudTrail that includes management
   events and writes to the central archive bucket;
4. if the initial org-wide deployment exposes a sandbox recorder gap, narrow
   the rule by excluding the sandbox account rather than widening this bounded
   step into sandbox recorder rollout;
5. once sandbox recorder prerequisites are completed separately, remove the
   exclusion and re-verify organization deployment across every intended
   account.

## Why This Boundary

This remains the smallest useful next AWS Config move:

- it converts the recorder baseline into a real detective control without
  jumping into multiple rule families;
- it uses organization deployment from the delegated-admin security account,
  which is the cleaner SAP-C02-style governance path than hand-creating
  separate local rules first;
- it turns the earlier sandbox exclusion into a temporary sequencing control
  rather than a permanent governance gap;
- it turns a real deployment failure into explicit evidence instead of hiding
  it.

## Trade-Offs

Accepted in this boundary:

- choose one CloudTrail-focused organization rule first instead of several
  starter rules at once;
- use `MULTI_REGION_CLOUD_TRAIL_ENABLED` rather than the weaker
  `CLOUD_TRAIL_ENABLED` because the accepted CloudTrail design is already
  multi-Region and management-event focused;
- exclude the sandbox account first instead of widening the first failed rule
  step into sandbox recorder rollout;
- after sandbox recorder rollout is complete, re-include the account in the
  same organization rule rather than creating a sandbox-only duplicate rule.

Rejected for this boundary:

- enabling CloudTrail CloudWatch Logs or encryption rules at the same time,
  because that would widen troubleshooting scope;
- onboarding the sandbox recorder in this same step, because the current design
  still leaves sandbox Config scope open after cost observation;
- leaving the sandbox account permanently excluded after its recorder baseline
  and central archive permissions were completed, because that would leave an
  unnecessary governance gap in an intended workload-bearing account;
- leaving the management-account service-linked-role blocker unresolved after it
  was identified, because that would leave the first organization rule in a
  half-deployed state.

## Expected Blast Radius

- creates one organization AWS Config managed rule in the delegated-admin
  security account;
- creates organization-managed local Config rule instances in the in-scope
  accounts as AWS finishes deployment;
- creates `AWSServiceRoleForConfigMultiAccountSetup` in the management account
  to unblock delegated-admin deployment into that account;
- removes the temporary sandbox exclusion once sandbox recorder prerequisites
  exist;
- does not create additional GuardDuty state or additional Config rules.

## Rollback Path

If this first organization rule needs to be removed:

1. delete organization rule `org-multi-region-cloudtrail-enabled`;
2. verify that the organization-managed local rule instances are removed from
   all four intended accounts;
3. leave the underlying recorders, delivery channels, aggregator, and
   CloudTrail organization trail intact;
4. reassess whether the next attempt should stay organization-wide, remain
   temporarily sandbox-excluded again, or follow a different first-rule path.

## Validation

Observed validation after the initial create attempt:

- the rule object was created:
  `docs/evidence/domain1-governance-config-org-rule-postchange-20260625.json`;
- the first detailed-status capture exposed sandbox account `974893866311`
  failure `NoAvailableConfigurationRecorder`:
  `docs/evidence/domain1-governance-config-org-rule-detailed-status-postchange-20260625.json`;
- the first organization status remained `CREATE_IN_PROGRESS` while accounts
  converged:
  `docs/evidence/domain1-governance-config-org-rule-statuses-postchange-20260625.json`.

Observed validation after the bounded exclusion update:

- the rule now explicitly excludes `974893866311`:
  `docs/evidence/domain1-governance-config-org-rule-postexclude-20260625.json`;
- lakehouse and security accounts initially reported `UPDATE_SUCCESSFUL`, while
  the management account first remained `UPDATE_IN_PROGRESS` and then surfaced
  a delegated-admin service-linked-role blocker:
  `docs/evidence/domain1-governance-config-org-rule-detailed-status-finalcheck-20260625.json`;
- the corresponding organization-rule status first remained
  `UPDATE_IN_PROGRESS`:
  `docs/evidence/domain1-governance-config-org-rule-statuses-finalcheck-20260625.json`;

Observed validation after resolving the management-account blocker and retrying:

- the management account initially lacked
  `AWSServiceRoleForConfigMultiAccountSetup`:
  `docs/evidence/domain1-governance-config-org-rule-management-multiaccount-slr-precreate-20260625.err`;
- the missing service-linked role was created in the management account:
  `docs/evidence/domain1-governance-config-org-rule-management-multiaccount-slr-create-20260625.json`
  and
  `docs/evidence/domain1-governance-config-org-rule-management-multiaccount-slr-postcreate-20260625.json`;
- the sandbox-excluded rule definition was retried:
  `docs/evidence/domain1-governance-config-org-rule-postretry-20260625.json`;
- final organization-rule status is `UPDATE_SUCCESSFUL`:
  `docs/evidence/domain1-governance-config-org-rule-statuses-postretry-20260625.json`;
- final detailed status shows `UPDATE_SUCCESSFUL` in all three intended
  in-scope accounts:
  `docs/evidence/domain1-governance-config-org-rule-detailed-status-postretry-20260625.json`;
- the security account's local organization-managed rule remains `COMPLIANT`:
  `docs/evidence/domain1-governance-config-security-rule-compliance-postretry-20260625.json`;
- the management account's local organization-managed rule is now present and
  `COMPLIANT`:
  `docs/evidence/domain1-governance-config-management-rules-finalcheck-20260625.json`
  and
  `docs/evidence/domain1-governance-config-management-rule-compliance-finalcheck-20260625.json`.

Observed validation after sandbox re-inclusion:

- the pre-change rule definition still excluded sandbox account `974893866311`:
  `docs/evidence/domain1-governance-config-org-rule-pre-sandbox-include-20260625.json`;
- the same organization rule was updated with no excluded accounts:
  `docs/evidence/domain1-governance-config-org-rule-post-sandbox-include-20260625.json`;
- final organization-rule status remained `UPDATE_SUCCESSFUL` after the
  re-include change:
  `docs/evidence/domain1-governance-config-org-rule-statuses-post-sandbox-include-20260625.json`;
- final detailed status now shows `UPDATE_SUCCESSFUL` in all four intended
  accounts:
  `docs/evidence/domain1-governance-config-org-rule-detailed-status-post-sandbox-include-20260625.json`;
- the sandbox account now has a local organization-managed rule in `ACTIVE`
  state:
  `docs/evidence/domain1-governance-config-sandbox-rules-post-org-rule-include-20260625.json`;
- the sandbox account's local organization-managed rule currently evaluates
  `COMPLIANT`:
  `docs/evidence/domain1-governance-config-sandbox-rule-compliance-post-org-rule-include-20260625.json`.

## Next Bounded Step After This One

The next AWS Config follow-up should stay narrow:

1. keep this first organization rule stable and use the now-complete
   four-account recorder baseline as the default control surface;
2. choose the next starter rule family such as CloudTrail CloudWatch Logs,
   CloudTrail encryption, S3 public-exposure prevention, or required tags.
