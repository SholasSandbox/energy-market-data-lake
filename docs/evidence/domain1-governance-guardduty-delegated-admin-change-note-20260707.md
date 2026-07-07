# Domain 1 GuardDuty Delegated Administration Change Note - 2026-07-07

<!-- markdownlint-disable MD013 -->

## Status

Completed.

On 2026-07-07, GuardDuty delegated administration was configured in
`eu-west-2` with `Security Tooling` (`668848431187`) as the delegated
administrator.

Final state:

- `guardduty.amazonaws.com` trusted service access is enabled in AWS
  Organizations;
- `Security Tooling` is the GuardDuty delegated administrator in `eu-west-2`;
- GuardDuty detector `6ccf9e93cbfefca63bcb3c31593649c3` is enabled in
  `Security Tooling`;
- GuardDuty detector `d2cf9e93cf11b0e1a118ec7c756b7008` is enabled in the
  management account for foundational coverage;
- foundational GuardDuty member coverage is enabled for lakehouse workload
  account `464975959576`, `Security Log Archive` account `955659429518`, and
  container sandbox account `974893866311`;
- `so-aws-admin` (`054394900225`) is excluded and remains on the decommission
  path;
- GuardDuty organization auto-enable is `NONE`, so new or excluded accounts are
  not silently enrolled;
- optional GuardDuty protection plans are disabled, including S3 data events,
  EKS audit logs, EKS runtime monitoring, EBS malware protection, RDS login
  events, Lambda network logs, Runtime Monitoring, and the currently reported
  `AI_ANALYST` feature.

## Approval Boundary

User approval for this bounded live change:

> I explicitly approve configuring GuardDuty delegated administration in
> eu-west-2 with Security Tooling 668848431187 as delegated administrator,
> foundational GuardDuty coverage only, and so-aws-admin excluded. Do not enable
> Security Hub, OAM, optional GuardDuty protection plans, change SCPs, change
> AWS Config, move accounts, or modify workload resources.

Explicitly out of scope and not performed:

- Security Hub enablement or delegated-administrator setup;
- OAM setup;
- optional GuardDuty protection-plan enablement;
- SCP changes;
- AWS Config changes;
- account moves, closure, or retirement actions;
- workload resource changes;
- GuardDuty enablement outside `eu-west-2`.

## Prechange Evidence

Fresh prechange evidence showed the expected state immediately before the live
change:

| Evidence | File |
|---|---|
| Management caller identity | `docs/evidence/domain1-governance-guardduty-delegated-admin-management-sts-prechange-20260707.json` |
| Security Tooling caller identity | `docs/evidence/domain1-governance-guardduty-delegated-admin-security-tooling-sts-prechange-20260707.json` |
| Organizations service access | `docs/evidence/domain1-governance-guardduty-delegated-admin-org-service-access-prechange-20260707.json` |
| Organizations delegated administrators for GuardDuty | `docs/evidence/domain1-governance-guardduty-delegated-admin-guardduty-delegated-admins-prechange-20260707.json` |
| GuardDuty organization admin state | `docs/evidence/domain1-governance-guardduty-delegated-admin-management-org-admin-accounts-prechange-20260707.json` |
| Management detector state | `docs/evidence/domain1-governance-guardduty-delegated-admin-management-detectors-prechange-20260707.json` |
| Security Tooling detector state | `docs/evidence/domain1-governance-guardduty-delegated-admin-security-tooling-detectors-prechange-20260707.json` |

Prechange summary:

- GuardDuty trusted service access was not enabled in AWS Organizations.
- No GuardDuty delegated administrator existed.
- `aws guardduty list-organization-admin-accounts` returned no admin accounts
  in `eu-west-2`.
- No GuardDuty detector existed in the management or Security Tooling account.

## Change Executed

The live change was executed in this order.

1. Assumed `OrganizationAccountAccessRole` into `Security Tooling`.
2. Created the GuardDuty detector in `Security Tooling` with foundational
   GuardDuty enabled and optional features explicitly disabled.
3. Created the GuardDuty detector in the management account with foundational
   GuardDuty enabled and optional features explicitly disabled.
4. Enabled AWS Organizations service access for `guardduty.amazonaws.com`.
5. Designated `Security Tooling` as the GuardDuty delegated administrator.
6. Set GuardDuty organization auto-enable to `NONE` and optional feature
   auto-enable to `NONE`.
7. Created GuardDuty members only for the approved active member accounts:
   `464975959576`, `955659429518`, and `974893866311`.
8. Re-applied member detector optional-feature settings as disabled.

Key execution evidence:

- `docs/evidence/domain1-governance-guardduty-delegated-admin-security-tooling-create-detector-20260707.json`
- `docs/evidence/domain1-governance-guardduty-delegated-admin-management-create-detector-20260707.json`
- `docs/evidence/domain1-governance-guardduty-delegated-admin-enable-guardduty-service-access-20260707.json`
- `docs/evidence/domain1-governance-guardduty-delegated-admin-enable-organization-admin-account-20260707.json`
- `docs/evidence/domain1-governance-guardduty-delegated-admin-update-organization-configuration-none-20260707.json`
- `docs/evidence/domain1-governance-guardduty-delegated-admin-create-members-selected-active-accounts-20260707.json`
- `docs/evidence/domain1-governance-guardduty-delegated-admin-update-member-detectors-optional-disabled-20260707.json`

## Postchange Evidence

| Evidence | File |
|---|---|
| GuardDuty service access | `docs/evidence/domain1-governance-guardduty-delegated-admin-org-service-access-postchange-20260707.json` |
| GuardDuty delegated administrator | `docs/evidence/domain1-governance-guardduty-delegated-admin-guardduty-delegated-admins-postchange-20260707.json`; `docs/evidence/domain1-governance-guardduty-delegated-admin-management-org-admin-accounts-postchange-20260707.json` |
| Security Tooling detector | `docs/evidence/domain1-governance-guardduty-delegated-admin-security-tooling-get-detector-final-20260707.json`; `docs/evidence/domain1-governance-guardduty-delegated-admin-security-tooling-list-detectors-postchange-20260707.json` |
| Management detector | `docs/evidence/domain1-governance-guardduty-delegated-admin-management-get-detector-final-20260707.json`; `docs/evidence/domain1-governance-guardduty-delegated-admin-management-list-detectors-postchange-20260707.json` |
| Organization configuration | `docs/evidence/domain1-governance-guardduty-delegated-admin-security-tooling-describe-organization-configuration-postchange-20260707.json` |
| Member list | `docs/evidence/domain1-governance-guardduty-delegated-admin-security-tooling-list-members-postchange-20260707.json` |
| Member detector feature posture | `docs/evidence/domain1-governance-guardduty-delegated-admin-security-tooling-get-member-detectors-postchange-20260707.json` |
| Validation summary | `docs/evidence/domain1-governance-guardduty-delegated-admin-validation-summary-20260707.json` |

Postchange validation:

- GuardDuty delegated administrator is `668848431187`.
- GuardDuty organization admin status is `ENABLED`.
- GuardDuty members are exactly `464975959576`, `955659429518`, and
  `974893866311`.
- `054394900225` is not a GuardDuty member.
- organization auto-enable is `NONE`;
- foundational GuardDuty sources are enabled: CloudTrail, DNS logs, and VPC flow
  logs;
- optional protection plans are disabled in the management detector, Security
  Tooling detector, and member detectors;
- public evidence redaction passed with
  `scripts/check_public_evidence_redaction.sh`.

## Rollback

Rollback would disable the GuardDuty organization relationship and detectors and
must be separately approved.

Recommended rollback order:

1. Stop monitoring and delete only the approved member associations.
2. Disable `Security Tooling` as GuardDuty delegated administrator.
3. Disable or delete the Security Tooling detector.
4. Disable or delete the management-account detector.
5. Disable `guardduty.amazonaws.com` trusted service access only if no other
   GuardDuty organization dependency exists.

Do not perform rollback as part of Security Hub, OAM, AWS Config, SCP, account
move, or workload-resource work without a separate boundary review.

## Follow-Up Required

- Observe GuardDuty usage/cost before adding Regions or optional protection
  plans.
- Keep `so-aws-admin` excluded while read-only dependency checks and dependency
  resolution prepare it for retirement.
- Revisit Security Hub only after the GuardDuty baseline has settled and the
  cost/standards value is clear.
- Revisit OAM separately if cross-account CloudWatch telemetry becomes useful.

## SAP-C02 Relevance

This supports SAP-C02 Domain 1 by demonstrating a bounded security-service
delegation rollout:

- keep the management account as the control plane, not the daily security
  operations account;
- use a dedicated security tooling account for delegated administration;
- separate foundational threat detection from optional protection plans;
- keep decommission-path accounts excluded instead of creating technical debt;
- capture prechange, mutation, postchange, validation, rollback, and cost
  follow-up evidence for a live multi-account governance change.
