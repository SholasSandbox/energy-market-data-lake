# Domain 1 GuardDuty Delegated Admin Planning - 2026-07-06

<!-- markdownlint-disable MD013 -->

## Status

Planning decision recorded. No live AWS change is authorized or performed by this
note.

## Decision

Use `Security Tooling` (`668848431187`) as the GuardDuty delegated administrator
account if GuardDuty is adopted.

Do not create another account for GuardDuty. Do not use `Security Log Archive`,
`so-aws-admin`, the Organizations management account, or any workload account as
the durable GuardDuty delegated administrator.

## Rationale

This follows the accepted security-account split:

- `Security Log Archive` (`955659429518`) remains storage-only for CloudTrail and
  AWS Config archive buckets, KMS keys, retention controls, and log-storage
  evidence.
- `Security Tooling` (`668848431187`) owns active delegated security tooling,
  including AWS Config aggregation today and GuardDuty delegated administration
  if adopted next.
- `so-aws-admin` (`054394900225`) is on the decommission path and must not become
  a new security-service home.

Using the same `Security Tooling` account for AWS Config, GuardDuty, and possible
future Security Hub keeps delegated security administration in one clear place
without broadening the archive account or multiplying governance accounts.

## Planning Scope

The GuardDuty planning target is:

- delegated administrator: `Security Tooling` (`668848431187`);
- Region posture: start with the active governance Region, `eu-west-2`;
- future Region rule: if GuardDuty is later enabled in more Regions, use the same
  delegated administrator account consistently in each enabled Region;
- initial service posture: foundational GuardDuty only;
- optional protection plans: off by default until a workload-specific value and
  cost decision is recorded;
- account coverage target: active organization accounts, excluding
  `so-aws-admin` while it remains on the decommission path.

Initial active-account coverage should be evaluated for:

| Account | Planned GuardDuty posture |
|---|---|
| Management / `349687196588` | Foundational coverage target; management remains control plane, not daily security operations. |
| Security Log Archive / `955659429518` | Foundational coverage target; account remains storage-only. |
| Security Tooling / `668848431187` | Delegated administrator and foundational coverage target. |
| Lakehouse workload / `464975959576` | Foundational coverage target. |
| Container Sandbox / `974893866311` | Foundational coverage target, with cost observation after enablement. |
| so-aws-admin / `054394900225` | Excluded on the decommission path unless dependency checks reveal a temporary reason to keep it active. |

## Required Read-Only Prechange Evidence

Before any live GuardDuty change is approved, collect read-only evidence for:

1. Management-account caller identity and Organizations account inventory.
2. GuardDuty organization administrator state in `eu-west-2`.
3. GuardDuty detector state in the management account, `Security Tooling`,
   `Security Log Archive`, lakehouse workload, sandbox, and `so-aws-admin`.
4. Current enabled AWS service access and delegated administrators.
5. Current GuardDuty organization configuration, member list, and auto-enable
   posture if any delegated administrator already exists.
6. Budget/cost expectation for foundational GuardDuty and any optional protection
   plans under consideration.

## Next Bounded Live Change If Later Approved

The first GuardDuty live implementation should be its own change boundary:

1. Confirm fresh prechange evidence and the acting account.
2. Enable or verify GuardDuty in `Security Tooling` for `eu-west-2` if required.
3. Designate `Security Tooling` as the GuardDuty delegated administrator in
   `eu-west-2`.
4. Configure foundational organization coverage for the approved active accounts.
5. Leave optional protection plans disabled unless separately approved.
6. Verify delegated administrator, detector, member, and organization
   configuration state.
7. Record postchange evidence and cost-observation follow-up.

## Explicitly Out Of Scope

- creating a new GuardDuty or security tooling account;
- using `so-aws-admin` for GuardDuty;
- enabling Security Hub;
- enabling OAM;
- changing AWS Config recorder scope;
- changing SCPs;
- moving or closing accounts;
- enabling optional GuardDuty protection plans without a separate decision.

## SAP-C02 And AWS Well-Architected Framework Relevance

This supports SAP-C02 Domain 1 by keeping delegated security administration out
of the management account, away from storage-only audit accounts, and anchored in
the intended security tooling boundary. It also supports AWS Well-Architected
security, operational excellence, and cost optimization by using clear ownership,
one delegated-admin account, deliberate Region scope, and staged optional-plan
adoption.
