# Domain 1 Governance Review - 2026-07-13

## Status and Boundary

This is the Governance review scheduled for the week of 2026-08-10 and started
early under direct user instruction. It consolidates the fresh 2026-07-13
Organizations/SCP, logging, lifecycle, IAM Identity Center, Config, and
GuardDuty evidence. It does not authorize or perform an additional AWS change.

## Review Outcome

The current Energy Data Lakehouse governance baseline is **operationally
sufficient for the active low-volume platform**. The Domain 1 programme remains
**Partial**, not because of an unaddressed production blocker, but because the
remaining work is deliberately evidence-gated or requires an explicit adoption
decision.

| Area | Review conclusion | Immediate action |
|---|---|---|
| Organization and SCP boundary | Lakehouse Workloads OU has the two intended narrow guardrails; broader SCP rollout needs separate testing and rollback planning. | No attachment now. |
| Identity Center | Routine Security Tooling, emergency, and archive-lifecycle paths are bounded; broader workload, billing, and organization permission sets remain future design choices. | No expansion now. |
| Central logging and archive storage | Organization trail, Config delivery, archive protection, and 30/90-day large-object lifecycle transitions are verified. | Maintain read-only continuity checks. |
| Config and GuardDuty | Config aggregation/recording and GuardDuty foundations are active in `eu-west-2`. | No optional data-source or rule expansion now. |
| Security Hub and OAM | Both remain intentional deferred adoption decisions. | Do not enable merely to improve a checklist status. |
| Budget thresholds | Account attribution is materially improved, but only one meaningful finalized month exists. | Perform the scheduled evidence review; do not invent a numeric threshold. |

## Priority Decisions

1. **Do not create a new live governance change just to be ahead of schedule.**
   The current controls already protect the platform's active risk surface.
2. Keep the documented `config:DescribeConfigRules` addition as the only
   low-blast-radius future IAM hardening candidate. It needs its own approval,
   precheck, rollout, and rollback; it is not needed for normal platform
   operation.
3. Keep Security Hub, OAM, optional GuardDuty protection plans, and broader
   SCPs as intentional future decisions. Each carries a distinct cost, coverage,
   access, or service-exception model.
4. Treat the cost-threshold evidence review as a time-based observation gate,
   not as a block on the platform or on the Domain 1 study plan.

## Revisit Triggers

Reopen a live governance change only if one of the following occurs:

- a trail, digest, Config recorder, delivery channel, or GuardDuty detector
  becomes unhealthy;
- a new account, Region, workload class, regulated data requirement, or
  persistent cost anomaly changes the risk profile;
- three meaningful finalized cost months make account-level threshold design
  defensible; or
- Security Hub, OAM, an optional GuardDuty source, an additional Config rule,
  or a specific SCP has a named operational requirement and separate approval.

## SAP-C02 Relevance

This review supports Domain 1 by showing that a mature architecture decision is
sometimes to retain a validated, least-privilege baseline and defer additional
managed services until their trade-offs are justified.
