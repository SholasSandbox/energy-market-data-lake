# Domain 1 Governance Hardening Review - 2026-07-13

## Status and Boundary

This is the documentation-only Governance hardening slice scheduled for the
week of 2026-08-03 and started early under direct user instruction. It uses
fresh read-only Security Tooling evidence. It does not enable Security Hub or
OAM, change AWS Config or GuardDuty, alter an SCP, or modify any Identity
Center permission set or assignment.

## Fresh Security Tooling Posture

The routine `SecurityToolingAdmin` role successfully reads the live
organization Config aggregator and GuardDuty detector inventory in `eu-west-2`.
The aggregator remains organization-scoped for that Region.

Three outcomes matter for hardening:

| Capability | Observed result | Interpretation |
|---|---|---|
| `config:DescribeConfigRules` | Denied because it is not granted | A small read-only operational visibility gap. |
| Security Hub `DescribeHub` | Explicitly denied | Intentional: Security Hub remains unadopted, so routine access must not imply adoption. |
| OAM `ListSinks` and `ListLinks` | Explicitly denied | Intentional: OAM remains unadopted, so routine access must not imply cross-account observability rollout. |

The live permission-set policy matches the repository policy. The Security Hub
and OAM denies are explicit, while Config-rule listing is simply absent. This
is not a configuration drift finding.

## Decision

Keep Security Hub and OAM **deferred**. Their costs, account-link or
standards/finding scope, operational ownership, and response model have not
been approved. Adding read access now would blur that boundary and create an
expectation of service operation without an adopted service.

Prepare one future, independently approval-bound hardening unit:

```json
{
  "Effect": "Allow",
  "Action": "config:DescribeConfigRules",
  "Resource": "*",
  "Condition": {
    "StringEquals": {
      "aws:RequestedRegion": "eu-west-2"
    }
  }
}
```

This would let the existing routine Security Tooling administrator enumerate
local Config rules for diagnosis. It would not add Config write actions, change
any rule, enable Security Hub/OAM, or weaken the explicit teardown denies.

## Required Gate for the Proposed Config-Read Change

Before any implementation, create a separate change note with a fresh
Identity Center policy/assignment inventory, IAM Access Analyzer validation,
the exact updated inline policy, a provisioning-status check, a successful
postchange `DescribeConfigRules` check, and a rollback that restores the
current policy then reprovisions the permission set. This review is not that
approval.

## SAP-C02 Relevance

This supports Domain 1 by distinguishing security-service adoption from access
to an existing control plane, keeping delegated administration least-privilege,
and using explicit defer decisions instead of enabling every available service.
