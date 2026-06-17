# Cost Allocation Tag Activation - 2026-06-17

## Scope

This evidence records the completed AWS Billing Cost Allocation Tag activation
for the selected Energy Data Lakehouse governance tags.

The activation was run from the AWS Organizations management account after the
previous workload-account attempt was blocked. Codex did not run these live AWS
commands; the command output was supplied by the repository owner.

## Organization Context

The AWS CLI profile `org-admin` authenticated to the Organizations management
account:

```json
{
  "Account": "349687196588",
  "Arn": "arn:aws:sts::349687196588:assumed-role/AWSReservedSSO_AdministratorAccess_be84055d6f72e1b2/shola-cloud-lab-admin"
}
```

The organization contained these active accounts:

| Account ID | Name | Role in current governance model |
|---|---|---|
| `349687196588` | `shola-cloud-lab` | AWS Organizations management account |
| `464975959576` | `Olusola_AWS` | Energy Data Lakehouse workload/member account |
| `974893866311` | `containers-lab.com` | Container labs member account |

This confirms the Energy Data Lakehouse account is a member account and that
Billing Cost Allocation Tag activation must be administered from the management
account or another authorized Billing administrator identity.

## Selected Tags Activated

Only the previously approved low-cardinality governance and cost-reporting tags
were activated:

| Tag key | Reason |
|---|---|
| `Project` | Portfolio/project cost grouping |
| `Workload` | Workload-level cost grouping |
| `Environment` | Environment cost grouping |
| `Purpose` | Resource-purpose reporting |
| `ManagedBy` | Ownership and management model reporting |
| `DataClassification` | Governance and classification reporting |
| `CostCenter` | Cost attribution |

`Owner`, `Name`, `Phase`, `Lesson`, `ProjectS3Path`, `iamPrincipal/*`,
`AmazonDataZone*`, and container/lab-specific tags were not activated as part
of this decision.

## Activation Command

```bash
aws ce update-cost-allocation-tags-status \
  --profile org-admin \
  --no-cli-pager \
  --cost-allocation-tags-status '[
    {"TagKey":"Project","Status":"Active"},
    {"TagKey":"Workload","Status":"Active"},
    {"TagKey":"Environment","Status":"Active"},
    {"TagKey":"Purpose","Status":"Active"},
    {"TagKey":"ManagedBy","Status":"Active"},
    {"TagKey":"DataClassification","Status":"Active"},
    {"TagKey":"CostCenter","Status":"Active"}
  ]'
```

Result:

```json
{
  "Errors": []
}
```

## Verification Command

```bash
aws ce list-cost-allocation-tags \
  --profile org-admin \
  --no-cli-pager \
  --status Active \
  --type UserDefined
```

Verified active tag keys:

| Tag key | Status | Last updated |
|---|---|---|
| `Workload` | Active | `2026-06-17T11:49:22Z` |
| `DataClassification` | Active | `2026-06-17T11:49:22Z` |
| `Purpose` | Active | `2026-06-17T11:49:22Z` |
| `ManagedBy` | Active | `2026-06-17T11:49:22Z` |
| `CostCenter` | Active | `2026-06-17T11:49:22Z` |
| `Project` | Active | `2026-06-17T11:49:22Z` |
| `Environment` | Active | `2026-06-17T11:49:22Z` |

## Outcome

The selected AWS Billing Cost Allocation Tags are active from the Organizations
management account. This closes the Lakehouse cost-allocation activation gap
that was previously blocked from the workload account.

Cost Explorer and billing reports may still have normal reporting latency after
activation, so later cost analysis should verify when the activated dimensions
appear in reports.
