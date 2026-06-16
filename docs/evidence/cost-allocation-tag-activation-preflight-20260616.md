# Cost Allocation Tag Activation Preflight - 2026-06-16

## Scope

This evidence records the separately approved account-governance action to
activate selected user-defined AWS Billing cost-allocation tags for the Energy
Data Lakehouse.

No tag activation was completed because the current AWS account is a linked
account and does not have access to Cost Allocation Tag administration.

## Approval

Explicit approval was granted on 2026-06-16 for the separate
account-governance decision to activate selected AWS Billing cost-allocation
tags.

## AWS Identity

```json
{
    "UserId": "AIDAWYQV2CYMFKHW4IY24",
    "Account": "464975959576",
    "Arn": "arn:aws:iam::464975959576:user/IAMUser1"
}
```

## Live Bucket Tags

The live data bucket still has the eight governance tags approved in ADR 0003:

```json
{
    "TagSet": [
        {"Key": "Project", "Value": "energy-market"},
        {"Key": "Owner", "Value": "Shola"},
        {"Key": "ManagedBy", "Value": "manual"},
        {"Key": "CostCenter", "Value": "sap-c02-lab"},
        {"Key": "DataClassification", "Value": "public"},
        {"Key": "Environment", "Value": "dev"},
        {"Key": "Purpose", "Value": "lakehouse-data"},
        {"Key": "Workload", "Value": "energy-market-data-lake"}
    ]
}
```

## Selected Tags For Billing Activation

Selected low-cardinality governance and cost-reporting tags:

| Tag key | Reason |
|---|---|
| `Project` | Portfolio/project cost grouping |
| `Workload` | Lakehouse workload cost grouping |
| `Environment` | Environment cost grouping |
| `Purpose` | Resource-purpose reporting |
| `ManagedBy` | Ownership model reporting |
| `DataClassification` | Governance and classification reporting |
| `CostCenter` | Cost attribution |

`Owner` was not selected for Billing activation because it is a person-name tag
and is less useful as a cost-reporting dimension.

## Commands Attempted

Read current Cost Allocation Tag status:

```bash
aws ce list-cost-allocation-tags \
  --status Active \
  --type UserDefined \
  --max-results 100 \
  --output json

aws ce list-cost-allocation-tags \
  --status Inactive \
  --type UserDefined \
  --max-results 100 \
  --output json
```

Both read attempts failed:

```text
aws: [ERROR]: An error occurred (AccessDeniedException) when calling the ListCostAllocationTags operation: Failed to list Cost Allocation Tags: Linked account doesn't have access to cost allocation tags.
```

Attempt approved activation:

```bash
aws ce update-cost-allocation-tags-status \
  --cost-allocation-tags-status file:///tmp/cost-allocation-tags-status.json \
  --output json
```

Payload:

```json
[
  {"TagKey": "Project", "Status": "Active"},
  {"TagKey": "Workload", "Status": "Active"},
  {"TagKey": "Environment", "Status": "Active"},
  {"TagKey": "Purpose", "Status": "Active"},
  {"TagKey": "ManagedBy", "Status": "Active"},
  {"TagKey": "DataClassification", "Status": "Active"},
  {"TagKey": "CostCenter", "Status": "Active"}
]
```

Result:

```text
Exit code: 254

aws: [ERROR]: An error occurred (AccessDeniedException) when calling the UpdateCostAllocationTagsStatus operation: Failed to update Cost Allocation Tag: Linked account doesn't have access to cost allocation tags.
```

## Outcome

The cost-allocation tag activation action is approved but blocked. Completion
requires running the same activation from the payer/management account, or from
an identity with Cost Allocation Tag administration access in that account.

The Energy Data Lakehouse workload account retains the bucket tags; only the
Billing cost-allocation activation remains incomplete.
