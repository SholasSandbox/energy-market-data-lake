# Domain 1 Evidence - Workload Tag Coverage Precheck - 2026-07-12

## Boundary

This is a read-only, public-safe precheck followed by a local Terraform
configuration hardening change. No AWS resource tag, tag policy, budget, IAM
configuration, or other AWS resource was changed.

## Lakehouse Account Inventory

The lakehouse workload-account profile returned 14 resources carrying
`Workload=energy-market-data-lake` in `eu-west-2` through the Resource Groups
Tagging API. The tagged set includes the Athena workgroup, Glue crawlers and
job, data and dashboard S3 buckets, Lambda functions and log groups, EventBridge
rules, Step Functions state machine, and SNS topic.

Focused inventory found no NAT gateways, Elastic IPs, or VPC endpoints in the
lakehouse account. Those common `EC2 - Other` cost sources therefore do not
provide a safe tagging target in this account.

## Interpretation

The Cost Explorer `Workload` gap is organization-wide and cannot safely be
attributed to the lakehouse account from separate account- and service-grouped
queries. Reapplying the tag to the existing lakehouse resources would not
improve that attribution evidence.

The next live tag action, if any, must identify a specific resource in the
account that incurs the untagged cost, confirm that the service supports the
tag, and use a separately approved change note with rollback and validation.
Do not apply a broad Organizations tag policy merely to address this result.

## Local Hardening

`infra/terraform/lakehouse` now defaults the supported-resource tag map to
`Workload=energy-market-data-lake` and rejects a conflicting supplied value.
This prevents future Terraform-managed lakehouse resources from silently
omitting or changing the workload attribution tag. It does not change existing
AWS resources until a separately reviewed Terraform plan and apply are
explicitly approved.

## SAP-C02 Relevance

This supports Domain 1 cost governance by separating verified resource-tag
coverage from unproven cost attribution, enforcing a stable low-cardinality
tag for future workload resources, and keeping any cross-account remediation
bounded and evidence-led.
