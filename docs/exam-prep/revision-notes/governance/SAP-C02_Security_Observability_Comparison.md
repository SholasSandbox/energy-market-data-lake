# SAP-C02 Study Note: OAM vs CloudTrail Log Archive vs AWS Config Aggregator

## Purpose

This note captures a compact SAP-C02 distinction between three services and
patterns that often appear together in multi-account security and observability
questions:

- Amazon CloudWatch Observability Access Manager (OAM)
- AWS CloudTrail organization log archive
- AWS Config aggregator

The key exam habit is to separate **operational visibility**, **audit evidence**,
and **configuration compliance**. They are related, but they answer different
questions and usually belong in different account boundaries.

## Quick Comparison

| Pattern | Primary purpose | Best account home | Exam question it answers |
| --- | --- | --- | --- |
| CloudWatch OAM / cross-account observability | Central live observability across accounts: metrics, logs, traces, and application telemetry views | `Security Tooling` or central monitoring account | "How do I monitor and troubleshoot workloads across multiple accounts without switching accounts?" |
| CloudTrail log archive | Durable audit evidence of API activity, delivered to S3 and protected with bucket, KMS, retention, and delete controls | `Log Archive` account | "How do I centrally retain tamper-resistant audit logs for all accounts?" |
| AWS Config aggregator | Central inventory and compliance view of resource configuration and Config rule results across accounts and Regions | `Security Tooling` account long term | "How do I see resource configuration, drift, and compliance posture across the organization?" |

## Mental Model

| Question | Think |
| --- | --- |
| "What is happening right now in my workloads?" | OAM / CloudWatch cross-account observability |
| "Who called which AWS API, from where, and when?" | CloudTrail organization trail and log archive |
| "What resources exist, how are they configured, and are they compliant?" | AWS Config recorder, rules, and aggregator |

## OAM: Operational Visibility

OAM supports CloudWatch cross-account observability. A monitoring account can
view observability data shared from source accounts, including CloudWatch
metrics, logs, traces, and related telemetry.

Use OAM when the question is about:

- centralized operational dashboards;
- cross-account troubleshooting;
- viewing CloudWatch metrics or logs from multiple accounts;
- tracing application behavior across account boundaries;
- reducing account switching during operations.

Do not choose OAM when the main requirement is:

- immutable audit retention;
- API-call forensics;
- compliance rule evaluation;
- detecting resource drift.

Recommended account placement:

- place OAM in a `Security Tooling` or central monitoring account;
- keep it out of the write-mostly `Log Archive` account;
- treat it as an operational visibility plane, not the audit evidence store.

## CloudTrail Log Archive: Audit Evidence

CloudTrail records AWS API activity. In a multi-account organization, the
common SAP-C02 pattern is one organization trail with logs delivered into a
central S3 bucket owned by a log archive account.

Use CloudTrail log archive when the question is about:

- who performed an action;
- API audit history;
- forensic investigation;
- central retention of audit logs;
- protecting logs from tampering or deletion;
- organization-wide management-event logging.

Do not choose CloudTrail log archive when the main requirement is:

- live metrics dashboards;
- application trace exploration;
- current resource compliance;
- configuration drift detection by itself.

Recommended account placement:

- store organization CloudTrail logs in the `Log Archive` account;
- protect the archive with S3 Block Public Access, versioning, tight bucket
  policy, KMS key policy, retention controls, and later delete-protection
  guardrails;
- keep the account storage-first and write-mostly.

## AWS Config Aggregator: Configuration And Compliance Posture

AWS Config records resource configuration history and evaluates Config rules.
An aggregator gives a central view across accounts and Regions.

Use AWS Config aggregator when the question is about:

- what resources exist;
- whether resources match required configuration;
- compliance posture across accounts;
- configuration drift;
- seeing Config rule results centrally;
- querying historical configuration state.

Do not choose AWS Config aggregator when the main requirement is:

- live application troubleshooting;
- CloudWatch metrics or traces;
- immutable API audit log retention;
- replacing CloudTrail as the source of "who did what."

Recommended account placement:

- keep recorders in each in-scope account and Region;
- place the long-term aggregator in the `Security Tooling` account;
- use the `Log Archive` account only for storage of delivered Config snapshots
  and history, not as the permanent delegated operations home.

## SAP-C02 Answer Rules

| If the scenario says... | Prefer... | Why |
| --- | --- | --- |
| "Centralized monitoring across multiple accounts" | OAM / CloudWatch cross-account observability | It is the service built for cross-account operational visibility. |
| "Troubleshoot application behavior using metrics, logs, and traces" | OAM / CloudWatch | The problem is live observability, not governance evidence. |
| "Retain audit logs for all accounts" | CloudTrail organization trail plus log archive bucket | CloudTrail records API activity; S3/KMS/retention protect the evidence. |
| "Investigate who deleted or changed a resource" | CloudTrail | The question is about API caller, time, source, and action. |
| "Detect non-compliant S3 buckets or missing encryption" | AWS Config rule plus aggregator | The question is about resource configuration compliance. |
| "Central view of compliance across accounts and Regions" | AWS Config aggregator | Aggregators centralize Config data and rule results. |
| "Security findings and standards across accounts" | Security Hub, usually after Config and GuardDuty are settled | Security Hub aggregates findings and standards, but depends on other security-service choices. |

## Lakehouse Governance Mapping

For the Energy Data Lakehouse governance model:

| Account boundary | Preferred responsibility |
| --- | --- |
| Management account | Organizations, IAM Identity Center, SCP administration, and explicit enablement decisions |
| `Security Log Archive` / `Log Archive` | CloudTrail and Config archive buckets, KMS keys, retention controls, and storage-only audit evidence |
| Future `Security Tooling` | OAM, AWS Config aggregator, GuardDuty delegated administration, possible later Security Hub |
| Workload accounts | Produce telemetry, audit events, configuration items, and findings |

The clean long-term design is therefore:

1. Keep `Security Log Archive` storage-only.
2. Create a separate `Security Tooling` account after the current break-glass
   and root-user emergency-only SCP blocker closes.
3. Migrate delegated-admin functions in order: AWS Config first, GuardDuty next,
   Security Hub only if intentionally adopted.
4. Treat OAM as a `Security Tooling` or central monitoring capability, not a log
   archive storage feature.

## References

- CloudWatch cross-account observability:
  `https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Unified-Cross-Account.html`
- CloudWatch Observability Access Manager API:
  `https://docs.aws.amazon.com/OAM/latest/APIReference/Welcome.html`
- CloudTrail organization trails:
  `https://docs.aws.amazon.com/awscloudtrail/latest/userguide/creating-trail-organization.html`
- AWS Config multi-account and multi-Region aggregation:
  `https://docs.aws.amazon.com/config/latest/developerguide/aggregate-data.html`
