# 09 - Security, Identity, Governance, and Compliance

**Last revised:** 2026-07-28

SAP-C02 security questions test layered controls: identity, resource policy, encryption, network boundary, detective controls, and organization-level guardrails.

## Organization and account governance

| Requirement | Service/control |
|---|---|
| Multi-account structure | AWS Organizations |
| Prevent actions across accounts/OUs | Service Control Policies (SCPs) |
| Account vending and baseline | Control Tower |
| Central identity federation | IAM Identity Center |
| Central audit/security accounts | Organizations + delegated administrators |
| Centralized logs | CloudTrail organization trail, CloudWatch/S3 log archive |

## SCPs

### What SCPs do

SCPs define the maximum available permissions for accounts in an organization or organizational unit.

### What SCPs do not do

- They do not grant permissions.
- They do not apply to the management account in the same way as member accounts.
- They do not replace IAM policies.
- They do not replace resource policies.

### Common SCP patterns

- Deny leaving organization.
- Deny disabling CloudTrail/Config/GuardDuty.
- Deny creating resources outside approved regions.
- Deny public S3 bucket settings except approved exceptions.
- Deny root user actions.
- Deny deleting central log buckets.
- Restrict high-risk IAM actions.

Trap: Use explicit deny in SCP for guardrails. Still grant allowed access with IAM/resource policies.

### Region-restriction SCP nuance

`aws:RequestedRegion` evaluates the Region that received the API call. Many global services use a single endpoint hosted in one Region—commonly `us-east-1`—and are not automatically ignored by the condition key. If that endpoint Region is not allowed, a deny-outside-approved-Regions SCP must explicitly exempt the required global-service actions with `NotAction`, commonly including `route53:*`, `iam:*`, and `cloudfront:*` where those services are approved. Allowing the endpoint Region also avoids the deny, but can permit unrelated Regional services there.

`NotAction` only removes those actions from this deny statement; it grants nothing. Keep the exception list limited to global services the organization actually uses.

Trap: Route 53 needs an explicit exception in the common Region-deny pattern, but not because IAM or CloudFront are “exempt by default”—they normally need explicit exceptions too.

## IAM

### Role vs policy basics

| Concept | Meaning |
|---|---|
| IAM user | Long-term identity; avoid for humans when federation is available |
| IAM role | Assumable identity with temporary credentials |
| Trust policy | Who can assume the role |
| Permissions policy | What the role can do |
| Resource policy | Who can access a resource |
| Permission boundary | Maximum permissions for a principal |
| Session policy | Further restricts a role session |

### PassRole vs AssumeRole

| Action | Meaning |
|---|---|
| `iam:PassRole` | Allows a principal to pass a role to an AWS service |
| `sts:AssumeRole` | Allows a principal to assume a role and receive credentials |

Example:

```text
Developer creates Lambda function
  -> needs iam:PassRole for Lambda execution role
Lambda runtime calls S3
  -> Lambda service assumes execution role
```

Trap: PassRole does not mean the developer personally gets the role credentials.

## KMS

### Key policy matters

KMS authorization often requires both:

- key policy permits the principal or enables IAM policies
- IAM policy allows KMS action
- service/resource policy alignment where relevant

### Common KMS exam points

- Use customer managed keys when key policy/rotation/audit/control requirements exist.
- Use grants for AWS service integrations where appropriate.
- Cross-account encrypted resource access requires key policy and resource/IAM access.
- Envelope encryption is common for scalable data encryption.
- Deleting keys has a waiting period and is dangerous.

Trap: Giving S3 access to an encrypted object is insufficient if KMS key access is missing.

## Secrets Manager vs SSM Parameter Store

| Requirement | Better fit |
|---|---|
| Managed secret rotation | Secrets Manager |
| Config parameters, simple secure strings | SSM Parameter Store |
| RDS credential rotation integration | Secrets Manager |
| Lower-cost parameter storage for app config | Parameter Store |

Trap: Do not hardcode secrets in Lambda environment variables or ECS task definitions without secure reference/encryption patterns.

## Logging and detective controls

| Service | Use |
|---|---|
| CloudTrail | API activity audit |
| CloudWatch Logs/Metrics/Alarms | Operational telemetry |
| AWS Config | Resource configuration history/compliance |
| GuardDuty | Threat detection |
| Security Hub | Aggregated security findings/posture |
| Detective | Security investigation |
| Macie | Sensitive data discovery in S3 |
| IAM Access Analyzer | External access and policy analysis |
| Inspector | Vulnerability management for workloads/images |
| Audit Manager | Audit evidence collection |

## Network/application protection

| Requirement | Service |
|---|---|
| Web Layer 7 rules | AWS WAF |
| DDoS standard protection | Shield Standard |
| Advanced DDoS protection/support/cost protections | Shield Advanced |
| Network firewalling | AWS Network Firewall |
| Appliance insertion | Gateway Load Balancer |
| Edge protection for public HTTP app | CloudFront + WAF + Shield |
| TLS certificates | ACM |

## S3 security

High-yield controls:

- Block Public Access.
- Bucket policies.
- IAM identity policies.
- Access Points.
- Object ownership.
- KMS encryption.
- CloudTrail data events for object-level audit.
- Macie for sensitive data discovery.
- Object Lock for WORM requirements.
- VPC endpoint and bucket policy conditions for private access.

Trap: A public bucket policy can still expose data if Block Public Access is disabled. Use layered controls.

## Cross-account access pattern

```text
Account A principal
  -> assumes role in Account B
  -> role permissions allow action
  -> target resource policy allows role if needed
  -> KMS key policy allows role if encrypted
```

## Exam traps

| Trap | Correction |
|---|---|
| “SCP grants admin access” | SCP only limits. IAM grants. |
| “IAM policy alone controls KMS” | KMS key policy is central. |
| “S3 encrypted object access only needs S3 permission” | KMS permission may also be required. |
| “GuardDuty blocks attacks” | It detects; response automation is separate. |
| “Security Hub scans resources directly” | It aggregates findings and checks posture. |
| “WAF protects all TCP traffic” | WAF is Layer 7 HTTP(S). |
| “Shield replaces WAF” | Shield is DDoS; WAF is web request filtering. |
