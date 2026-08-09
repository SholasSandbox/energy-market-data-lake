# 09 - Security, Identity, Governance, and Compliance

**Last revised:** 2026-08-09

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
- They do not apply to users or roles in the management account.
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

### OU inheritance pattern

Accounts inherit SCPs along the organization root-to-OU-to-account path. When
two populations share a baseline and one needs extra restrictions, make the
more restricted population a child OU:

```text
HR OU: common HR/Recruiting SCP
  -> Recruiting child OU: additional Recruiting-only SCP
```

This avoids duplicating the common policy. AWS Organizations moves accounts,
not an existing OU as a unit, so a restructuring answer can require creating
the child OU, moving its accounts, and removing the old empty OU. Do not remove
accounts from the organization merely to change the OU hierarchy.

### Organization-wide access-loss diagnosis

If principals in several member accounts lose the same service access at the
same time immediately after central policy work, inspect a shared parent SCP
before editing separate IAM policies.

```text
simultaneous multi-account denial
  -> common root/OU/account SCP path
  -> authorized principal in management account corrects the SCP
```

An SCP also restricts the root user of an affected **member** account, so member
root cannot bypass it. SCPs do not restrict management-account principals;
that is the recovery/control-plane distinction. An IAM policy edited separately
inside each member account cannot override an SCP deny or missing SCP allow.

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

### Workforce identity versus application users

| Identity population | Default service boundary |
|---|---|
| Employees and administrators accessing multiple AWS accounts | IAM Identity Center, external IdP integration, groups, account assignments and permission sets |
| Application customers signing up/signing in and receiving application tokens | Amazon Cognito user pools |
| Application users receiving scoped temporary AWS credentials | Cognito identity pools |
| Workloads calling AWS services | IAM roles: EC2 instance profile, ECS task role, Lambda execution role, EKS workload identity pattern |

Trap: Cognito does not replace IAM Identity Center for workforce account access,
and Identity Center is not the customer sign-up directory for an application.

### Directory Service selection

| Requirement | Choice |
|---|---|
| Keep an on-premises AD as the source and proxy AWS applications to it | AD Connector |
| Standalone basic Samba-compatible directory/LDAP without MFA | Simple AD, only for an existing eligible customer |
| Retire on-premises AD; require managed Microsoft AD/LDAP and RADIUS MFA | AWS Managed Microsoft AD |
| Workforce access to AWS accounts without an LDAP/application-directory requirement | IAM Identity Center identity store |

MFA is the decisive elimination rule: Simple AD does not support MFA, while AWS
Managed Microsoft AD does. AD Connector cannot outlive the on-premises
directory it proxies. Also treat Simple AD as a legacy exam boundary: AWS
stopped opening it to new customers on 30 July 2026.

### WorkSpaces with on-premises credentials

Choose AD Connector when WorkSpaces users must authenticate against the
existing on-premises Active Directory, credentials must remain on premises,
and Direct Connect or VPN already provides directory reachability. AD
Connector is deployed as an AWS Directory Service directory in the WorkSpaces
VPC and proxies authentication requests to on-premises domain controllers; it
does not store user credentials.

```text
WorkSpaces in AWS
  -> AD Connector in the WorkSpaces VPC
  -> Direct Connect or VPN
  -> on-premises Active Directory and domain resources
```

This preserves the users' AD identity and SSO to domain-authorized files and
services. Do not place AD Connector “on premises”: it is an AWS-managed proxy
created in the VPC. Do not migrate passwords into AWS Managed Microsoft AD when
the requirement explicitly prohibits credential storage outside the company.

### SCP attachment scope follows inheritance

Attach a guardrail at the highest scope that exactly matches its population:

```text
all current/future member accounts -> organization root
one account class                  -> that OU
one exceptional account           -> that account, only if necessary
```

Therefore, a member-account root-user deny belongs at the organization root,
while a `us-west-2` Region restriction that applies only to development belongs
on the Dev OU. The Prod OU then inherits the root-user control without
inheriting the Dev-only Region restriction. SCPs do not restrict the
Organizations management account itself.

### Cognito user pools versus identity pools

```text
user pool     -> application directory, sign-up/sign-in and application tokens
identity pool -> temporary AWS credentials for authenticated or guest users
```

An identity pool can support both unauthenticated guest identities and
authenticated identities. If an existing custom IdP cannot federate through
SAML or OIDC, a developer-authenticated identity provider lets the application's
backend validate the user and exchange that proof with the identity pool.

For a free-to-premium transition, one identity pool with separate guest and
authenticated flows keeps the identity transition inside one pool. Two pools
create an avoidable identity-migration boundary. A user pool is not the answer
for an arbitrary non-SAML/non-OIDC custom provider merely because the stem uses
the word “authentication”.

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

For timely notification when `ScheduleKeyDeletion` is called, match the
CloudTrail API event with EventBridge and publish to SNS. This alerts an
administrator during the cancellable waiting period. A daily Athena query is
delayed and operationally heavier; automatic cancellation changes the requested
human-approval control and requires custom Lambda code.

Trap: Giving S3 access to an encrypted object is insufficient if KMS key access is missing.

### KMS versus CloudHSM

| Requirement | First choice |
|---|---|
| Managed keys integrated with AWS services, envelope encryption, policies and grants | KMS |
| Single-tenant hardware security modules, direct PKCS #11/JCE/CNG application interfaces, or explicit HSM ownership/control requirement | CloudHSM |
| Keep external key material outside AWS while using KMS APIs where supported | KMS external key store pattern; do not substitute CloudHSM automatically |

CloudHSM is not “more secure KMS” for every workload. It transfers more cluster,
availability, user and application-integration responsibility to the customer.

### FIPS Level 3 and scheduled key availability

Do not use “FIPS Level 3” by itself as a current CloudHSM discriminator. Current
AWS KMS HSMs and CloudHSM FIPS-mode HSMs have Level 3 validation. Choose from
the operating and ownership requirements:

| Requirement | Better fit |
|---|---|
| Managed, highly available regional key service; AWS-service integration; least operational effort | KMS |
| Dedicated single-tenant HSMs, direct cryptographic interfaces, customer-controlled HSM users/algorithms | CloudHSM |
| Customer-managed key must be unavailable outside a schedule | Schedule `DisableKey` and `EnableKey`; a disabled KMS key cannot perform cryptographic operations |
| CloudHSM-specific workload is intermittent | HSMs can be scaled to zero and later restored from backup, but this is a higher-operations answer justified only by the CloudHSM requirement |

Changing a key policy is not as complete or direct as disabling the KMS key:
grants and multiple principals complicate authorization, whereas key state
controls cryptographic use. Remember that disabling a key is eventually
consistent and does not revoke plaintext data keys already cached by an
application or service.

## Secrets Manager vs SSM Parameter Store

| Requirement | Better fit |
|---|---|
| Managed secret rotation | Secrets Manager |
| Config parameters, simple secure strings | SSM Parameter Store |
| RDS credential rotation integration | Secrets Manager |
| Lower-cost parameter storage for app config | Parameter Store |

Trap: Do not hardcode secrets in Lambda environment variables or ECS task definitions without secure reference/encryption patterns.

### Database-secret rotation strategies

| Strategy | Use | Important trade-off |
|---|---|---|
| Single user | Simplest strategy and the default for most use cases | There is a short interval between the database password change and secret update in which new connections can be denied; retry mitigates it |
| Alternating users | Application credential with an explicit highest-availability requirement during rotation | Maintains two database users and alternates them; needs a separate superuser secret and permission parity for the clone |

Exam rule: a failure caused by **hardcoded credentials** does not by itself
justify alternating users. If the requirement is least development effort,
store the credential in Secrets Manager, retrieve `AWSCURRENT`, and use
single-user automatic rotation. Select alternating users only when the stem
explicitly requires the smallest rotation-time denial window or highest
availability and accepts the extra superuser/clone complexity.

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

### Security Hub multi-account, multi-Region aggregation

For the least operational overhead across an organization, integrate Security
Hub with AWS Organizations, designate a delegated Security Hub administrator,
and manage organization accounts as members. For a genuinely multi-Region
reporting requirement, use central configuration or configure a
home/aggregation Region with linked Regions. Findings from linked Regions are
then replicated to the home Region for one reporting view.

Security Hub must still be enabled and configured in each relevant account and
Region; aggregation does not enable the service in linked Regions. Audit
Manager assembles assessment evidence and is not a substitute for consolidated
security findings. In an older answer set, an Organizations-integrated option
may omit the aggregation-Region step yet still beat a manual per-account
Security Hub option on operational overhead; retain the current home/linked
Region requirement when designing the real architecture.

### Tag policies: standardization versus required presence

Organizations tag policies define compliant tag-key spelling/case and allowed
values. For supported resource types, **Prevent noncompliant operations** can
block create/update operations that supply a noncompliant tag. Use Resource
Groups Tagging API/Tag Editor compliance views to find existing mismatches and
correct them through the service that owns each resource.

Important boundary: basic tag-policy enforcement does not force every resource
to carry a tag; an untagged resource is not noncompliant merely because the tag
policy defines that key. If mandatory tag presence is required, add an
appropriate creation-time control such as an SCP where the service supports
request-tag condition keys, or govern provisioning through approved IaC and
Service Catalog patterns. Therefore, a scenario that asks to standardize
existing tags **and** require the tags on future resources normally combines:

```text
tag policy        -> standardize supplied keys, case and values
Resource Groups   -> find/report existing noncompliance
service workflow  -> correct existing resources
SCP               -> deny supported create calls that omit required tags
```

### CloudTrail integrity versus encryption and access logging

One trail records both console and SDK API activity. Enable CloudTrail log-file
integrity validation when the requirement is to detect alteration or deletion
after delivery. CloudTrail writes signed digest files that chain hashes of the
logs. S3 encryption protects confidentiality at rest; S3 server access logging
records bucket access; neither proves that the CloudTrail files are unchanged.

### Organization conformance packs

Use an AWS Config organization conformance pack to deploy a common template of
Config rules and remediation actions across organization accounts, with an
excluded-account list where required. Enable trusted access and use the
management account or a registered delegated administrator. The service-linked
roles protect centrally deployed rules from ordinary member-account changes.

Organization conformance-pack deployment is Regional and AWS Config recording
must already exist. When deployment from a delegated administrator must include
the management account, ensure the required service-linked role also exists in
that account; do not assume delegated administration alone bootstraps it.

### Access Analyzer trust-zone rule

An external-access analyzer reasons over resource-based policies. The selected
zone of trust controls what counts as external:

```text
account zone      -> principals in other accounts, including same-org accounts,
                     can be reported as external
organization zone -> organization members are trusted; principals outside the
                     organization are external
```

Use an organization-zone analyzer when the requirement is to identify access
from outside the organization to supported resources such as SQS queues. Use
CloudTrail when the question asks who actually called an API; it does not list
every principal that a resource policy could authorize.

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

### Firewall Manager is the organization-scale control plane

Use AWS Firewall Manager when an organization needs centrally defined and
automatically applied policies for WAF, Shield Advanced, Network Firewall,
Route 53 Resolver DNS Firewall, security groups, or network ACLs across current
and future accounts/resources.

The underlying WAF/firewall/security-group rule performs the traffic control.
Firewall Manager distributes, scopes, monitors and can remediate that policy at
organization scale. AWS Config is a prerequisite for parts of its compliance
model; Config alone does not deploy the firewall protections.

Select the Firewall Manager policy type from the enforcement layer named in the
requirement:

- Use a **Network Firewall policy** when the requirement is to enforce a packet
  filter across organization accounts independently of their varied security
  groups. In a stateless rule group, lower numeric priorities run first: pass
  SSH from trusted CIDRs before the drop/default-drop path handles other SSH.
- Use a **content audit security group policy** when the requirement explicitly
  asks to inspect and remediate existing security-group rule content. A content
  audit policy expresses an allowed-rule model or a disallowed-rule model; do
  not assume one audit security group can mix both models.
- A **common security group policy** distributes a centrally managed primary
  group, but an allow rule in that group does not prove that other attached
  security groups lack a permissive SSH rule.

This distinction corrects the overly broad shortcut that every
organization-wide SSH-CIDR requirement automatically implies a content-audit
security-group policy.

### Multi-account structure follows common controls

For a large organization:

- place log archive, security tooling/audit and related security functions in
  separate security-owned accounts to preserve separation of duties;
- group ordinary workloads into production, development and test OUs and apply
  common SCPs at the OU level; and
- place genuinely exceptional workloads in an Exceptions OU, with narrowly
  scoped account-level controls only for their unique requirements.

Avoid one OU per workload when many workloads share the same environment
controls. Avoid mixing exceptions into ordinary OUs because their account-level
overrides become harder to discover and govern.

### Organizations backup policies

An Organizations backup policy centrally creates effective AWS Backup plans in
member accounts. Attach a complete, validated policy at the organization root
for a universal daily-EBS requirement. Inherited policy-created plans are
visible but not editable in member accounts, making this more reliable than
asking every administrator to launch Service Catalog or StackSet resources.

Backup policies still depend on valid vaults, selections and IAM roles. Validate
the effective policy and inspect the first actual backup jobs; policy attachment
alone is not recovery evidence.

### Artifact versus Audit Manager

| Need | Service |
|---|---|
| Download AWS compliance reports/certifications or manage AWS agreements | AWS Artifact |
| Collect and organize evidence about the customer’s AWS usage and controls against an assessment framework | AWS Audit Manager |

Audit Manager helps collect evidence; it does not declare the organization
compliant and is not a replacement for an auditor or legal assessment.

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

### Cross-account AssumeRole requires both sides

A cross-account role session succeeds only when both sides allow it:

```text
caller account identity policy
  -> allows sts:AssumeRole on the exact role ARN

role-owning account trust policy
  -> allows that caller as Principal
```

The role's permissions policy then defines what the resulting session can do.
An identity policy in the caller account cannot create trust by itself, and
changing an inline policy into a managed policy does not change its effect.

If exactly one named principal may assume the role, scope the trust-policy
`Principal` to that user's or role's ARN rather than trusting every user with a
wildcard. For workforce designs, prefer federation and roles to long-lived IAM
users, but still recognize the direct-user trust pattern when the exam stem
explicitly fixes an IAM user as the caller.

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
| “Cognito is the workforce AWS account portal” | Use IAM Identity Center for workforce account access; Cognito is for application identities. |
| “CloudHSM is the default for every customer-managed key” | KMS is the integrated managed default; select CloudHSM for explicit single-tenant HSM/application-interface requirements. |
| “FIPS Level 3 automatically means CloudHSM” | Current KMS HSMs also meet Level 3; choose CloudHSM only when dedicated-HSM control or interfaces are required. |
| “A member-account root user can bypass an SCP” | SCPs restrict member-account root; repair the organization policy through an authorized management-account principal. |
| “Firewall Manager filters packets itself” | It centrally deploys and governs supported protection policies; the underlying services enforce traffic rules. |

## Additional references

- Firewall Manager overview: https://docs.aws.amazon.com/waf/latest/developerguide/fms-chapter.html
- Audit Manager and Artifact boundary: https://docs.aws.amazon.com/audit-manager/latest/userguide/what-is.html
- Cryptography service decision guide: https://docs.aws.amazon.com/pdfs/decision-guides/latest/cryptography-on-aws-how-to-choose/cryptography-on-aws-how-to-choose.pdf
- SCP effects and management-account boundary: https://docs.aws.amazon.com/organizations/latest/userguide/orgs_manage_policies_scps.html
- Enable and disable KMS keys: https://docs.aws.amazon.com/kms/latest/developerguide/enabling-keys.html
- KMS resilience: https://docs.aws.amazon.com/kms/latest/developerguide/disaster-recovery-resiliency.html
