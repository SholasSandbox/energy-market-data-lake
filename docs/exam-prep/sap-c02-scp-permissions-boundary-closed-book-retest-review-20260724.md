# SAP-C02 SCP Versus Permissions Boundary Retest Review - 2026-07-24

<!-- markdownlint-disable MD013 MD060 -->

**Last revised:** 2026-07-25

## Result

| Field | Result |
|---|---|
| Questions | 6 |
| Exact-match score | **6/6 (100%)** |
| Start / end | 19:47 / 20:05 |
| Derived elapsed time | 18 minutes |
| Learner-marked uncertainty | Questions 1, 2, and 5 |
| Uncertain answers correct | 3/3 |
| Recurrence of original SCP/boundary trap | No |

The learner explicitly reported completion with a frozen answer block in the
question-only source. Compliance with the closed-book rules is learner-attested
by that submission and is not independently observable. The attempt occurred
on 2026-07-24, before the preferred 2026-07-26 spacing date, so it is recorded
as a successful immediate-remediation check rather than the planned
spaced-recall proof.

## Exact-Match Marking

| Question | Learner answer | Correct answer | Result |
|---:|---|---|---|
| 1 | C, E | C, E | Correct |
| 2 | B, D, F | B, D, F | Correct |
| 3 | D | D | Correct |
| 4 | B, E | B, E | Correct |
| 5 | A, D | A, D | Correct |
| 6 | A, C, F | A, C, F | Correct |

## Model Review

### 1 - OU-wide preventive restriction

**C and E.** An SCP attached to the `Research` OU limits principals in every
current and future member account beneath that OU. Keeping the central network
account in a separately governed OU prevents it from inheriting that research
restriction. A permissions boundary would cover only the IAM entities to which
it is attached; AWS Config detects configuration state but is not the requested
preventive authorization control.

### 2 - Safe delegated role creation

**B, D, and F.** The customer managed boundary policy defines the approved
maximum. The delegated creator's IAM permissions must require that boundary on
role creation, using the `iam:PermissionsBoundary` condition key, and must
prevent removal or modification of the protected ceiling. An SCP does not
grant `iam:CreateRole`, and allowing the team to alter the boundary would let
it escape the delegation model.

### 3 - Resource policy versus a role boundary

**D.** The S3 bucket policy names the IAM role ARN, so its allow remains
limited by an implicit deny in the role's permissions boundary. Because the
boundary does not allow `s3:PutObject`, the upload is denied. The close exam
trap is a policy that instead grants directly to an assumed-role **session
ARN**: within the same account, such a grant is not limited by an implicit deny
in the role boundary, although any explicit deny would still win.

### 4 - Organization-root CloudTrail guardrail

**B and E.** An SCP at the organization root limits principals in member
accounts below it, including member-account root users. SCPs do not restrict
principals in the Organizations management account and do not grant that
account any permissions. A Config rule is detective rather than an equivalent
preventive authorization limit.

### 5 - One Identity Center-generated role

**A and D.** Configuring a permissions boundary on the `SandboxBuilder`
permission set causes IAM Identity Center to attach the boundary to the IAM
role it creates. Protecting the boundary and permission-set administration
prevents the workforce group from weakening its own ceiling. An account SCP
would affect a broader principal scope than required.

### 6 - Region SCP and KMS key policy

**A, C, and F.** The inherited SCP explicitly denies the regional KMS request
outside `eu-west-2`. The KMS key-policy allow, role identity policy, boundary,
and `AdministratorAccess` cannot override that deny. Moving the account to an
OU without the Region-deny inheritance could change the result if all remaining
grant and guardrail requirements are satisfied. An RCP that does not block the
request grants nothing by itself.

## Durable Mental Model

```text
one role needs a ceiling       -> permissions boundary
accounts or an OU need a limit -> SCP
resources need an org limit    -> RCP

granting policy
  intersect boundary, session policy, SCP, and RCP where applicable
  then explicit deny wins
```

The role-ARN versus role-session-ARN distinction remains the advanced edge
case worth retaining. The focused trap is remediated, but recurrence monitoring
continues through independent full mocks.

## Official AWS References

- [Permissions boundaries for IAM entities](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_boundaries.html)
- [IAM roles created by IAM Identity Center](https://docs.aws.amazon.com/singlesignon/latest/userguide/identity-center-and-iam-roles.html)
- [Custom permissions and boundaries in permission sets](https://docs.aws.amazon.com/singlesignon/latest/userguide/permissionsetcustom.html)
- [AWS Organizations concepts and SCP inheritance](https://docs.aws.amazon.com/organizations/latest/userguide/orgs_getting-started_concepts.html)
- [Resource control policies](https://docs.aws.amazon.com/organizations/latest/userguide/orgs_manage_policies_rcps.html)
- [SCP syntax and Region restriction example](https://docs.aws.amazon.com/organizations/latest/userguide/orgs_manage_policies_scps_syntax.html)
