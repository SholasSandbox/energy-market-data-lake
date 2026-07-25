# SAP-C02 SCP Versus Permissions Boundary Closed-Book Retest - 6 Questions - 2026-07-24

<!-- markdownlint-disable MD013 MD060 -->

**Last revised:** 2026-07-25

## Purpose and Evidence Boundary

**Document role:** fresh, question-only exact-match retest.

This retest checks whether the SCP-versus-permissions-boundary decision model
transfers to unfamiliar scenarios involving IAM Identity Center-created roles,
delegated IAM administration, resource policies, and organization inheritance.
It contains no answer key, explanations, scoring hints, or marked responses.

Generate or possess this document now, but for useful spaced-recall evidence,
attempt it only after completing the planned model review and closing every
answer-bearing source. The preferred attempt date is **2026-07-26 or later**.

## Closed-Book Attempt Rules

1. Close the IAM/Identity Center study guide, hidden-gap model review,
   wrong-answer log, tracker, AWS documentation, search, and AI assistance.
2. Use one uninterrupted **18-minute** timer.
3. Choose exactly the number of responses requested. A multiple-response item
   receives credit only when the complete response set is correct.
4. Guess rather than leave an item blank.
5. Do not record explanations while answering.
6. Freeze the complete answer set before recording uncertainty or requesting
   marking. A frozen submission is one you will not edit after seeing feedback.

## Questions

### 1 - Choose TWO

A company places all research workload accounts in a `Research` OU. No
principal in those accounts may create or attach an internet gateway, including
administrators, automation roles, and future IAM Identity Center-created roles.
A centrally managed network account must retain the ability to create internet
gateways for approved shared networking designs. The solution should require
the least identity-by-identity administration.

Which TWO actions best meet the requirements?

- A. Add the deny only to the permissions boundary used by the current
  `ResearchDeveloper` permission set.
- B. Apply an RCP to the research accounts that denies external principals
  access to existing internet gateways.
- C. Attach an SCP denying the relevant internet-gateway actions to the
  `Research` OU.
- D. Deploy an AWS Config rule that reports newly created internet gateways
  and treat the report as the preventive control.
- E. Keep the central network account in a separately governed OU that does
  not inherit the research restriction.
- F. Add an inline deny policy manually to every existing role in every
  research account.

### 2 - Choose THREE

A platform team delegates IAM role creation to application teams. Application
teams may choose the permissions policies for roles they create, but every new
role must remain within a security-approved maximum-permissions envelope. The
teams must not be able to create an unbounded role, remove the required
boundary, or modify the boundary policy.

Which THREE controls directly implement this delegation model?

- A. Attach the approved boundary only to the platform administrator role and
  rely on it to propagate automatically to newly created roles.
- B. Create a customer managed policy that represents the approved permissions
  boundary for application roles.
- C. Attach an SCP that grants `iam:CreateRole` to application teams.
- D. Allow role creation only when the request specifies the approved boundary,
  using an `iam:PermissionsBoundary` condition.
- E. Give application teams permission to create new versions of the boundary
  policy so they can resolve deployment failures.
- F. Prevent the delegated teams from removing the required boundary or
  changing the approved boundary policy.

### 3 - Single choice

An `Analyst` permission set is assigned to a workforce group in a Lakehouse
account. IAM Identity Center provisions the corresponding `AWSReservedSSO_*`
role. The role's identity policy allows Athena queries, `s3:Get*`, `s3:List*`,
and `s3:PutObject`. A permissions boundary on the permission set allows Athena,
`s3:Get*`, and `s3:List*`, but does not allow `s3:PutObject`. The account
inherits the default `FullAWSAccess` SCP. A bucket policy grants `s3:PutObject`
to the IAM role ARN.

What happens when a session for that role tries to upload an object?

- A. The upload succeeds because the bucket policy grants the action directly
  to the role ARN.
- B. The upload succeeds because `FullAWSAccess` in the SCP grants every AWS
  action.
- C. The upload succeeds because permission-set policies take precedence over
  permissions boundaries.
- D. The upload is denied because the role's permissions boundary does not
  permit `s3:PutObject`.

### 4 - Choose TWO

An organization must prevent principals in every current and future member
account from stopping an organization trail or deleting its trail resources.
The restriction must cover IAM users, IAM roles, IAM Identity Center-created
roles, and member-account root users. The Organizations management account must
retain its separate control-plane authority.

Which TWO statements are correct?

- A. A boundary attached to the current administrator permission set covers
  all present and future principals in every member account.
- B. An SCP attached at the organization root can impose the preventive limit
  on principals in member accounts beneath that root.
- C. An RCP is required because CloudTrail API operations are always authorized
  only through a trail resource policy.
- D. The SCP grants the management account permission to administer the trail.
- E. The restriction can apply to a member-account root user, while SCPs do not
  restrict principals in the Organizations management account.
- F. An AWS Config rule is equivalent to the SCP because both prevent the API
  request before it occurs.

### 5 - Choose TWO

A sandbox workforce group receives a `SandboxBuilder` permission set. Only the
IAM role created from this permission set needs a maximum-permissions ceiling;
other roles in the sandbox account have separately approved duties and must not
inherit that ceiling. Administrators may later broaden the identity policy in
`SandboxBuilder`, but must never make that role more powerful than the approved
service set.

Which TWO actions best satisfy the requirement?

- A. Configure the approved permissions boundary in the `SandboxBuilder`
  permission set so it is applied to the generated IAM role.
- B. Attach an SCP containing the same service list to the sandbox account.
- C. Replace the permission set with an IAM user carrying long-lived access
  keys and attach the boundary to that user.
- D. Protect administration of the boundary policy and permission set so the
  workforce group cannot remove or weaken its own ceiling.
- E. Put the role in a new OU because Organizations can place individual IAM
  roles into OUs.
- F. Use an RCP because an RCP is the identity-level maximum for one role.

### 6 - Choose THREE

A role in a member account has an identity policy and permissions boundary that
both permit `kms:Decrypt`. A KMS key policy in `eu-west-1` permits that role to
use the key. An applicable RCP does not block principals from the organization.
However, the role's account inherits an SCP with an explicit deny for regional
service actions outside `eu-west-2`, and the request targets the key in
`eu-west-1`.

Which THREE statements are correct?

- A. The decrypt request is denied by the inherited SCP.
- B. The permissions boundary overrides the SCP because it is attached closer
  to the role.
- C. The KMS key-policy allow cannot override the applicable SCP explicit deny.
- D. The RCP grants `kms:Decrypt` because it does not block the organization
  principal.
- E. Adding `AdministratorAccess` to the role would override the inherited
  deny.
- F. Moving the account to an OU without that Region-deny inheritance could
  change the outcome, assuming all remaining grant and guardrail requirements
  continue to be satisfied.

## Frozen Submission Block

Do not complete this block until every answer is final.

```text
Start: 19:47
End: 20:05
Elapsed:
Uncertain: 1,2,5

1:CE
2:BDF
3:D
4:BE
5:AD
6:ACF
```

## Submission Rule

Submit the complete frozen block exactly once. Marking begins only after that
explicit submission. Do not reopen the study guide or request hints before the
answer set is frozen.

## Submission Status

Submission accepted on **2026-07-24** after the learner explicitly reported
completion. The frozen answer block above is preserved unchanged. The derived
elapsed time is **18 minutes**.

Scoring and answer explanations are recorded separately in
[`sap-c02-scp-permissions-boundary-closed-book-retest-review-20260724.md`](sap-c02-scp-permissions-boundary-closed-book-retest-review-20260724.md)
so this file remains question-only. Because the attempt occurred before the
preferred 2026-07-26 spacing date, it is useful immediate-remediation evidence
but not the planned spaced-recall proof.
