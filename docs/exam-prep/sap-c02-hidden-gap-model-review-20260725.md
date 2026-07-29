# SAP-C02 Hidden-Gap Model Review - 2026-07-25

<!-- markdownlint-disable MD013 MD060 -->

**Last revised:** 2026-07-28

## Document Role and Boundary

**Role:** answer-bearing model review and remediation guide.

Use this document during the planned 2026-07-25 review session. Do not open it
during a closed-book diagnostic, spaced retest, or full mock examination.
Rereading this document is remediation evidence, not recall or test evidence.

This review addresses the two repeated exact-match traps found in full mock 001
and the later 15-question hidden-gap diagnostic:

1. organization-wide SCP scope versus identity-level permissions boundaries;
2. DynamoDB write sharding and its fan-out read consequence.

## Recommended 35-Minute Session

| Time | Activity | Output |
|---:|---|---|
| 10 minutes | Review the SCP and permissions-boundary model | Recreate the organization-to-identity hierarchy |
| 10 minutes | Review the DynamoDB sharding model | Recreate the write and read paths |
| 10 minutes | Close this document and reconstruct both models | Two closed-book diagrams and decision rules |
| 5 minutes | Explain the losing choices | One concise explanation for each repeated trap |

Do not score the rereading session. The scored evidence comes from the later
fresh retest.

## Model 1 - SCP Versus IAM Permissions Boundary

### Scope hierarchy

```text
AWS Organization
└── Organizational Unit
    └── Member account
        └── IAM user or role
```

### Control comparison

| Control | Scope | What it does | What it does not do |
|---|---|---|---|
| Service Control Policy | Organization, OU, or member account | Sets the maximum permissions available to affected principals | Does not grant permission |
| Permissions boundary | Specific IAM user or role | Caps what identity policies can grant to that identity | Does not grant permission and does not automatically constrain every identity in an account |
| Identity-based policy | IAM user, group, or role | Grants permissions to the identity, subject to other applicable controls | Cannot override an applicable explicit deny |
| Resource-based policy | One resource | Controls access to that resource | Does not replace organization and identity evaluation |
| Session policy | One temporary session | Further limits the session's available permissions | Does not expand the permissions of the parent identity |

### Core decision rule

> Use an SCP for an organization-wide, OU-wide, or account-wide preventive
> restriction. Use a permissions boundary to constrain a delegated IAM user or
> role.

### Region-restriction pattern

For a workload Region restriction:

1. apply the SCP to the workload OU;
2. evaluate regional actions using `aws:RequestedRegion`;
3. preserve necessary global-service exceptions carefully;
4. place accounts requiring different inheritance, such as central security
   accounts, in a separate OU; and
5. remember that member-account administrator permissions cannot override an
   inherited explicit deny.

`aws:RequestedRegion` evaluates the Region that received the API request. Many
global services use a single endpoint hosted in one Region—commonly
`us-east-1`—so they are **not exempt from the condition key by default**. If
that endpoint Region is outside the allowed list, a deny-outside-approved-
Regions SCP must explicitly exclude every required global service, normally
with `NotAction`; alternatively, allowing the endpoint Region prevents that
specific deny but can also permit Regional services there. In this repository's
example, the explicit exception list includes `route53:*` as well as `iam:*`,
`cloudfront:*`, and the other approved global services. Keep the exception list
narrow: `NotAction` prevents the regional deny from applying; it does not grant
access.

Exam trap: “global” does not mean “automatically ignored by
`aws:RequestedRegion`.” Route 53, IAM, and CloudFront requests would be caught
by a deny that excludes `us-east-1` unless their required actions are explicitly
excepted.

### Policy-evaluation checklist

Before choosing an answer, ask:

1. Is the required scope the organization, OU, account, resource, or identity?
2. Does the proposed policy grant permission or only limit it?
3. Which accounts and identities inherit the control?
4. Is there an explicit deny?
5. Does any account require a different inheritance path?
6. Is the scenario asking for prevention or only detection and remediation?

### Why the permissions-boundary answer loses

Attaching a permissions boundary to every developer role is identity-by-identity
administration. It does not create the requested OU-wide preventive guardrail,
does not automatically cover every current and future principal, and is not the
least-administration solution when the restriction belongs at the workload-OU
boundary.

### SCP and permissions-boundary references

- [AWS Organizations service control policies](https://docs.aws.amazon.com/organizations/latest/userguide/orgs_manage_policies_scps.html)
- [IAM permissions boundaries](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_boundaries.html)
- [IAM requested-Region policy example and global-service exceptions](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_examples_aws_deny-requested-region.html)

## Model 2 - DynamoDB Write Sharding and Fan-Out Reads

### Write path

```text
Hot logical key
gateway-123
    │
    ├── shard function ──> gateway-123#0
    ├── shard function ──> gateway-123#1
    ├── shard function ──> gateway-123#2
    └── shard function ──> gateway-123#N

Result: writes are distributed across a larger partition-key space.
```

### Read path for the complete logical entity

```text
Query gateway-123#0 ─┐
Query gateway-123#1 ─┤
Query gateway-123#2 ─┼──> merge results ──> order by sort key
Query gateway-123#N ─┘
```

### Core decision rules

- On-demand and provisioned modes manage capacity; neither repairs a badly
  concentrated partition-key design.
- Write sharding distributes writes across multiple partition-key values.
- Random suffixes spread writes but generally require checking all possible
  shards when reading the full logical entity.
- A calculated suffix can support efficient point reads when the application
  can derive the target shard from a known value.
- Reading a logical entity that spans shards requires multiple `Query`
  operations, normally executed in parallel, followed by application-level
  aggregation and ordering.
- DynamoDB Accelerator caches reads; it does not redistribute base-table or
  global-secondary-index writes.
- DynamoDB Streams records item changes; enabling a stream does not repair a
  hot partition-key design.

### Base table and index distinction

A base table can have well-distributed keys while a global secondary index is
hot because its index partition key has low cardinality, such as `OPEN`.
Sharding index keys into values such as `OPEN#0` through `OPEN#N` distributes
index writes, but queries for all open items must read the relevant index shards
and merge the results.

### Why the DAX answer loses

DynamoDB Accelerator can reduce repeated read latency, but it does not change
where GSI writes land. Selecting DAX without selecting the required fan-out
query and merge step misses the architectural consequence of sharding.

### DynamoDB sharding references

- [DynamoDB partition-key write sharding](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-sharding.html)
- [DynamoDB GSI write sharding and parallel queries](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-indexes-gsi-sharding.html)

## Closed-Book Reconstruction

After reviewing both models, close this document. Without notes, complete one
copy of this template for each model:

```text
Requirement:
Required control or data scope:
Winning design:
Why it wins:
Why the attractive alternative loses:
Operational consequence or trade-off:
```

Then redraw:

1. the Organization -> OU -> account -> IAM identity hierarchy; and
2. the DynamoDB sharded write path plus fan-out read-and-merge path.

## Completion Record

Complete this only after the closed-book reconstruction.

```text
Review date: 2026-07-25
Start:
End:
SCP model reconstructed without notes: Yes / No
DynamoDB model reconstructed without notes: Yes / No
Remaining uncertainty:
```

## Next Evidence Step

Complete a fresh six-question exact-match retest after appropriate spacing,
preferably on 2026-07-26 or 2026-07-27. Do not repeat the same 15 diagnostic
questions. A clean retest is required before treating either recurring trap as
resolved and before using full mock 002 as confirmation evidence.
