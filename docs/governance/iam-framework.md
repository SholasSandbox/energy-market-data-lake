<!-- markdownlint-disable MD013 MD060 -->

# Energy Lakehouse IAM Implementation Checklist

**Status:** Illustrative revision artifact; not approved for implementation<br>
**Last revised:** 2026-08-13<br>
**Primary SAP-C02 mapping:** Domain 1 security and governance; Domain 2 workload design<br>
**Repository mapping:** Lambda ingestion, Glue raw-to-curated processing, Athena query access, EventBridge scheduling, and Step Functions orchestration

## Purpose and safety boundary

This checklist shows how IAM could be expressed for the Energy Data Lakehouse
without changing AWS, Terraform, or the implemented permission model. It is a
revision aid for reading and comparing:

- role trust policies;
- identity-based permissions policies;
- Lambda execution-role policies;
- Lambda resource-based invocation policies; and
- the two-sided authorization required for role assumption.

The examples are valid JSON shapes with illustrative placeholders. They are
not deployment inputs, approved target state, or evidence that a permission is
live. Do not paste them into AWS or translate them into Terraform without a
separate design review, local policy validation, a reviewed plan, explicit
approval, rollback steps, and post-change evidence.

No item in this document authorizes an AWS API call or infrastructure change.

### Companion tutorial checklist

The separate tutorial workspace contains
`/Users/[redacted-user]/Kiro-Workspace/handlers/docs/iam/persistence-handler-iam-checklist.md`.
That checklist supplies the teaching pattern used here:

1. start from observed handler behavior;
2. map each SDK call to an IAM action and exact resource type;
3. keep the orchestrator role separate from the Lambda execution role;
4. list conditional and explicitly forbidden permissions; and
5. show illustrative JSON without treating it as deployable infrastructure.

The tutorial's S3 and DynamoDB persistence permissions are not Lakehouse
implementation evidence and are not copied into this repository. This document
applies the same reasoning method to the Lakehouse's own Lambda, S3, Glue,
Athena, EventBridge, and Step Functions boundaries.

## The policy question to ask first

| Policy type | Attached to | Main question | Uses `Principal`? | Lakehouse example |
|---|---|---|---:|---|
| Role trust policy | IAM role | Who may obtain a session as this role? | Yes | Allow the Lambda service to assume the ingestion execution role |
| Identity policy | User, group, or role | What actions may this identity perform on which resources? | No | Allow the ingestion role to put objects only under `raw/` |
| Lambda execution-role policy | Lambda's IAM role | What may the function do after Lambda assumes its role? | No | Write logs and place ingestion objects in S3 |
| Lambda resource policy | Lambda function, alias, or version | Who may call this function? | Yes | Allow one EventBridge rule to invoke the ingestion function |
| S3 bucket policy | S3 bucket | Who may access this bucket, and under what conditions? | Yes | Optional defense-in-depth around transport, organization, or named roles |
| SCP | Organization root, OU, or account | What is the maximum principal-side permission available in governed accounts? | No | Prevent selected high-risk actions across the Workloads OU |

### Core exam rule

```text
Trust policy     = who may become the role
Identity policy  = what the resulting identity may do
Resource policy  = who may access or invoke the named resource
SCP or boundary  = maximum permission, never a grant
```

A trust policy that names a principal does not give the assumed role permission
to use S3, Glue, Athena, or Lambda. Those permissions come from the role's
identity policies. Conversely, a broad identity policy on a role is useless to
a caller that cannot satisfy the role's trust policy and the applicable
`sts:AssumeRole` authorization path.

## Illustrative role inventory

| Role | Trusted principal | Identity permissions | Explicit non-permissions |
|---|---|---|---|
| Ingestion Lambda execution role | `lambda.amazonaws.com` | CloudWatch Logs and `s3:PutObject` under approved `raw/` dataset prefixes | No curated writes, deletes, bucket administration, or role passing |
| Glue service role | `glue.amazonaws.com` | Read `raw/`, `curated/`, and `scripts/`; write/delete only `curated/`; Glue service operations and logs | No raw writes/deletes or Athena-result access |
| Athena query role | One approved workforce or automation role | Use one workgroup, read one catalog/database, read `curated/`, manage bounded query results | No `raw/` reads, catalog mutation, workgroup administration, or result deletion |
| Step Functions execution role | `states.amazonaws.com` | Invoke the named orchestration Lambda and publish to the named failure topic | No direct S3, Bedrock, or broad Lambda access unless the state machine actually needs it |
| EventBridge target role | `events.amazonaws.com` | Start only the named state machine | No Lambda, S3, or general Step Functions administration |

The current repository contains broader and differently composed permissions in
places. These examples teach the intended least-privilege reasoning; they do
not silently change or reclassify the live state.

## Current code behavior to IAM mapping

This table follows the companion persistence checklist's code-first method.
The entries describe the observed Lakehouse path and the permission direction
to review; they are not a claim that the illustrative policies below are live.

| Observed behavior | Representative API or integration | Permission direction | Least-privilege resource scope |
|---|---|---|---|
| Ingestion Lambda stores fetched Elexon, ENTSO-E, and ENTSOG payloads | `s3.put_object(...)` | `s3:PutObject` on the Lambda execution role | Only the handler's durable `raw/` dataset prefixes |
| Ingestion Lambda emits runtime logs | Lambda runtime to CloudWatch Logs | `logs:CreateLogStream` and `logs:PutLogEvents` on the execution role | Only `/aws/lambda/<INGEST_FUNCTION_NAME>` when the log group is pre-created |
| Daily ingestion schedule invokes the ingestion Lambda | EventBridge rule target | `lambda:InvokeFunction` in the Lambda function's resource policy | Only the named function and source rule ARN |
| Glue lists raw, curated, and script keys | S3 list APIs | `s3:ListBucket` with `s3:prefix` conditions on the Glue role | The data bucket with only `raw/`, `curated/`, and `scripts/` prefixes |
| Glue loads source/script objects and writes transformed output | S3 get/put/delete APIs | Object actions on the Glue role | Read selected inputs; mutate only `curated/*` |
| Athena executes through the Lakehouse workgroup | Athena query APIs | Athena actions on the query role | Only the named workgroup ARN |
| Athena resolves catalog metadata | Glue Data Catalog read APIs | Read-only Glue actions on the query role | Catalog, Lakehouse database, and its tables |
| Athena reads data and writes results | S3 APIs used by Athena | Curated reads plus bounded result-object writes on the query role | `curated/*` and `athena-results/*`, never `raw/*` |
| EventBridge starts the scheduled orchestration | EventBridge target role | `states:StartExecution` on the target role | Only the named state machine |
| State machine calls the orchestration function | Step Functions Lambda integration | `lambda:InvokeFunction` on the state-machine role | Only the named function and required aliases/versions |
| Orchestration Lambda reads/writes workflow artifacts and optionally invokes a model | S3 and Bedrock Runtime SDK calls | S3 and optional `bedrock:InvokeModel` on the Lambda execution role | Exact data/dashboard prefixes and selected model ARN |

### Role-separation checkpoint

```text
EventBridge target role
  -> states:StartExecution only

Step Functions execution role
  -> lambda:InvokeFunction
  -> sns:Publish for the named failure topic

Lambda execution role
  -> runtime S3, logging, and optional Bedrock calls
```

Do not fix a Lambda access error by adding S3 or Bedrock permissions to the
Step Functions role. Do not fix a state-machine invocation error by adding
`states:StartExecution` to the Lambda role. Permission belongs to the identity
that makes the failing API request.

## Checklist 1: role trust policies

Before approving any trust policy, check:

- [ ] The trusted principal is a specific AWS service, account, role, or
  federated provider; it is not an unexplained wildcard.
- [ ] The action matches the federation path: normally `sts:AssumeRole`, or the
  appropriate SAML/OIDC operation where federation is used.
- [ ] Service principals use the service's documented principal name.
- [ ] Cross-service access is restricted with `aws:SourceArn` and
  `aws:SourceAccount` where those keys are supported and meaningful.
- [ ] Cross-account trust identifies both sides: the target role trusts the
  source, and the source identity is authorized to call `sts:AssumeRole`.
- [ ] Third-party trust uses a unique external ID when the confused-deputy risk
  applies.
- [ ] Session duration, source identity, session tags, MFA, and organization
  conditions are considered for human or cross-account sessions.
- [ ] The trust policy is not mistaken for the role's permissions policy.

### Example 1A: Lambda execution-role trust policy

This policy belongs on the ingestion Lambda's IAM role. It lets the Lambda
service obtain temporary credentials for that role. It does not allow the
function to write S3 objects or logs by itself.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "TrustLambdaService",
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

Revision cue: `lambda.amazonaws.com` belongs in the trust policy; S3 actions
belong in the execution role's identity policy.

### Example 1B: Glue service-role trust policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "TrustGlueService",
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

Revision cue: Glue needs both this trust relationship and identity permissions
for the catalog, logs, scripts, raw inputs, and curated outputs it actually
uses.

### Example 1C: Step Functions execution-role trust policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "TrustStepFunctionsService",
      "Effect": "Allow",
      "Principal": {
        "Service": "states.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

Revision cue: Step Functions assumes this role. The role then needs an identity
policy granting `lambda:InvokeFunction` on each Lambda task it calls. The
Lambda function does not need Step Functions permissions merely because it is
invoked by a state machine.

### Example 1D: EventBridge target-role trust policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "TrustEventBridgeService",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

Revision cue: when EventBridge targets a Step Functions state machine through a
target role, the target role needs `states:StartExecution`. This is different
from EventBridge directly invoking Lambda through a Lambda resource policy.

### Example 1E: tightly scoped Athena role trust

This study example trusts one named workforce role. Replace the placeholder
with the actual approved role only after deciding how IAM Identity Center or
automation will reach the query role.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "TrustApprovedLakehouseAnalystRole",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::<ACCOUNT_ID>:role/<APPROVED_CALLER_ROLE>"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

Important nuance: an MFA condition can be appropriate for a direct IAM-user
role switch but does not fit every IAM Identity Center, federated, or workload
session. Choose conditions from the actual federation path rather than adding
controls that the caller cannot satisfy. For IAM Identity Center permission-set
roles, also account for their generated ARN path and suffix when choosing a
durable trust pattern.

## Checklist 2: identity policies

For every identity policy, check:

- [ ] Each statement has one clear purpose and a meaningful `Sid`.
- [ ] Actions match the runtime call path rather than a service-level wildcard.
- [ ] Resource ARNs are as specific as the service supports.
- [ ] S3 bucket actions use the bucket ARN; object actions use object ARNs.
- [ ] `s3:ListBucket` is constrained with `s3:prefix` when only selected zones
  should be visible.
- [ ] Write, delete, administration, and `iam:PassRole` permissions are absent
  unless a demonstrated call path requires them.
- [ ] KMS permissions are added only when the selected encryption path uses a
  customer-managed key and the key policy also permits the principal.
- [ ] CloudWatch Logs permissions target the correct log group where practical.
- [ ] Wildcard resources are retained only for actions that do not support
  resource-level permissions, with the reason documented.
- [ ] SCPs, permissions boundaries, session policies, resource policies, and
  explicit denies are reviewed as additional limits; none is treated as a
  grant.

### Example 2A: ingestion Lambda execution policy

This example reflects the current handler's essential behavior: write logs and
store fetched market data under approved `raw/` dataset prefixes. The exact
prefix list must follow the handler's durable key contract.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "WriteFunctionLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:eu-west-2:<ACCOUNT_ID>:log-group:/aws/lambda/<INGEST_FUNCTION_NAME>:*"
    },
    {
      "Sid": "WriteApprovedRawDatasets",
      "Effect": "Allow",
      "Action": "s3:PutObject",
      "Resource": [
        "arn:aws:s3:::<DATA_BUCKET>/raw/elexon/*",
        "arn:aws:s3:::<DATA_BUCKET>/raw/entsoe/*",
        "arn:aws:s3:::<DATA_BUCKET>/raw/entsog/*"
      ]
    }
  ]
}
```

Why the example omits common permissions:

- no `s3:ListBucket`: the handler writes deterministic keys and does not list;
- no `s3:GetObject`: the ingestion handler does not read the stored objects;
- no `s3:PutObjectAcl`: S3 Object Ownership and bucket controls should avoid
  per-object ACL administration where possible;
- no `s3:DeleteObject`: ingestion must not erase source evidence; and
- no `logs:CreateLogGroup`: the repository manages the log group separately.

If log groups are not pre-created, the execution role may also need
`logs:CreateLogGroup`, normally through `AWSLambdaBasicExecutionRole`. That is
an operational choice, not a reason to broaden S3 access.

### Example 2B: Glue raw-to-curated S3 policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadBucketLocation",
      "Effect": "Allow",
      "Action": "s3:GetBucketLocation",
      "Resource": "arn:aws:s3:::<DATA_BUCKET>"
    },
    {
      "Sid": "ListRequiredPrefixes",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:ListBucketVersions"
      ],
      "Resource": "arn:aws:s3:::<DATA_BUCKET>",
      "Condition": {
        "StringLike": {
          "s3:prefix": [
            "raw",
            "raw/*",
            "curated",
            "curated/*",
            "scripts",
            "scripts/*"
          ]
        }
      }
    },
    {
      "Sid": "ReadInputsCatalogOutputsAndScripts",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion"
      ],
      "Resource": [
        "arn:aws:s3:::<DATA_BUCKET>/raw/*",
        "arn:aws:s3:::<DATA_BUCKET>/curated/*",
        "arn:aws:s3:::<DATA_BUCKET>/scripts/*"
      ]
    },
    {
      "Sid": "WriteCuratedObjectsOnly",
      "Effect": "Allow",
      "Action": [
        "s3:AbortMultipartUpload",
        "s3:DeleteObject",
        "s3:ListMultipartUploadParts",
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::<DATA_BUCKET>/curated/*"
    }
  ]
}
```

Revision cue: `s3:ListBucket` cannot use an object ARN. It applies to the bucket
and uses a prefix condition. `s3:GetObject` and `s3:PutObject` apply to object
ARNs. Glue may replace curated outputs, but it should not write or delete raw
source objects.

The Glue role also needs its required Glue service and logging permissions.
The repository currently supplies those with the AWS-managed
`AWSGlueServiceRole` policy and keeps data-bucket access in a separate custom
policy.

### Example 2C: Athena curated-query identity policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "UseOneLakehouseWorkgroup",
      "Effect": "Allow",
      "Action": [
        "athena:BatchGetQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:GetQueryResultsStream",
        "athena:GetQueryRuntimeStatistics",
        "athena:GetWorkGroup",
        "athena:ListQueryExecutions",
        "athena:StartQueryExecution",
        "athena:StopQueryExecution"
      ],
      "Resource": "arn:aws:athena:eu-west-2:<ACCOUNT_ID>:workgroup/<WORKGROUP_NAME>"
    },
    {
      "Sid": "ReadLakehouseCatalog",
      "Effect": "Allow",
      "Action": [
        "glue:BatchGetPartition",
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:GetTable",
        "glue:GetTables",
        "glue:GetTableVersion",
        "glue:GetTableVersions"
      ],
      "Resource": [
        "arn:aws:glue:eu-west-2:<ACCOUNT_ID>:catalog",
        "arn:aws:glue:eu-west-2:<ACCOUNT_ID>:database/<DATABASE_NAME>",
        "arn:aws:glue:eu-west-2:<ACCOUNT_ID>:table/<DATABASE_NAME>/*"
      ]
    },
    {
      "Sid": "ReadBucketLocation",
      "Effect": "Allow",
      "Action": "s3:GetBucketLocation",
      "Resource": "arn:aws:s3:::<DATA_BUCKET>"
    },
    {
      "Sid": "ListCuratedAndQueryResults",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:ListBucketVersions"
      ],
      "Resource": "arn:aws:s3:::<DATA_BUCKET>",
      "Condition": {
        "StringLike": {
          "s3:prefix": [
            "curated",
            "curated/*",
            "athena-results",
            "athena-results/*"
          ]
        }
      }
    },
    {
      "Sid": "ReadCuratedObjects",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion"
      ],
      "Resource": "arn:aws:s3:::<DATA_BUCKET>/curated/*"
    },
    {
      "Sid": "ManageBoundedQueryResults",
      "Effect": "Allow",
      "Action": [
        "s3:AbortMultipartUpload",
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListMultipartUploadParts",
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::<DATA_BUCKET>/athena-results/*"
    }
  ]
}
```

Revision cue: a workgroup can enforce settings such as the query-result
location, but it does not grant the caller S3 or Glue Catalog access. IAM must
authorize all three planes: Athena API, catalog metadata, and S3 data/results.

### Example 2D: caller-side permission to assume the Athena role

This policy belongs to the approved caller, not to the Athena query role.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AssumeOnlyLakehouseQueryRole",
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Resource": "arn:aws:iam::<ACCOUNT_ID>:role/<ATHENA_QUERY_ROLE>"
    }
  ]
}
```

Cross-account role assumption normally needs both this caller-side grant and a
target-role trust policy that trusts the caller's account or role. For a
same-account principal named directly in the trust policy, IAM evaluation has
additional resource-policy nuances; the safe exam model is still to inspect
both sides rather than assume that one document always tells the whole story.

### Example 2E: Step Functions invokes only the orchestration Lambda

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "InvokeNamedOrchestrationFunction",
      "Effect": "Allow",
      "Action": "lambda:InvokeFunction",
      "Resource": [
        "arn:aws:lambda:eu-west-2:<ACCOUNT_ID>:function:<ORCHESTRATION_FUNCTION>",
        "arn:aws:lambda:eu-west-2:<ACCOUNT_ID>:function:<ORCHESTRATION_FUNCTION>:*"
      ]
    },
    {
      "Sid": "PublishNamedFailureTopic",
      "Effect": "Allow",
      "Action": "sns:Publish",
      "Resource": "arn:aws:sns:eu-west-2:<ACCOUNT_ID>:<FAILURE_TOPIC>"
    }
  ]
}
```

Revision cue: the qualified Lambda ARN pattern is needed if the state machine
invokes a version or alias. Step Functions does not inherit the Lambda
execution role's S3 or Bedrock permissions.

### Example 2F: EventBridge starts only the named state machine

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "StartNamedLakehouseStateMachine",
      "Effect": "Allow",
      "Action": "states:StartExecution",
      "Resource": "arn:aws:states:eu-west-2:<ACCOUNT_ID>:stateMachine:<STATE_MACHINE_NAME>"
    }
  ]
}
```

Revision cue: EventBridge assumes its target role, then the identity policy on
that role authorizes `states:StartExecution`. This is not a Lambda execution
policy and does not authorize the state machine's later tasks.

## Checklist 3: Lambda-specific policies

For each Lambda function, separately review three directions:

1. **Who can assume its execution role?** Usually the Lambda service through
   the role trust policy.
2. **What can the function do?** The execution role's identity policies.
3. **Who can invoke the function?** The function's resource-based policy, or an
   invoking principal's identity authorization where that model applies.

Check the following:

- [ ] The execution role trusts only the correct service or federation path.
- [ ] Runtime access is derived from actual SDK/API calls in the handler.
- [ ] Logging permissions are present and independent of business-data access.
- [ ] A service invocation statement uses `lambda:InvokeFunction`, a specific
  service principal, and a source restriction.
- [ ] `aws:SourceArn` identifies the expected EventBridge rule, S3 bucket, SNS
  topic, or other supported source.
- [ ] `aws:SourceAccount` is included where supported to reduce confused-deputy
  exposure, especially when the source ARN does not contain an account ID.
- [ ] Permission is scoped to the intended function, version, or alias.
- [ ] The function is not granted permission to invoke itself unless recursion
  is explicitly required and controlled.
- [ ] VPC access, KMS, Secrets Manager, SQS/Kinesis event-source mapping, and
  dead-letter permissions are added only when the configured feature needs
  them.

### Example 3A: EventBridge may invoke the ingestion Lambda

This is a Lambda resource-based policy. It is attached to the function, not to
the EventBridge rule and not to the Lambda execution role.

```json
{
  "Version": "2012-10-17",
  "Id": "default",
  "Statement": [
    {
      "Sid": "AllowNamedScheduleToInvokeIngestion",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "lambda:InvokeFunction",
      "Resource": "arn:aws:lambda:eu-west-2:<ACCOUNT_ID>:function:<INGEST_FUNCTION_NAME>",
      "Condition": {
        "ArnLike": {
          "AWS:SourceArn": "arn:aws:events:eu-west-2:<ACCOUNT_ID>:rule/<INGEST_SCHEDULE_RULE>"
        },
        "StringEquals": {
          "AWS:SourceAccount": "<ACCOUNT_ID>"
        }
      }
    }
  ]
}
```

The `Principal` is EventBridge because EventBridge calls Lambda. The condition
limits that service permission to one rule and account. The execution role's
S3 policy is evaluated only after the function starts and calls S3.

### Example 3B: why Step Functions usually looks different

For the repository's state-machine-to-Lambda path, the main grant is the
Step Functions execution role's identity policy in Example 2E. Step Functions
uses that role to call Lambda. Do not automatically add a second broad Lambda
resource statement merely because a state machine invokes the function.

Exam cue:

```text
EventBridge rule -> Lambda
  commonly: Lambda resource policy permits events.amazonaws.com

EventBridge rule -> Step Functions
  EventBridge target role permits states:StartExecution

Step Functions -> Lambda
  Step Functions execution role permits lambda:InvokeFunction
```

## Optional encryption overlay

The current public-data Lakehouse retains SSE-S3 unless a documented promotion
trigger authorizes SSE-KMS. If a customer-managed KMS key is later selected,
review both sides:

- the caller's identity policy must permit the required KMS operation; and
- the KMS key policy must allow the principal or delegate permission through
  IAM.

Illustrative runtime statement:

```json
{
  "Sid": "UseApprovedLakehouseKey",
  "Effect": "Allow",
  "Action": [
    "kms:Decrypt",
    "kms:Encrypt",
    "kms:GenerateDataKey"
  ],
  "Resource": "arn:aws:kms:eu-west-2:<ACCOUNT_ID>:key/<KEY_ID>"
}
```

Do not add `kms:*`, and do not add KMS permissions when SSE-S3 is the selected
path. Glue, Athena, S3 replication, logging, and cross-account access can each
need different KMS actions and key-policy principals.

## End-to-end reasoning checklist

Use this sequence for an SAP-C02 scenario or future Lakehouse design review:

1. Identify the human, workload, or AWS service making the first request.
2. Identify whether it uses its own identity, assumes a role, or invokes a
   resource through a service integration.
3. Inspect the role trust policy or resource-based policy for that request.
4. Inspect the caller's identity policy where the authorization model requires
   one.
5. Inspect the resulting role's identity policy for the downstream API call.
6. Match every action to the correct resource ARN type and condition keys.
7. Apply permissions boundaries, session policies, SCPs, RCPs, resource
   policies, and explicit denies as ceilings or additional gates.
8. For KMS-protected data, verify both IAM authorization and the key policy.
9. Test one expected allow and at least one meaningful deny.
10. Record the evidence boundary: policy shape, local validation, deployed
    configuration, and successful runtime behavior are four different claims.

### Worked request path: scheduled ingestion

```text
EventBridge schedule
  -> Lambda resource policy allows the named rule to invoke the function
  -> Lambda service assumes the execution role through its trust policy
  -> execution-role identity policy allows CloudWatch Logs writes
  -> execution-role identity policy allows PutObject only under approved raw prefixes
  -> S3 bucket policy, SCPs, and any KMS key policy must also permit the request
```

### Worked request path: scheduled AI orchestration

```text
EventBridge schedule
  -> EventBridge assumes its target role
  -> target-role identity policy allows StartExecution on one state machine
  -> Step Functions assumes its execution role
  -> state-machine role allows InvokeFunction on the named Lambda
  -> Lambda service assumes the Lambda execution role
  -> Lambda execution-role policies allow only its required S3, logging, and optional Bedrock calls
```

### Worked request path: analyst query

```text
Approved analyst or automation identity
  -> caller identity policy allows AssumeRole on the Athena query role
  -> Athena role trust policy accepts that caller and required conditions
  -> Athena role identity policy permits one workgroup
  -> Glue permissions expose only the intended catalog/database/tables
  -> S3 permissions expose curated data and bounded query results, not raw data
  -> KMS key policy and IAM permissions join the path only if SSE-KMS is used
```

## Anti-pattern review

Reject or challenge these shapes unless the scenario explicitly justifies them:

- `"Principal": "*"` in an allow trust or Lambda resource statement;
- `"Action": "*"` or `<service>:*` for a runtime role;
- `"Resource": "*"` where the action supports a specific ARN;
- one shared execution role for unrelated Lambda functions;
- Step Functions receiving S3 permissions that only its Lambda task uses;
- a Lambda execution role receiving permission for EventBridge to invoke it;
- confusing `iam:PassRole` with `sts:AssumeRole`;
- using an S3 object ARN for `s3:ListBucket`;
- assuming an Athena workgroup grants Glue Catalog or S3 access;
- adding KMS permissions without checking the key policy;
- treating an SCP or permissions boundary as an allow; and
- claiming least privilege from policy text without a representative allow and
  deny test.

## Evidence and future implementation gates

This document is complete as a study artifact when:

- [x] trust, identity, execution-role, and Lambda resource-policy boundaries
  are distinguished;
- [x] examples map to real Lakehouse components without changing them;
- [x] S3 bucket/object ARN and prefix-condition differences are visible;
- [x] EventBridge-to-Lambda, EventBridge-to-Step-Functions, and
  Step-Functions-to-Lambda paths are separated;
- [x] role assumption shows both target trust and caller authorization; and
- [x] all example documents remain illustrative and locally parseable.

Any future implementation requires all of the following before it may proceed:

- [ ] a named tracker gap and approved target role;
- [ ] comparison against the current Terraform and live evidence;
- [ ] IAM Access Analyzer policy validation where available;
- [ ] local repository contract checks;
- [ ] a reviewed Terraform plan with no unrelated changes or destroys;
- [ ] explicit user approval for the precise AWS mutation;
- [ ] rollback instructions;
- [ ] positive and negative runtime tests; and
- [ ] durable evidence that distinguishes configured permissions from proven
  behavior.

## Relationship to current repository artifacts

- `infra/terraform/lakehouse/iam.tf` contains the implemented Terraform role
  and policy definitions. This checklist does not modify them.
- `docs/adr/0004-glue-athena-access-boundaries.md` records the accepted and
  live-verified Glue/Athena boundary.
- `docs/glue-athena-iam-deployment-runbook.md` records the already completed
  deployment and verification process for that boundary.
- `scripts/check_lakehouse_iam_policies.py` validates selected current
  Terraform contracts; it does not validate this illustrative target model.
- `docs/policies/kms-lakehouse-key-policy.example.json` shows the separate KMS
  key-policy side of a possible future SSE-KMS design.
- `docs/planning/sap-c02-readiness-tracker.md` remains the controlling source
  for sequencing, status, and any future implementation decision.

## Official references

- [IAM policies and permissions](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html)
- [IAM role trust policies](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_update-role-trust-policy.html)
- [Principal policy element](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_principal.html)
- [AWS STS AssumeRole](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html)
- [Managing permissions in Lambda](https://docs.aws.amazon.com/lambda/latest/dg/lambda-permissions.html)
- [Lambda resource-based policies](https://docs.aws.amazon.com/lambda/latest/dg/access-control-resource-based.html)
- [Step Functions invoking Lambda](https://docs.aws.amazon.com/step-functions/latest/dg/connect-lambda.html)
- [EventBridge target roles](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-events-iam-roles.html)
- [EventBridge resource-based policies](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-use-resource-based.html)
- [Fine-grained Glue Data Catalog access](https://docs.aws.amazon.com/athena/latest/ug/fine-grained-access-to-glue-resources.html)
- [Athena workgroup IAM policies](https://docs.aws.amazon.com/athena/latest/ug/workgroups-iam-policy.html)
- [Amazon S3 policy condition keys](https://docs.aws.amazon.com/AmazonS3/latest/userguide/amazon-s3-policy-keys.html)
