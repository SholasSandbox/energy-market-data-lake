<!-- markdownlint-disable MD013 MD060 -->

# SAP-C02 Full Mock 002 Spaced Retest — Assessment

**Assessed:** 2026-08-01
**Attempt:** [frozen eight-question submission](sap-c02-full-mock-002-spaced-retest-8q-20260801.md)
**Start / end:** 13:18 / 13:35
**Elapsed:** 17 minutes
**Result:** 8/8 (100%)

## Evidence Boundary

The attempt was submitted with `Submission Status: FROZEN` before this
answer-bearing assessment was created. Multiple-response questions are graded
by exact match. This is focused remediation evidence for the four Full Mock 002
gaps; it is not a full-domain mock or booking evidence.

## Score

| Question | Submitted | Correct | Result | Decision boundary |
|---:|---|---|---|---|
| 1 | B | B | Correct | IAM Identity Center for workforce AWS-account access |
| 2 | A, D | A, D | Correct | Identity Center for workforce; Cognito identity pool for application users |
| 3 | C | C | Correct | Migration Hub home Region |
| 4 | A, C, E | A, C, E | Correct | Discovery Agent, DataSync, and Migration Hub roles |
| 5 | B, E | B, E | Correct | Multi-node DAX cluster plus DAX client |
| 6 | C | C | Correct | Application request path through the DAX client |
| 7 | C | C | Correct | S3 interface endpoint for private on-premises access |
| 8 | A, D | A, D | Correct | Gateway endpoint for VPC origin; interface endpoint for on-premises origin |

| Question type | Correct | Total | Score |
|---|---:|---:|---:|
| Single response | 4 | 4 | 100% |
| Multiple response, exact match | 4 | 4 | 100% |
| Overall | 8 | 8 | 100% |

Questions 4 and 7 were marked uncertain. Both were correct. Uncertainty was
therefore calibrated conservatively and did not hide an additional knowledge
gap in this focused sample.

## Rationale

### 1. Workforce access control plane — B

IAM Identity Center is the multi-account workforce access control plane. An
organization instance can use a connected identity source, permission sets,
and group-to-account assignments. IAM Identity Center provisions and manages
the corresponding roles in assigned accounts and users obtain temporary role
credentials. Cognito is not the normal control plane for employee access to an
AWS Organizations estate.

### 2. Workforce and application identities — A and D

The two requirements describe different populations and access paths:

- employees accessing AWS accounts use IAM Identity Center permission sets and
  account assignments; and
- mobile application users can exchange an authenticated identity for scoped,
  temporary AWS credentials through a Cognito identity pool and IAM roles.

Permission sets create or update account roles; they are not direct credential
objects for mobile application customers.

### 3. Migration programme visibility — C

Migration Hub stores discovery and migration-tracking data in its configured
home Region. That one home Region can track applications migrating into
multiple target Regions. DataSync and S3 replication move supported workload
data or objects; neither copies Migration Hub's managed control-plane database
into another Region.

### 4. Discovery, transfer, and tracking — A, C, and E

Each service maps to a separate requirement:

- Application Discovery Agent collects detailed host, process, and TCP
  connection evidence from supported physical servers and VMs;
- DataSync transfers supported file or object data, including on-premises NFS
  data to Amazon EFS; and
- Migration Hub groups applications and tracks status reported by integrated
  migration tools from its home Region.

The VMware vCenter inventory module does not run on physical servers or, by
itself, provide their host-level process evidence. Application Migration
Service rehosts supported servers; it is not the NFS transfer service. DataSync
does not replace Migration Hub's programme-tracking role.

### 5. DAX availability and request path — B and E

A multi-node DAX cluster distributed across Availability Zones supplies the
required cache availability. The application must use the DAX client and the
cluster endpoint so supported DynamoDB operations reach DAX. DAX is
API-compatible, but creating a cluster does not transparently redirect requests
from a standard DynamoDB client.

Strongly consistent reads sent through DAX are passed through to DynamoDB and
are not cached. DynamoDB remains the durable system of record.

### 6. DAX integration diagnosis — C

The standard DynamoDB client sends requests directly to DynamoDB. The DAX
client uses the DAX cluster endpoint and performs routing and load balancing
across cluster nodes. Capacity changes, indexes, and DNS aliases do not insert
DAX into an application request path that bypasses it.

### 7. Private S3 access from on premises — C

An S3 interface endpoint exposes private IP addresses that an on-premises
network can reach over Direct Connect or Site-to-Site VPN when routing, DNS,
security, and endpoint policies permit it. An S3 gateway endpoint is associated
with VPC route tables and its connectivity cannot be extended to on-premises
clients through Direct Connect, VPN, transit gateway, or VPC peering.

### 8. Mixed VPC and on-premises S3 clients — A and D

The origin of the request determines the endpoint path:

- VPC workloads can use the no-additional-charge S3 gateway endpoint through
  their associated route tables; and
- on-premises clients use the reachable private IP addresses of an S3
  interface endpoint with the appropriate Resolver and private-DNS design.

AWS supports a combined pattern in which on-premises queries resolve to the
interface endpoint while VPC-originated traffic continues to use the gateway
endpoint. Gateway endpoint routes cannot be propagated to on-premises clients.

## Assessment

| Full Mock 002 gap | Retest result | Status |
|---|---:|---|
| IAM Identity Center versus Cognito | 2/2 | Focused remediation complete |
| Migration Hub home Region and migration-service boundaries | 2/2 | Focused remediation complete |
| DAX cluster and client integration | 2/2 | Focused remediation complete |
| S3 interface versus gateway endpoint by client origin | 2/2 | Focused remediation complete |

No new wrong-answer theme was exposed. The focused remediation gate is
complete. This does not prove broad transfer or long-term retention; monitor
all four distinctions in Full Mock 003 and later independent mocks rather than
adding another immediate retest.

## Sources

- [Configure access to AWS accounts with IAM Identity Center](https://docs.aws.amazon.com/singlesignon/latest/userguide/manage-your-accounts.html)
- [IAM roles created by IAM Identity Center](https://docs.aws.amazon.com/singlesignon/latest/userguide/identity-center-and-iam-roles.html)
- [Getting credentials from Amazon Cognito identity pools](https://docs.aws.amazon.com/cognito/latest/developerguide/getting-credentials.html)
- [Managing the Migration Hub home Region](https://docs.aws.amazon.com/migrationhub/latest/ug/home-region.html)
- [AWS Application Discovery Agent](https://docs.aws.amazon.com/application-discovery/latest/userguide/discovery-agent.html)
- [What is AWS DataSync?](https://docs.aws.amazon.com/datasync/latest/userguide/what-is-datasync.html)
- [Tracking migrations in Migration Hub](https://docs.aws.amazon.com/migrationhub/latest/ug/migrate-wt-track.html)
- [DAX: How it works](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DAX.concepts.html)
- [DAX and DynamoDB consistency](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DAX.consistency.html)
- [Gateway endpoints for Amazon S3](https://docs.aws.amazon.com/vpc/latest/privatelink/vpc-endpoints-s3.html)
