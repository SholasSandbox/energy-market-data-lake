<!-- markdownlint-disable MD013 MD060 -->

# SAP-C02 Full Mock 002 — Closed-Book Spaced Retest

**Created:** 2026-08-01
**Questions:** 8
**Suggested limit:** 24 minutes
**Mode:** Fresh, closed-book, close-distractor remediation
**Completed:** 2026-08-01
**Elapsed:** 17 minutes
**Submission status:** FROZEN

## Purpose and Evidence Boundary

This question-only retest covers the four genuine gaps from Full Mock 002:

1. workforce AWS-account access versus application-user identity;
2. Migration Hub home-Region visibility versus data transfer;
3. DAX cluster deployment plus DAX client integration; and
4. S3 interface versus gateway endpoints for on-premises access.

The questions use fresh scenarios and distractors. They do not reuse Full Mock
002 wording. This is focused remediation evidence, not a full-domain mock and
not booking evidence.

No answer key or rationale belongs in this document before the submission is
frozen. Keep the wrong-answer log, lessons, reviews, documentation, search, and
AI assistance closed during the attempt.

## Attempt Rules

1. Answer all eight questions in one uninterrupted sitting.
2. Select one answer unless the question says **Choose TWO** or **Choose
   THREE**.
3. Multiple-response questions receive credit only for an exact match: every
   required option and no additional option.
4. Record uncertain question numbers only after choosing all answers.
5. Enter start and end times, then change `Submission status` to `FROZEN`.
6. Do not revise a frozen response before marking.

---

## Questions

### 1. Workforce access control plane

A company has 60 AWS accounts in AWS Organizations. Employees authenticate
through an external corporate identity provider. The security team must assign
developers, auditors, and administrators different access levels to selected
accounts through group membership. Users must receive temporary credentials,
and the company does not want to create IAM users or manually maintain a
federation role in every account.

Which solution meets these requirements with the LEAST operational overhead?

A. Create an Amazon Cognito user pool, place employees in user-pool groups, and
map those groups to roles in every AWS account.<br>
B. Use an organization instance of IAM Identity Center, connect the corporate
identity source, define permission sets, and assign groups to AWS accounts.<br>
C. Create IAM users in the Organizations management account and allow them to
assume administrator roles in all member accounts.<br>
D. Use AWS Resource Access Manager to share IAM roles from a central security
account with the member accounts.

### 2. Workforce and application identities — Choose TWO

A company has two separate identity requirements:

- employees need centrally assigned access to AWS accounts according to their
  corporate directory groups; and
- customers signed in to a mobile application need temporary, limited AWS
  credentials to upload files to one S3 prefix.

Which TWO choices correctly map the identity services to these requirements?

A. Use IAM Identity Center permission sets and account assignments for the
employees.<br>
B. Use IAM users with long-lived access keys for the employees because
permission sets cannot result in IAM roles.<br>
C. Use IAM Identity Center permission sets to grant the mobile customers
credentials directly from the application.<br>
D. Use an Amazon Cognito identity pool with appropriately scoped IAM roles for
the mobile customers.<br>
E. Use an Amazon Cognito user pool as the multi-account AWS access-assignment
control plane for the employees.

### 3. Migration programme visibility

A migration team has registered discovery collectors and migration tools with
AWS Migration Hub. The configured home Region is `eu-west-1`. The team is
migrating applications into `eu-west-2` and `eu-central-1`. A programme manager
opens Migration Hub in `eu-central-1` and cannot find the previously collected
portfolio and migration status.

What should the programme manager do?

A. Use AWS DataSync to copy the Migration Hub discovery database from
`eu-west-1` to `eu-central-1`.<br>
B. Create an organization AWS Config aggregator in `eu-central-1` to import the
Migration Hub application groups.<br>
C. View and manage the migration portfolio from the configured Migration Hub
home Region, which can track migrations into multiple target Regions.<br>
D. Export the discovery data to S3 and use cross-Region replication to make
Migration Hub populate the new Region.

### 4. Discovery, transfer, and tracking — Choose THREE

An enterprise is planning migration waves for physical servers. It needs
detailed running-process and TCP connection data to identify dependencies. It
must then transfer changing files from an on-premises NFS server to Amazon EFS
and maintain a central view of application migration status across several
migration tools.

Which THREE actions map directly to these requirements?

A. Install AWS Application Discovery Agent on the supported physical servers
to collect host, process, and TCP connection evidence.<br>
B. Deploy only the Agentless Collector VMware vCenter inventory module to
collect detailed process and TCP data from the physical servers.<br>
C. Configure AWS DataSync locations and a task to transfer the NFS data to
Amazon EFS.<br>
D. Use AWS Application Migration Service solely as a managed NFS file-copy and
verification service.<br>
E. Use AWS Migration Hub in its home Region to group applications and track
migration status reported by integrated tools.<br>
F. Use AWS DataSync as the programme-level database for discovery inventory,
application groups, migration waves, and tool status.

### 5. DAX availability and request path — Choose TWO

A product catalogue uses DynamoDB as its system of record. The application
performs millions of repeated, eventually consistent reads and needs
microsecond latency. The cache must continue serving requests after one cache
node or Availability Zone fails. The company wants minimal changes to its
DynamoDB access patterns.

Which TWO actions meet the requirements?

A. Send strongly consistent reads through DAX and require DAX to serve them
from its cache.<br>
B. Deploy a multi-node DAX cluster with nodes distributed across Availability
Zones.<br>
C. Create the DAX cluster without changing the application because DynamoDB
automatically redirects SDK requests to DAX.<br>
D. Replace DynamoDB with a single Memcached node as the durable system of
record.<br>
E. Configure the application to use the DAX client and cluster endpoint for
supported DynamoDB operations.

### 6. DAX integration diagnosis

A team creates a healthy multi-node DAX cluster for a read-heavy DynamoDB
application. CloudWatch shows almost no DAX requests, and application latency
does not change. A code review shows that the application still creates a
standard DynamoDB client and sends `GetItem` requests directly to DynamoDB.

Which change most directly fixes the problem?

A. Increase the DynamoDB table's write capacity so DynamoDB can push requests
into DAX.<br>
B. Add a global secondary index and point the existing DynamoDB client at the
index ARN.<br>
C. Use the DAX client and cluster endpoint for the supported read operations.<br>
D. Create a Route 53 alias record that maps the DynamoDB regional endpoint to
the DAX cluster.

### 7. Private S3 access from on premises

An on-premises backup application connects to a VPC through Direct Connect. It
must access an S3 bucket by using private IP addresses. The company prohibits
NAT gateways and public S3 endpoints for this traffic.

Which solution is MOST appropriate?

A. Associate an S3 gateway endpoint with the VPC route table and propagate its
route through Direct Connect to the on-premises network.<br>
B. Add an internet gateway to the VPC and advertise its default route over the
Direct Connect private virtual interface.<br>
C. Create an S3 interface VPC endpoint that is reachable over the private
network path, with the required DNS and endpoint-policy configuration.<br>
D. Create an S3 Multi-Region Access Point because it automatically supplies an
on-premises private IP endpoint.

### 8. Mixed VPC and on-premises S3 clients — Choose TWO

Applications in private VPC subnets and applications in an on-premises data
centre both access S3 in the same Region. The data centre is connected through
Direct Connect. All traffic must remain private, and the company wants to avoid
unnecessary interface-endpoint charges for traffic originating inside the VPC.

Which TWO design choices best meet the requirements?

A. Use an S3 gateway endpoint associated with the workload route tables for
traffic originating from the VPC subnets.<br>
B. Extend the S3 gateway endpoint to the on-premises network by propagating its
route through a transit gateway and Direct Connect.<br>
C. Route both VPC and on-premises S3 traffic through a NAT gateway so that one
path serves all clients.<br>
D. Use an S3 interface endpoint and the appropriate private DNS/Resolver path
for traffic originating from the on-premises network.<br>
E. Remove the gateway endpoint and require all VPC workloads to use the
interface endpoint because interface endpoints have no hourly or data-processing
charges.

---

## Frozen Submission Template

```text
Start: 13:18
End:13:35
Uncertain: 4,7

1: B
2: AD
3: C
4: ACE
5: BE
6: C
7: C
8: AD

Submission Status: FROZEN
```
