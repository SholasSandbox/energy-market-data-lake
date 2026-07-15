# Networking Scenario Drill — Blind Attempt — 2026-07-15

<!-- markdownlint-disable MD013 -->

## Purpose and Boundary

This is a blind SAP-C02 Networking recall attempt for the tracker-ordered
follow-up to the completed VPC connectivity comparison matrix. It is
documentation-only and does not authorize any AWS action.

Before submission, the learner was instructed not to consult
`docs/exam-prep/networking-scenario-drill-review-20260715.md`, the networking
planning notes, diagrams, external documentation, or an answer key. The
submitted choices below were then reviewed against the source-backed material.

Submit the choices as `1B, 2A, ...`. A score or learner-recall update may be
recorded only after the learner explicitly submits this blind attempt.

## Questions

### 1. Same-Region S3 access

Private workloads in one VPC need same-Region Amazon S3 access with the lowest
additional endpoint cost. Choose the best design.

- A. NAT Gateway
- B. S3 gateway endpoint
- C. S3 interface endpoint
- D. Transit Gateway

### 2. Hybrid DynamoDB access

An on-premises application connected over Direct Connect must privately access
Amazon DynamoDB. Choose the best design.

- A. DynamoDB gateway endpoint
- B. DynamoDB interface endpoint, plus the required transport and
  endpoint-specific DNS approach
- C. Internet Gateway
- D. VPC peering only

### 3. Interface endpoint network control

Which control applies directly to an interface endpoint's elastic network
interfaces (ENIs)?

- A. Endpoint security group
- B. Gateway endpoint route-table association
- C. NAT Gateway Elastic IP address
- D. Transit Gateway route propagation

### 4. Gateway route selection

A gateway endpoint is associated with a private subnet's route table and the
table also has `0.0.0.0/0` routed to a NAT Gateway. What routes same-Region S3
traffic?

- A. NAT Gateway, because it is the default route
- B. Internet Gateway
- C. Gateway endpoint prefix-list route
- D. Whichever path has lower cost

### 5. Endpoint-policy boundary

Which statement best describes an endpoint policy?

- A. It replaces IAM and S3 bucket or DynamoDB table policies.
- B. It is an additional guardrail on endpoint use; IAM and resource policies
  still apply.
- C. It is a security group for gateway endpoints.
- D. It enables transitive routing.

### 6. Unsupported public IPv4 destination

A private workload needs outbound IPv4 access to a third-party public API with
no supported PrivateLink endpoint. Choose the best design.

- A. S3 gateway endpoint
- B. NAT Gateway, with Availability Zone, resilience, and cost review
- C. DynamoDB interface endpoint
- D. Route 53 Resolver inbound endpoint

### 7. S3 dual-endpoint design

For S3, why might a design retain both gateway and interface endpoints?

- A. Gateway endpoints provide on-premises access.
- B. In-VPC traffic can use the unbilled gateway endpoint while hybrid callers
  use the interface endpoint.
- C. Interface endpoints require no DNS decision.
- D. Gateway endpoints provide private ENIs.

### 8. Interface endpoint cost model

Which statement is true?

- A. Interface endpoints create broad VPC-to-VPC routing.
- B. Gateway endpoints serve all AWS APIs.
- C. Interface endpoint costs scale with endpoint type, selected Availability
  Zone hours, and processed data.
- D. NAT Gateway cost disappears whenever any endpoint exists.

## Answer Submission

| Question | Learner choice |
|---:|---|
| 1 |  B|
| 2 |  B|
| 3 |  A|
| 4 |  C|
| 5 |  B|
| 6 |  B|
| 7 |  B|
| 8 |  C|

## Attempt Status

Completed and explicitly accepted as the learner submission on 2026-07-15.

- Mode: untimed blind recall, eight focused questions.
- Submitted choices: `1B, 2B, 3A, 4C, 5B, 6B, 7B, 8C`.
- Score: **8/8 (100%)**.
- Outcome: clean pass; no wrong-answer entry or remediation is required.
- Evidence boundary: supports focused VPC endpoint and NAT Gateway recall; it
  is not a timed exam, full Networking-domain assessment, wrong-answer review
  cycle, or booking criterion by itself.
