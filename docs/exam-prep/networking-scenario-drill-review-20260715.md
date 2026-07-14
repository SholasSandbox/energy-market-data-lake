# SAP-C02 Networking Scenario Drill Review - 2026-07-15

<!-- markdownlint-disable MD013 -->

## Scope and Evidence Boundary

This source-backed review tests the decision rules in the repository's
Networking artifacts. It closes the documentation-review gate for the Transit
Gateway, PrivateLink/peering/TGW, Direct Connect/VPN, Route 53 Resolver, and NAT
Gateway deliverables.

The answer key is evidence that the scenarios were reviewed against the
documented service boundaries. It is **not** a learner score, timed-practice
result, confidence increase, or completed wrong-answer review cycle. Complete
the questions without the answer key in a later session before recording recall
evidence.

## Blind Questions

### 1. Stateful versus stateless return traffic

An application accepts HTTPS from an approved corporate CIDR. Its security
group permits inbound TCP 443, and its subnet network ACL permits inbound TCP
443. The connection still fails because response traffic is blocked at the
subnet boundary. What is the most likely missing control?

- A. An inbound ephemeral-port rule in the security group
- B. An outbound ephemeral-port rule in the network ACL
- C. A second security group attached to the route table
- D. A Transit Gateway route for the corporate CIDR

### 2. Private S3 access

Private-subnet workloads transfer large volumes to S3 in the same Region. They
need no on-premises, peered-VPC, or Transit Gateway access to the endpoint. What
is the least-cost private access pattern?

- A. NAT Gateway
- B. S3 interface endpoint
- C. S3 gateway endpoint
- D. Internet Gateway with public IP addresses

### 3. Narrow cross-account service exposure

Many consumer VPCs in different accounts need private access to one provider
API. Consumers must not receive broad IP reachability into the provider VPC.
Which pattern best fits?

- A. Full-mesh VPC peering
- B. Transit Gateway with all routes propagated
- C. AWS PrivateLink endpoint service and consumer endpoints
- D. NAT Gateway in the provider VPC

### 4. Two-VPC bilateral connectivity

Two VPCs have non-overlapping CIDRs and need direct private IP communication.
No transit, shared inspection, or hybrid routing is required. Which option is
the smallest fit?

- A. VPC peering
- B. Transit Gateway
- C. PrivateLink
- D. Direct Connect gateway

### 5. Multi-VPC hybrid hub

Dozens of VPCs require segmented access to shared services, centralized
inspection, and an on-premises network. Which service is the routing hub?

- A. VPC peering
- B. AWS Transit Gateway
- C. Route 53 Resolver
- D. Gateway VPC endpoint

### 6. Stateful centralized inspection

Traffic routed through a stateful inspection VPC returns through a different
Availability Zone and bypasses the original appliance flow. Which design
concern must be corrected first?

- A. PrivateLink endpoint acceptance
- B. Transit Gateway appliance mode and symmetric route design
- C. S3 endpoint policy
- D. Direct Connect public virtual interface

### 7. Fast hybrid connectivity

A company needs encrypted hybrid connectivity within days for a short-lived
transition and can tolerate internet-path variability. Which option should be
implemented first?

- A. Site-to-Site VPN
- B. Direct Connect only
- C. VPC peering
- D. Route 53 Resolver only

### 8. Consistent and encrypted hybrid path

A production hybrid workload requires more consistent bandwidth and latency
than internet VPN alone, while retaining an encrypted IPsec path. Which pattern
best meets both requirements?

- A. Direct Connect alone, because it always provides IPsec
- B. Direct Connect plus Site-to-Site VPN
- C. Transit Gateway without a transport attachment
- D. Internet Gateway plus network ACLs

### 9. AWS resolves on-premises names

AWS workloads must resolve records in `corp.example.internal`, whose
authoritative DNS servers remain on-premises. Which DNS components are needed?

- A. Route 53 Resolver outbound endpoint and a forwarding rule
- B. Route 53 Resolver inbound endpoint only
- C. AWS Config aggregator
- D. Transit Gateway route table only

### 10. On-premises resolves AWS private names

On-premises clients must resolve an AWS private hosted-zone suffix. Which DNS
flow is correct?

- A. On-premises conditional forwarder to a Route 53 Resolver inbound endpoint
- B. Resolver outbound endpoint to the on-premises DNS server
- C. Public hosted zone through a NAT Gateway
- D. Direct Connect without DNS forwarding

### 11. NAT cost reduction

Cost evidence shows most NAT-processed bytes are same-Region S3 and DynamoDB
traffic. What is the first remediation to assess?

- A. Add more NAT Gateways in every subnet
- B. Route eligible traffic through S3 and DynamoDB gateway endpoints
- C. Replace NAT Gateway with Transit Gateway
- D. Add public IP addresses to every workload

### 12. Interface endpoint versus NAT cost

A design proposes several low-volume interface endpoints across three
Availability Zones. Which cost conclusion is valid before deployment?

- A. Interface endpoints are always cheaper than NAT Gateway
- B. NAT Gateway is always cheaper than interface endpoints
- C. Compare endpoint-AZ hours and endpoint data processing with NAT hours,
  processed bytes, and applicable transfer charges
- D. Private connectivity has no hourly cost

## Answer Key and Review

| Question | Answer | Decision rule | Main trap |
|---:|:---:|---|---|
| 1 | B | Network ACLs are stateless, so permitted inbound traffic needs an applicable outbound return rule; security groups are stateful. | Adding a security-group return rule when the subnet boundary is blocking the response. |
| 2 | C | S3 gateway endpoints have no additional endpoint charge and avoid NAT for eligible same-VPC access. | Selecting an interface endpoint merely because it is private. |
| 3 | C | PrivateLink exposes a bounded service without broad provider-VPC routing. | Solving one-service access with full network connectivity. |
| 4 | A | Peering is the smallest general IP-routing fit for two non-overlapping VPCs with no transit requirement. | Choosing Transit Gateway before scale or transit exists. |
| 5 | B | Transit Gateway is the regional hub for multi-VPC and hybrid attachments with route-domain control. | Treating peering as transitive or Route 53 Resolver as a router. |
| 6 | B | Stateful inspection needs symmetric flow handling; appliance mode and deliberate forward/return routes preserve the path. | Adding services that do not address asymmetric routing. |
| 7 | A | Site-to-Site VPN provides encrypted IPsec transport and is normally faster to establish than dedicated connectivity. | Choosing the longer-lead dedicated option despite the stated time constraint. |
| 8 | B | Direct Connect supplies the more consistent underlay; VPN supplies the designed IPsec overlay or backup path. | Assuming Direct Connect alone automatically supplies IPsec. |
| 9 | A | Outbound means AWS asks an external/on-premises resolver; the suffix forwarding rule selects that path. | Reversing inbound and outbound endpoint direction. |
| 10 | A | Inbound means an external/on-premises resolver asks the VPC Resolver about AWS private names. | Believing transport alone configures DNS forwarding. |
| 11 | B | Move eligible S3/DynamoDB traffic to no-additional-charge gateway endpoints before paying NAT processing for it. | Scaling the expensive path instead of removing eligible traffic. |
| 12 | C | Interface endpoint cost multiplies by service and selected AZ; NAT cost depends on gateway-hours, processed bytes, and routing. | Treating either service as universally cheaper. |

## Cross-Scenario Decision Map

| Requirement category | First-choice pattern |
|---|---|
| One private service | PrivateLink/interface endpoint, or gateway endpoint for eligible S3/DynamoDB access |
| Two VPCs, direct relationship | VPC peering |
| Many VPCs or VPCs plus on-premises | Transit Gateway |
| Fast encrypted hybrid transport | Site-to-Site VPN |
| More consistent hybrid transport plus IPsec | Direct Connect plus VPN |
| AWS queries on-premises DNS | Resolver outbound endpoint plus forwarding rule |
| On-premises queries AWS private DNS | Conditional forwarder plus Resolver inbound endpoint |
| General unsupported outbound IPv4 destinations | NAT Gateway only after the endpoint and cost gates |

## Review Outcome

All twelve scenarios map each selected service to an explicit functional,
routing, DNS, resilience, or cost requirement. The review found no conflict
among the current Networking artifacts.

The remaining matrix gaps are not these reviewed decisions: a dedicated
VPC/subnet/route-table diagram and a VPC endpoint diagram or bounded lab remain
open. Learner recall also remains unscored.

## References

- Security groups: `https://docs.aws.amazon.com/vpc/latest/userguide/vpc-security-groups.html`
- Network ACLs: `https://docs.aws.amazon.com/vpc/latest/userguide/vpc-network-acls.html`
- VPC peering: `https://docs.aws.amazon.com/vpc/latest/peering/vpc-peering-basics.html`
- AWS Transit Gateway: `https://docs.aws.amazon.com/vpc/latest/tgw/what-is-transit-gateway.html`
- Transit Gateway appliance mode: `https://docs.aws.amazon.com/vpc/latest/tgw/transit-gateway-appliance-scenario.html`
- AWS PrivateLink: `https://docs.aws.amazon.com/vpc/latest/privatelink/what-is-privatelink.html`
- Site-to-Site VPN: `https://docs.aws.amazon.com/vpn/latest/s2svpn/VPC_VPN.html`
- Route 53 VPC Resolver: `https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/resolver.html`
- NAT Gateway pricing guidance: `https://docs.aws.amazon.com/vpc/latest/userguide/nat-gateway-pricing.html`
- Gateway endpoints: `https://docs.aws.amazon.com/vpc/latest/privatelink/gateway-endpoints.html`
- AWS PrivateLink pricing: `https://aws.amazon.com/privatelink/pricing/`
