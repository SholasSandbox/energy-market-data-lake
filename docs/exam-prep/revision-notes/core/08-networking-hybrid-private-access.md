# 08 - Networking, Hybrid Connectivity, and Private Access

**Last revised:** 2026-07-28

Networking questions in SAP-C02 are usually about scale, isolation, transitivity, routing, hybrid connectivity, and cost.

## Core chooser

| Requirement | Service/pattern |
|---|---|
| Isolated network boundary | VPC |
| Private subnet outbound internet | NAT Gateway |
| Private access to AWS services | VPC endpoints |
| Many VPCs/accounts hub connectivity | Transit Gateway |
| Simple VPC-to-VPC private link | VPC peering |
| Private service exposure to consumers | PrivateLink |
| Dedicated private hybrid connectivity | Direct Connect |
| Encrypted tunnel over internet | Site-to-Site VPN |
| Client remote access VPN | Client VPN |
| Central network firewalling | AWS Network Firewall / GWLB appliance pattern |
| DNS inside VPC | Route 53 Resolver/private hosted zones |

## VPC basics

Important objects:

- CIDR block
- Subnets
- Route tables
- Internet Gateway
- NAT Gateway
- Security Groups
- Network ACLs
- VPC endpoints
- DHCP options
- Route 53 Resolver

## Public vs private subnet

| Subnet type | Required route |
|---|---|
| Public subnet | Route to Internet Gateway and public IP on resource/load balancer |
| Private subnet with outbound internet | Route to NAT Gateway |
| Isolated private subnet | No internet route; use VPC endpoints/private connectivity |

Trap: A subnet is public because of routing, not because of its name.

## Security Groups vs NACLs

| Control | Type | State | Use |
|---|---|---|---|
| Security Group | Instance/ENI-level | Stateful | Primary workload firewall |
| Network ACL | Subnet-level | Stateless | Broad subnet guardrail, explicit deny |

Trap: Security Groups do not support explicit deny; NACLs do.

## VPC endpoints

| Endpoint type | Use |
|---|---|
| Gateway endpoint | S3 and DynamoDB private access via route table |
| Interface endpoint | PrivateLink-powered ENI endpoint for many AWS services |
| Gateway Load Balancer endpoint | Appliance insertion pattern |

Benefits:

- keep traffic off public internet
- reduce NAT Gateway dependency/cost for AWS API traffic
- improve security posture with endpoint policies
- enable private subnet access to AWS services

Trap: Endpoint policy, IAM policy, resource policy, and KMS key policy may all need alignment.

## NAT Gateway cost trap

NAT Gateway has hourly and data processing costs. Architectures with many private workloads pulling large data from S3/ECR/CloudWatch through NAT can become expensive.

Mitigation:

- S3 Gateway endpoint
- DynamoDB Gateway endpoint
- ECR API/Docker interface endpoints
- CloudWatch Logs interface endpoint
- Secrets Manager/SSM/KMS interface endpoints where needed
- regional workload placement and routing review

## VPC peering

Choose peering when:

- small number of VPCs need direct private connectivity
- no transitive routing is required
- CIDRs do not overlap

Avoid peering when:

- many VPCs/accounts create mesh complexity
- transitive routing is required
- centralized inspection/hub architecture is needed

## Transit Gateway

Choose Transit Gateway when:

- many VPCs/accounts need hub-and-spoke routing
- hybrid connectivity should be centralized
- route tables must segment environments
- Direct Connect/VPN should attach to a central hub
- network team needs centralized routing control

Trap: Transit Gateway adds cost and route complexity. For two VPCs, peering may be simpler.

## PrivateLink

Choose PrivateLink when:

- provider exposes a private service to consumer VPCs/accounts
- consumer should not access provider VPC CIDR broadly
- overlapping CIDRs exist
- SaaS/private service model is needed
- NLB-backed endpoint service pattern fits

Trap: PrivateLink is not transitive network routing. It exposes a service endpoint, not full VPC connectivity.

## Direct Connect and VPN

| Requirement | Choice |
|---|---|
| Consistent bandwidth/private connectivity | Direct Connect |
| Encrypted tunnel quickly over internet | Site-to-Site VPN |
| Backup path for Direct Connect | VPN |
| Multiple VPC access from DX | Direct Connect Gateway + Transit Gateway/VIF design |
| Remote users to VPC | Client VPN |

Trap: Direct Connect is private but not encrypted by default. Use MACsec where supported or VPN over DX if encryption requirement exists.

### Direct Connect BGP route choice

AWS evaluates the destination prefix first. A more-specific prefix wins by longest-prefix match before local preference, AS-path length, or MED is considered. Advertise a more-specific prefix over the preferred Direct Connect BGP session when only that address range should favor the path.

Trap: AS-path prepending influences equally specific BGP routes; it cannot defeat a competing longer prefix.

## Route 53 Resolver

Use for hybrid DNS:

- inbound Resolver endpoints allow on-prem DNS to resolve AWS private zones
- outbound Resolver endpoints allow VPC resources to query on-prem DNS
- Resolver rules forward specific domains

Trap: DNS resolution must be designed explicitly in hybrid architectures. Network connectivity alone does not solve name resolution.

## Centralized inspection

Common patterns:

```text
Spoke VPCs
  -> Transit Gateway
  -> Inspection VPC
  -> AWS Network Firewall / GWLB appliances
  -> egress/on-prem/internet
```

Watch for:

- asymmetric routing
- route table segmentation
- appliance scaling
- fail-open/fail-closed requirements
- logging and compliance

## Exam traps

| Trap | Correction |
|---|---|
| “VPC peering is transitive” | It is not. |
| “Private subnet means no outbound access” | It can have outbound via NAT or endpoints. |
| “NAT Gateway is needed for S3 access” | Gateway endpoints can provide private S3 access. |
| “Direct Connect encrypts traffic automatically” | It is private connectivity, not automatically encrypted. |
| “PrivateLink connects whole VPCs” | It exposes services through endpoints. |
| “Security Group blocks are stateless” | Security Groups are stateful. |
| “NACLs are only allow rules” | NACLs support allow and deny and are stateless. |
