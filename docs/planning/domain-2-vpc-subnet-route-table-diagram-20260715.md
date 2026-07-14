# Domain 2 VPC, Subnet, and Route-Table Diagram - 2026-07-15

<!-- markdownlint-disable MD013 -->

## Scope

This documentation-only study artifact closes the tracker evidence gap for a
dedicated VPC/subnet/route-table diagram. The companion Mermaid source is
`diagrams/vpc-subnet-route-table-study.mmd`.

The diagram is a decision model, not the current Lakehouse topology. It does
not authorize a VPC, subnet, route table, internet gateway, NAT Gateway, VPC
endpoint, security-group, network-ACL, or workload change in AWS.

## Reading the Diagram

The example VPC spans two Availability Zones. Each subnet is confined to one
Availability Zone and is explicitly associated with a route table:

- public subnets use a public route table with the VPC-local route and a
  default IPv4 route to an internet gateway;
- private application subnets use zonal private route tables with the
  VPC-local route, an S3 gateway-endpoint route, and an optional default route
  to a NAT Gateway in the same Availability Zone; and
- the S3 gateway endpoint is associated with the private route tables and is
  preferred for eligible S3 traffic instead of NAT.

The NAT routes are deliberately conditional. The accepted Lakehouse posture is
endpoint-first, and NAT is justified only for required outbound IPv4
destinations that a narrower option cannot serve.

## Route-Table Model

| Route table | Destination | Target | Meaning |
|---|---|---|---|
| Public | VPC CIDR | `local` | Communication within the VPC, subject to security controls |
| Public | `0.0.0.0/0` | Internet gateway | Public IPv4 path; a resource still needs suitable addressing and security rules |
| Private AZ A | VPC CIDR | `local` | Intra-VPC routing from the AZ A private subnet |
| Private AZ A | S3 prefix list | S3 gateway endpoint | Private S3 service access without NAT |
| Private AZ A | `0.0.0.0/0` | NAT Gateway AZ A, if justified | Outbound IPv4 path without a direct internet-gateway route |
| Private AZ B | VPC CIDR | `local` | Intra-VPC routing from the AZ B private subnet |
| Private AZ B | S3 prefix list | S3 gateway endpoint | Private S3 service access without NAT |
| Private AZ B | `0.0.0.0/0` | NAT Gateway AZ B, if justified | Zonal outbound IPv4 path without cross-AZ dependency |

## Decision Rules and Traps

| Scenario signal | Correct reasoning | Common trap |
|---|---|---|
| A subnet has a direct default route to an internet gateway. | It is a public subnet by routing definition. | Assuming every resource is publicly reachable without public addressing and security permission. |
| A subnet has no direct internet-gateway route. | It is not public; optional NAT can provide outbound IPv4 access. | Calling a private subnet public merely because it can reach the internet through NAT. |
| A route table contains several matching destinations. | The most specific matching route selects the path. | Assuming the default route wins over a more specific service-prefix route. |
| S3 traffic matches an S3 gateway-endpoint route. | Use the endpoint path rather than NAT for eligible traffic. | Paying NAT processing for traffic that a gateway endpoint can carry. |
| A packet has a valid route. | Continue checking security groups, network ACLs, endpoint policies, and the return path. | Treating a route as authorization. |
| Multi-AZ private egress is required. | Keep the NAT path zonal when the cost and resilience decision justifies NAT. | Routing both AZs through one NAT Gateway without acknowledging cross-AZ cost and dependency. |

## Lakehouse Posture

This diagram does not assert that the current Lakehouse uses this topology.
For a future private workload VPC, the decision order remains:

1. create only the subnets and routes required by the workload boundary;
2. use gateway endpoints for eligible S3/DynamoDB access;
3. add interface endpoints selectively for supported private service access;
4. add NAT only for evidenced unsupported outbound IPv4 destinations; and
5. require separate approval, cost comparison, rollback, and validation before
   any live network implementation.

## Acceptance Criteria Met by This Slice

- depicts subnets across two Availability Zones;
- maps each subnet class to its route table;
- distinguishes local, internet-gateway, gateway-endpoint, and optional NAT
  routes;
- records the endpoint-first cost posture and zonal NAT warning; and
- separates routing decisions from traffic authorization.

The remaining connectivity-matrix evidence gap is a VPC endpoint diagram or
bounded lab. Learner recall remains separately unscored.

## References

- VPC route tables:
  `https://docs.aws.amazon.com/vpc/latest/userguide/VPC_Route_Tables.html`
- VPC subnets and routing classifications:
  `https://docs.aws.amazon.com/vpc/latest/userguide/configure-subnets.html`
- Gateway endpoints:
  `https://docs.aws.amazon.com/vpc/latest/privatelink/gateway-endpoints.html`
- NAT Gateway pricing guidance:
  `https://docs.aws.amazon.com/vpc/latest/userguide/nat-gateway-pricing.html`
