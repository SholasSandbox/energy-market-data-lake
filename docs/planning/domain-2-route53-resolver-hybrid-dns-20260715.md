# Domain 2 Route 53 Resolver Hybrid DNS Diagram - 2026-07-15

<!-- markdownlint-disable MD013 -->

## Scope

This documentation-only study artifact advances the tracker-required Route 53
Resolver hybrid DNS diagram. The companion Mermaid source is
`diagrams/route53-resolver-hybrid-dns-study.mmd`.

It does not authorize Resolver endpoints, Resolver rules, private hosted zones,
Direct Connect, Site-to-Site VPN, security-group, route-table, or DNS changes
in AWS.

## Reading the Diagram

### AWS workload resolves an on-premises private name

1. A workload sends the query to the VPC Resolver at the VPC-provided DNS
   address.
2. A Resolver forwarding rule for the on-premises suffix selects an outbound
   Resolver endpoint.
3. The outbound endpoint sends the query across an existing private transport
   path, such as Site-to-Site VPN or Direct Connect, to the on-premises DNS
   resolver.
4. The answer returns over the reverse DNS path.

### On-premises client resolves an AWS private name

1. The client sends the AWS-private-domain query to its on-premises DNS
   resolver.
2. An on-premises conditional forwarder sends that suffix to an inbound
   Resolver endpoint over existing private transport.
3. The inbound endpoint passes the query to the VPC Resolver, which answers
   from the applicable private hosted zone or VPC DNS namespace.
4. The answer returns over the reverse DNS path.

## Decision Rules and Traps

| Requirement | Correct first decision | Do not substitute |
|---|---|---|
| AWS workloads must resolve an on-premises private suffix. | Outbound Resolver endpoint plus a Resolver forwarding rule. | Transit Gateway, Direct Connect, VPN, or AWS Config alone; these do not create DNS forwarding. |
| On-premises workloads must resolve AWS private hosted-zone names. | Inbound Resolver endpoint plus an on-premises conditional forwarder. | A public DNS record or a transport-only design. |
| Both directions are required. | Design the two directional flows independently; each has its own forwarding and network-permission path. | Assuming one endpoint automatically provides bidirectional forwarding. |
| The requirement is only network transport. | Choose VPN, Direct Connect, or Transit Gateway based on the transport topology. | Adding Resolver endpoints without a DNS namespace requirement. |

## Design Checks

- Use private, non-overlapping network ranges and permit DNS traffic on the
  applicable path; transport reachability remains a separate prerequisite.
- Define the exact suffixes to forward and avoid broad forwarding rules that
  create unexpected DNS dependencies.
- Design endpoint availability, capacity, monitoring, and failure handling
  before treating hybrid DNS as production-ready.
- Keep private hosted-zone association, Resolver-rule association, and
  on-premises forwarding ownership explicit.

## Lakehouse Posture

The current Energy Data Lakehouse has no evidenced hybrid private-DNS need.
This diagram is an exam and future-architecture decision aid, not a promotion
plan. Revisit it only if a real on-premises/private-domain dependency is
introduced.

## Acceptance Criteria Met by This Slice

- depicts inbound and outbound Resolver endpoint flows separately;
- separates DNS forwarding from VPN, Direct Connect, and Transit Gateway
  transport decisions;
- records forwarding ownership and design checks; and
- preserves the no-AWS-implementation boundary.

Scenario-drill review remains open before the tracker deliverable is treated as
fully complete.

## Reference

- Route 53 VPC Resolver hybrid DNS documentation:
  `https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/resolver.html`
