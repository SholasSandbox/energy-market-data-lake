# Domain 2 PrivateLink, VPC Peering, and Transit Gateway Decision - 2026-07-14

<!-- markdownlint-disable MD013 -->

## Scope and Decision Boundary

This documentation-only study artifact advances two tracker deliverables:

1. the PrivateLink versus VPC peering versus Transit Gateway decision table; and
2. the Transit Gateway hub-and-spoke diagram in
   `diagrams/tgw-hub-and-spoke-study.mmd`.

It improves SAP-C02 network-design reasoning. It does **not** authorize a VPC,
Transit Gateway, endpoint service, VPC endpoint, route table, firewall, or
cross-account AWS change.

## Decision Table

| Requirement signal | Prefer | Why it wins | Reject the alternatives when | Design checks |
|---|---|---|---|---|
| Consumers need private access to one provider-owned service, API, database, or appliance; they do not need reachability to the provider VPC CIDR. | AWS PrivateLink | It exposes a deliberately narrow service boundary through VPC endpoints without creating broad layer-3 trust. | Do not use peering or Transit Gateway merely to consume one service. | Define the service owner/consumer boundary, endpoint policy or security-group controls as applicable, private DNS behavior, Availability Zone coverage, and per-endpoint cost. |
| Two VPCs with non-overlapping CIDRs need direct, bilateral private IP connectivity and the relationship will remain small. | VPC peering | It is the least-complex general network path for one explicit VPC-to-VPC relationship. | Do not use it for overlapping CIDRs, transitive routing, shared hybrid transport, centralized inspection, or a growing mesh. | Add routes on both sides, set security controls, and decide whether private DNS resolution is required. |
| Several VPCs, shared services, inspection, and/or on-premises networks need governed hub-and-spoke transit. | AWS Transit Gateway | It centralizes attachment and route-domain management instead of maintaining many one-to-one peerings. | Do not use it for a single simple bilateral connection or a one-service consumption use case. | Deliberately associate and propagate attachments, separate route domains, account for attachment/data-processing cost, and design symmetric inspection routing. |

### Non-Substitutable Properties

| Property | PrivateLink | VPC peering | Transit Gateway |
|---|---|---|---|
| Broad VPC-to-VPC IP routing | No | Yes, only between the two peers | Yes, subject to attachment and route-table policy |
| Transitive routing | No | No | Yes, when route tables allow it |
| Overlapping CIDRs | Can isolate service consumption from network overlap | No | Requires deliberate design; do not assume overlap is transparently solved |
| Provider-to-consumer exposure | Specific service only | Entire routed prefixes subject to controls | Entire routed prefixes subject to controls |
| Best scaling model | Many consumers of a bounded service | Small number of explicit bilateral links | Multi-VPC and hybrid hub-and-spoke |
| Typical SAP-C02 wrong answer | Selecting it when broad private IP routing is required | Treating it as a transit hub | Selecting it when the requirement is only one service or two small VPCs |

## Transit Gateway Hub-and-Spoke Reading Guide

The companion diagram has three intentional route domains:

- **workload route domain:** permits approved workload-to-shared-service paths;
- **inspection route domain:** steers selected egress or east-west flows through
  the inspection VPC and must preserve symmetric return routing;
- **hybrid route domain:** limits on-premises prefixes to explicitly approved
  attachments and is separate from the DNS decision.

The Transit Gateway is the transit control plane; VPC route tables still need
routes to the applicable attachment. Transit Gateway does not, by itself,
replace security groups, network ACLs, firewall policy, or Route 53 Resolver.

## Lakehouse Posture and Promotion Triggers

The current Energy Data Lakehouse baseline does not need a Transit Gateway or
VPC peering. The least-broad future option would be a VPC endpoint for a named
private AWS-service access need. Revisit this decision only when one of these
triggers is evidenced:

- two or more workload VPCs require controlled private IP connectivity;
- a shared service or centralized inspection boundary is introduced;
- a real hybrid transport requirement exists; or
- a provider service must be shared privately with consumers without exposing
  its whole VPC.

## Acceptance Criteria Met by This Slice

- distinguishes service consumption, direct bilateral routing, and hub-and-spoke transit;
- records non-transitivity and CIDR constraints for peering;
- records Transit Gateway route-domain, inspection-symmetry, and cost checks;
- supplies a conceptual hub-and-spoke diagram; and
- preserves the no-AWS-implementation boundary.

The next exercise should test these choices against scenario wording before the
deliverables are treated as fully complete.

## References

- AWS PrivateLink overview: `https://docs.aws.amazon.com/vpc/latest/privatelink/what-is-privatelink.html`
- VPC peering fundamentals and limitations: `https://docs.aws.amazon.com/vpc/latest/peering/vpc-peering-basics.html`
- AWS Transit Gateway concepts: `https://docs.aws.amazon.com/vpc/latest/tgw/what-is-transit-gateway.html`
