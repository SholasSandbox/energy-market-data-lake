# Domain 2 Centralized Inspection VPC Architecture - 2026-07-15

<!-- markdownlint-disable MD013 -->

## Scope and Evidence Boundary

This source-backed reading guide closes the tracker-required centralized-inspection VPC architecture sketch. The companion Mermaid source is `diagrams/centralized-inspection-vpc-study.mmd`.

The artifact supports SAP-C02 multi-VPC segmentation, Transit Gateway route-domain, stateful-firewall, and troubleshooting decisions. It does not authorize a VPC, Transit Gateway, attachment, route table, AWS Network Firewall, Gateway Load Balancer, NAT Gateway, VPN, Direct Connect, logging, or other AWS change.

## Winning Pattern

When many VPC or hybrid attachments must use one governed stateful inspection point, use a dedicated inspection VPC attached to AWS Transit Gateway. Separate the attachments that have **not yet been inspected** from the inspection attachment that sends traffic onward **after inspection**.

The central rule is:

> Forward and return traffic for a stateful flow must traverse the same firewall endpoint in the same Availability Zone.

Transit Gateway appliance mode on the inspection VPC attachment provides flow stickiness across the attachment. Transit Gateway route tables and the inspection VPC's subnet route tables must still steer both directions through the firewall; appliance mode does not repair a bypass route.

## Reading the Diagram

### Forward East-West or Hybrid Flow

1. Workload VPC A sends an approved remote prefix to its Transit Gateway attachment.
2. The source attachment is associated with the **spoke/uninspected Transit Gateway route table**.
3. That route table sends the selected destination prefix to the inspection VPC attachment rather than directly to VPC B or the hybrid attachment.
4. Inspection VPC attachment-subnet routes steer the packet through the firewall endpoint selected for the flow.
5. After inspection, the inspection VPC returns the packet to the same Transit Gateway attachment.
6. The inspection attachment is associated with the **inspection/inspected Transit Gateway route table**.
7. That route table sends the approved destination prefix to VPC B, VPN, or Direct Connect gateway attachment.

### Return Flow

The destination route points back to Transit Gateway. Its attachment enters the spoke/uninspected route domain, which again steers the flow to the inspection attachment. Appliance mode and Availability Zone–aligned VPC routes keep the response on the stateful endpoint that saw the forward flow. The inspection/inspected route table then returns the response to the original source attachment.

Exam trap: drawing a firewall between two boxes is not enough. Both Transit Gateway passes, both VPC route directions, and endpoint symmetry must be evidenced.

## Route-Domain Responsibilities

| Component | Association | Routes it should learn or contain | Boundary it enforces |
|---|---|---|---|
| Spoke/uninspected TGW route table | Workload, shared-service, egress-edge, or approved hybrid source attachments | Static or deliberately controlled routes for inspected destinations pointing to the inspection attachment | Prevents a source attachment from reaching another protected attachment directly. |
| Inspection/inspected TGW route table | Inspection VPC attachment | Approved workload, shared-service, egress, and on-premises prefixes pointing to their real destination attachments | Lets traffic leave the inspection VPC only after the firewall pass. |
| Workload VPC route table | Workload subnets | Remote protected prefixes, or a justified default route, pointing to Transit Gateway | Sends selected flows into the central routing domain. |
| Inspection attachment-subnet route table | Transit Gateway attachment subnets in the inspection VPC | Protected destinations pointing to the zonally appropriate firewall endpoint | Forces the first VPC-side pass through inspection. |
| Firewall-subnet route table | Dedicated firewall endpoint subnets | Protected destinations or return prefixes pointing back to Transit Gateway or the approved next hop | Returns inspected traffic without bypassing the stateful endpoint. |

An attachment is associated with one Transit Gateway route table and can propagate routes to multiple route tables. Association selects the table used to route traffic arriving from that attachment; propagation controls which attachment prefixes a table learns. Do not enable broad default association and propagation when they would create a direct bypass.

## Stateful Symmetry and Appliance Mode

Stateful firewalls track both directions of a flow. Without a symmetric route, the response can reach another endpoint or Availability Zone whose state table did not see the original packet and can therefore drop it.

For the classic inspection-VPC pattern:

- enable appliance mode on the inspection VPC Transit Gateway attachment;
- provide attachment and firewall endpoint coverage in the intended Availability Zones;
- keep the forward and return route chains identical in policy and Availability Zone;
- enable the route propagation required for Transit Gateway's Availability Zone–aware appliance-mode behavior;
- use exactly one Transit Gateway connection to the appliance VPC when flow stickiness depends on one shared state domain; and
- test failure behavior rather than assuming that an endpoint or Availability Zone loss preserves every existing session.

AWS Network Firewall does not support asymmetric routing. Both directions must use the same firewall endpoint for stateful inspection to work correctly.

## Inspection Technology Choices

| Requirement | Preferred option | Key checks |
|---|---|---|
| AWS-managed stateful and stateless network inspection | AWS Network Firewall endpoints in dedicated inspection subnets | Firewall policy, rule order/action, logging, HOME_NET scope, endpoint per intended AZ, symmetric routing, and cost. |
| Existing or specialist third-party virtual appliances | Gateway Load Balancer with Gateway Load Balancer endpoints | Appliance scaling/health, GENEVE support, endpoint routes, symmetric flow handling, vendor licensing, and operational ownership. |
| Native firewall integration directly with Transit Gateway | Transit gateway-attached AWS Network Firewall where available and suitable | Same-Region/AZ prerequisites, route-table-only steering model, sharing/ownership boundary, always-enabled appliance mode, and regional feature availability. |

Do not select a centralized inspection VPC merely because centralization sounds more secure. Distributed firewalls can reduce routing complexity and cross-Availability Zone data transfer for isolated VPCs. The winning design depends on inspection scope, organizational ownership, failure domains, latency, throughput, cost, and the need for common policy.

## Traffic Classes

| Traffic class | Central inspection decision |
|---|---|
| East-west VPC-to-VPC | Steer both attachments through the uninspected and inspected route domains; suppress any direct bypass route. |
| VPC-to-on-premises | Route the workload and VPN/Direct Connect gateway attachments through the inspection attachment in both directions. DNS forwarding remains a separate Route 53 Resolver decision. |
| Centralized IPv4 egress | After inspection, send the flow to a deliberately designed egress VPC or zonal NAT/Internet Gateway path, and return it through the same inspection path. Do not assume NAT Gateway is part of every inspection design. |
| Internet ingress | Use a separately validated ingress architecture; asymmetric entry through an Internet Gateway and return through Transit Gateway can break stateful symmetry. |
| Private AWS-service access | Prefer gateway or interface VPC endpoints when the requirement is narrow service access; do not force all service traffic through a general inspection/NAT path without a stated policy need. |

## High-Value SAP-C02 Traps

1. **Transit Gateway is a router, not a firewall.** It steers attachments through route tables; the firewall evaluates traffic.
2. **Appliance mode is necessary but not sufficient.** Incorrect VPC or Transit Gateway routes can still bypass the firewall.
3. **Stateful inspection requires symmetry.** Both directions must traverse the same endpoint and Availability Zone for the flow.
4. **Association and propagation are different.** Association chooses the ingress lookup table; propagation teaches routes to selected tables.
5. **A single default TGW route table can defeat segmentation.** Use deliberate route domains when inspection is mandatory.
6. **Routes do not authorize traffic.** Security groups, network ACLs, firewall policy, and service policies remain separate.
7. **DNS is not packet inspection.** Route 53 Resolver forwards or resolves names; Transit Gateway and the firewall carry and inspect traffic.
8. **Centralized egress is not automatically cheapest.** Include Transit Gateway processing, firewall endpoint, cross-AZ, NAT, data-transfer, logging, and operations costs.
9. **High availability is zonal by design.** One firewall endpoint in one Availability Zone creates a failure and cross-AZ dependency.
10. **Ingress and egress paths are not interchangeable.** Validate the complete forward and return sequence for each traffic class.

## Failure and Validation Checklist

- Export or inspect Transit Gateway route tables and prove there is no direct spoke-to-spoke bypass.
- Confirm the inspection attachment is associated with the inspected route table and has appliance mode enabled.
- Confirm source attachments use the uninspected route table and that only intended prefixes are steered to inspection.
- Trace VPC route tables from the Transit Gateway attachment subnet to the firewall endpoint and back to Transit Gateway.
- Verify firewall endpoints exist and are healthy in every intended Availability Zone.
- Use VPC Flow Logs, Transit Gateway Flow Logs, AWS Network Firewall flow/alert logs, and CloudWatch metrics where applicable.
- Use Reachability Analyzer for supported static-path checks, while remembering that it does not prove application-layer or stateful policy behavior.
- Test forward and return flows, cross-Availability Zone behavior, endpoint failure, route changes, and rollback.
- Establish cost attribution for Transit Gateway, firewall processing, cross-AZ transfer, NAT, and logs before implementation.

## Lakehouse Posture

The current Energy Data Lakehouse has no evidenced multi-VPC or hybrid traffic volume that justifies a centralized inspection VPC. This artifact is exam and future-architecture evidence only. The current least-broad posture remains endpoint-first private service access and NAT-last IPv4 egress.

Revisit implementation only if multiple VPCs, centralized policy ownership, hybrid attachments, or mandatory shared inspection become real requirements and a separately approved change package defines cost, rollback, validation, and blast radius.

## Acceptance Criteria Met

- supplies the required centralized-inspection VPC architecture sketch;
- separates uninspected and inspected Transit Gateway route domains;
- shows the two-pass firewall path and symmetric return requirement;
- records appliance-mode, Availability Zone, routing, security, logging, and cost checks;
- distinguishes east-west, hybrid, egress, ingress, and private-service traffic; and
- preserves the documentation-only and no-AWS-change boundary.

## Official AWS References

- [AWS Transit Gateway appliance mode and shared-services appliance example](https://docs.aws.amazon.com/vpc/latest/tgw/how-transit-gateways-work.html)
- [Transit Gateway VPC attachments and appliance mode](https://docs.aws.amazon.com/vpc/latest/tgw/tgw-vpc-attachments.html)
- [Transit Gateway route tables](https://docs.aws.amazon.com/vpc/latest/tgw/tgw-route-tables.html)
- [Avoiding asymmetric routing with AWS Network Firewall](https://docs.aws.amazon.com/network-firewall/latest/developerguide/asymmetric-routing.html)
- [Centralized network security for VPC-to-VPC and hybrid traffic](https://docs.aws.amazon.com/whitepapers/latest/building-scalable-secure-multi-vpc-network-infrastructure/centralized-network-security-for-vpc-to-vpc-and-on-premises-to-vpc-traffic.html)
- [AWS Network Firewall components](https://docs.aws.amazon.com/network-firewall/latest/developerguide/firewall-components.html)
- [Transit gateway-attached firewall considerations](https://docs.aws.amazon.com/network-firewall/latest/developerguide/tgw-firewall-considerations.html)
- [AWS Prescriptive Guidance: centralized inspection with Transit Gateway](https://docs.aws.amazon.com/prescriptive-guidance/latest/integrate-third-party-services/architecture-3.html)
