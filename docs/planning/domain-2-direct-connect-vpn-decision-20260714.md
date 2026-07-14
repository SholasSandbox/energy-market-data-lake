# Domain 2 Direct Connect and Site-to-Site VPN Decision - 2026-07-14

<!-- markdownlint-disable MD013 -->

## Scope

This is the tracker-required hybrid-connectivity decision table. It is a
documentation-only SAP-C02 study artifact and does not authorize Direct
Connect, Site-to-Site VPN, Transit Gateway, virtual private gateway, routing,
or DNS changes in AWS.

## Decision Table

| Requirement signal | Prefer | Why it wins | Do not choose it when | Design checks and SAP-C02 trap |
|---|---|---|---|---|
| Hybrid connectivity is needed quickly, with low initial commitment, or as a backup path. | AWS Site-to-Site VPN | It supplies encrypted IPsec transport over the internet without waiting for dedicated circuit provisioning. | The workload needs sustained, highly predictable latency or bandwidth characteristics. | A connection includes two tunnels; design both paths, BGP/static routing, health monitoring, and failover. Do not confuse encrypted transport with hybrid DNS resolution. |
| A long-lived hybrid workload needs more consistent network performance and higher bandwidth potential. | AWS Direct Connect | It provides dedicated private network connectivity rather than internet-based transport. | The requirement prioritizes fastest delivery, temporary connectivity, or minimal operational commitment. | Confirm location/provider lead time, virtual interface and gateway model, routing, redundancy, and cost. Do not assume Direct Connect alone provides IPsec encryption. |
| Hybrid traffic needs both dedicated connectivity and encrypted overlay/failover characteristics. | Direct Connect plus Site-to-Site VPN | The pattern combines the more consistent Direct Connect path with AWS-managed IPsec VPN. | The stated need is only a low-complexity, short-lived hybrid proof. | Define primary/backup route preference and test failure behavior. The combined pattern has more dependencies and operational overhead. |
| Multiple VPCs and on-premises networks need centralized hybrid transit. | Transit Gateway with VPN and/or Direct Connect gateway attachments | It makes hybrid routing a hub-and-spoke control-plane decision rather than a series of isolated VPC links. | Only one VPC needs a straightforward hybrid link. | Separate Transit Gateway route domains and on-premises prefix propagation from DNS forwarding; do not treat the TGW as a DNS solution. |

## Comparative Guardrails

| Dimension | Site-to-Site VPN | Direct Connect | Direct Connect plus VPN |
|---|---|---|---|
| Underlay | Internet | Dedicated private connection | Dedicated connection with IPsec overlay/backup pattern |
| Encryption | IPsec | Not implied by the transport choice | IPsec contributes end-to-end encryption where designed |
| Time to establish | Usually faster | Requires provider/location coordination | Longest setup and most coordination |
| Performance expectation | Internet-path dependent | More consistent latency and bandwidth potential | More consistent primary path plus encryption/failover design |
| Resilience focus | Use both tunnels and customer-side redundancy | Design redundant Direct Connect paths and routing | Test primary/backup preference and both independent failure modes |
| Typical wrong answer | Choosing it when consistent performance is the primary requirement | Choosing it when the scenario prioritizes speed or low commitment | Choosing it without a stated encryption, resilience, or consistency requirement |

## Lakehouse Decision and Promotion Triggers

The current lakehouse baseline has no evidenced on-premises dependency, so
neither connectivity option is justified today. If a later requirement appears:

1. start with Site-to-Site VPN for a bounded hybrid proof or backup path;
2. promote to Direct Connect only when durable performance, bandwidth, or
   operational-consistency evidence justifies its added commitment; and
3. use Route 53 Resolver separately if systems must resolve each other's
   private DNS namespaces.

This deliberately keeps the transport, routing, encryption, and DNS decisions
separate.

## Acceptance Criteria Met by This Slice

- compares VPN, Direct Connect, and the combined pattern;
- records availability, encryption, routing, cost/commitment, and operational
  trade-offs;
- identifies the Transit Gateway decision boundary for multi-VPC hybrid
  routing; and
- preserves the no-AWS-implementation boundary.

Scenario drills remain necessary before marking the tracker deliverable fully
complete.

## References

- AWS Direct Connect plus Site-to-Site VPN: `https://docs.aws.amazon.com/whitepapers/latest/aws-vpc-connectivity-options/aws-direct-connect-site-to-site-vpn.html`
- AWS Site-to-Site VPN concepts and features: `https://docs.aws.amazon.com/vpn/latest/s2svpn/VPC_VPN.html`
