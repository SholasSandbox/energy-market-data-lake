# Domain 2 Network Access Patterns - 2026-06-21

<!-- markdownlint-disable MD013 -->

## Scope

This note records a compact SAP-C02 carry-forward comparison for the network
access patterns most likely to appear in Energy Data Lakehouse architecture
questions.

It aligns with:

- `docs/planning/sap-c02-readiness-tracker.md`, which keeps Domain 2 active
  through 2026-07-12 and identifies networking as a carry-forward weak area.
- `docs/planning/domain-2-lakehouse-consolidation-20260617.md`, which calls
  out network access patterns as an open Domain 2 decision skill.
- `/Users/[redacted-user]/Kiro-Workspace/aws-sap-c02-governance/exercises/sap-c02-exercise-002-marking-and-revision-log.md`,
  which exposed a hybrid DNS weak area around Route 53 Resolver.

This is a documentation-only study artifact. It does not authorize VPC
implementation, hybrid connectivity changes, Direct Connect, VPN, Transit
Gateway, PrivateLink, or DNS changes in AWS.

## Why This Matters

SAP-C02 questions often hinge on choosing the smallest networking pattern that
solves the real requirement:

- private service consumption versus full network connectivity
- one-to-one VPC connectivity versus multi-VPC hub-and-spoke
- hybrid transport versus hybrid DNS resolution
- quick setup versus more consistent long-term hybrid performance

The main exam trap from exercise 002 was treating a DNS problem like a
governance problem. Route 53 Resolver solves hybrid DNS resolution; AWS Config
does not.

## Quick Decision Rules

| If the requirement is... | Prefer... | Why |
|---|---|---|
| Private access from a VPC to an AWS service or a specific service in another account, without opening full network reachability | AWS PrivateLink / VPC endpoints | Private service-level access is narrower than broad VPC-to-VPC routing |
| Private IP connectivity between two VPCs with simple, limited scale and no transit requirement | VPC peering | Lowest-complexity direct VPC-to-VPC routing |
| Hub-and-spoke connectivity across many VPCs and possibly on-premises networks | AWS Transit Gateway | Central transit is easier to scale than many one-to-one peerings |
| Hybrid connectivity to on-premises fast, cheaply, or temporarily | AWS Site-to-Site VPN | Faster to establish than dedicated private circuits |
| More consistent hybrid connectivity with lower latency and higher bandwidth expectations | AWS Direct Connect | Dedicated connectivity is more stable than internet-based VPN |
| Hybrid name resolution between AWS and on-premises | Route 53 Resolver inbound/outbound endpoints plus forwarding rules | DNS forwarding is a separate problem from transport |

## VPC Connectivity Comparison Matrix (Early Study Slice)

This expands the compact carry-forward aid into the first bounded matrix slice.
It improves decision quality, but does not complete the later diagrams or
authorize a network build.

| Pattern | Connectivity scope and routing | Use when | Do not choose when | Key trade-off and exam trap |
|---|---|---|---|---|
| VPC fundamentals | Subnets use route tables; a route's target determines where traffic can go. Security groups and network ACLs are separate control layers. | Explaining the baseline before selecting a connectivity service. | Treating a subnet, route table, or security group as a connectivity service. | A route is necessary but does not itself grant traffic permission. |
| Security groups vs network ACLs | Security groups are stateful and apply to ENIs; network ACLs are stateless and apply at the subnet boundary. | Controlling workload-to-workload access or an explicit subnet-level allow/deny boundary. | Using a network ACL as though return traffic is automatically allowed. | Security groups do not need an ephemeral-return rule; network ACLs do. |
| Gateway and interface VPC endpoints | Gateway endpoints add route-table entries for S3/DynamoDB; interface endpoints use private ENIs for supported services and PrivateLink-based service access. Neither provides general VPC-to-VPC routing. | A VPC needs private, least-broad access to an AWS service or a published service. | The requirement is broad IP connectivity among CIDRs. | Choosing peering or TGW when only a service, not a network, must be reached. |
| VPC peering | Direct private IP routes between two non-overlapping VPC CIDRs; no transitive routing. | Two VPCs need a small, explicit relationship with no shared transit requirement. | CIDRs overlap or the topology needs hub-and-spoke, inspection, or transit. | Peering does not make a third VPC or on-premises network reachable through either peer. |
| AWS Transit Gateway | Central route tables connect VPC, VPN, and Direct Connect attachments in a hub-and-spoke topology. | Several VPCs, shared services, inspection paths, or hybrid attachments need managed transit. | Only one simple VPC-to-VPC path is required. | It reduces mesh complexity but introduces attachment, routing, and data-processing cost. |
| Centralized inspection VPC | Traffic is steered through a dedicated inspection path, commonly with Transit Gateway route domains and firewall or Gateway Load Balancer endpoints; symmetric routing matters. | A multi-VPC estate needs consistent egress or east-west inspection. | A single small VPC has no stated centralized-control requirement. | Sending traffic to an appliance without designing return-path symmetry causes asymmetric flows. |
| NAT Gateway | Private-subnet workloads use a route to NAT for outbound IPv4 internet or public-service access; NAT is placed per Availability Zone for resilient zonal egress. | Private workloads need outbound access and no inbound internet initiation. | Private AWS-service access can use a VPC endpoint, or an IPv6-only design can use an egress-only internet gateway. | NAT is not a private-service endpoint and cross-AZ routing can add cost and resilience risk. |
| Site-to-Site VPN | Encrypted IPsec transport over the internet, normally using two tunnels, attaches to a virtual private gateway or Transit Gateway. | Hybrid access is needed quickly, with lower commitment, or as a backup path. | The requirement prioritizes consistently high bandwidth or predictable latency. | VPN solves transport, not hybrid DNS by itself. |
| Direct Connect | Dedicated private hybrid transport; a VPN overlay can add encryption or backup where the scenario requires it. | Long-lived hybrid connectivity needs more consistent performance and higher bandwidth potential. | The need is temporary, low-volume, or urgently needs the quickest setup. | Direct Connect is not automatically encrypted and does not replace Route 53 Resolver for DNS forwarding. |
| Route 53 Resolver | Inbound endpoints answer on-premises queries for AWS private names; outbound endpoints forward AWS queries for on-premises domains. Transport still needs VPN, Direct Connect, or another path. | Hybrid systems must resolve each other's private DNS names. | The requirement is only packet transport or only public DNS resolution. | Do not answer a DNS problem with Transit Gateway, Direct Connect, or AWS Config alone. |

### Matrix Acceptance Boundary

The decision-level matrix and its required diagrams/tables are now complete:
the VPC endpoint comparison is recorded in
`docs/planning/domain-2-vpc-endpoint-diagram-20260715.md` and
`diagrams/vpc-endpoint-study.mmd`. The matrix remains a documentation-only
study aid; it does not prove a live network implementation or score learner
recall.

## Focus Comparison: PrivateLink vs Peering vs Transit Gateway

| Choice | Best fit | What it does not do | Practical cue |
|---|---|---|---|
| AWS PrivateLink | Publish or consume a specific service privately | It does not create broad layer-3 connectivity between VPCs | Use when the question says "access this service privately" rather than "connect these networks" |
| VPC peering | Direct connectivity between two VPCs | It does not support transitive routing and cannot be used as a shared transit point | Use when the topology is small and explicit |
| AWS Transit Gateway | Central routing across many VPCs and on-premises attachments | It is usually more than you need for one small VPC-to-VPC path | Use when the topology is becoming hub-and-spoke or shared-services oriented |

Decision shortcut:

- One service only: PrivateLink.
- Two VPCs only: peering.
- Many VPCs or VPCs plus on-premises: Transit Gateway.

## Focus Comparison: Direct Connect vs VPN

| Choice | Best fit | Strength | Limitation | Practical cue |
|---|---|---|---|---|
| AWS Site-to-Site VPN | Fast hybrid setup, lower commitment, or backup connectivity | Quick to establish and encrypted with two tunnels for high availability | Internet-based path is generally less consistent than dedicated connectivity | Use when the question emphasizes speed, minimal setup, or backup |
| AWS Direct Connect | Longer-lived hybrid connectivity with stronger consistency expectations | More consistent network experience, lower latency, and higher bandwidth potential | More setup effort and commitment than VPN | Use when the question emphasizes predictable performance or production-style hybrid links |
| Direct Connect + VPN | Stronger hybrid design needing both private connectivity and encrypted failover/overlay | Combines dedicated connectivity with managed IPsec | More moving parts | Use when the question wants resilience plus more consistent hybrid transport |

Decision shortcut:

- Fastest path: VPN.
- Most consistent path: Direct Connect.
- Production-style belt-and-braces path: Direct Connect plus VPN.

## Focus Comparison: Route 53 Resolver

Hybrid DNS questions should trigger one answer pattern first:

- inbound endpoints for queries from on-premises into AWS
- outbound endpoints for queries from AWS into on-premises
- forwarding rules for the relevant private domains

Important distinction:

- Direct Connect or VPN carries packets.
- Route 53 Resolver solves DNS forwarding and resolution.

If the requirement says AWS workloads must resolve on-premises names and
on-premises systems must resolve AWS private names, the answer starts with
Route 53 Resolver, not AWS Config, not Transit Gateway by itself, and not
Direct Connect by itself.

### Compact Hybrid DNS Sketch

```text
AWS VPC workload
   |
   v
VPC Resolver (VPC+2)
   |
   | outbound endpoint + forwarding rule for on-prem domain
   v
Direct Connect or Site-to-Site VPN
   |
   v
On-prem DNS resolver

On-prem client
   |
   v
On-prem DNS resolver
   |
   | forwarding rule for AWS private domain
   v
Direct Connect or Site-to-Site VPN
   |
   v
Route 53 Resolver inbound endpoint
   |
   v
AWS private hosted zone / VPC name resolution
```

Memory hook:

- outbound endpoint = AWS asks on-premises
- inbound endpoint = on-premises asks AWS
- forwarding rules decide which private domains follow each path

## For This Lakehouse

For the current Energy Data Lakehouse case study, the repo baseline remains
frozen. This note is a decision guide, not an implementation plan.

If networking work is revisited later, the most likely sequence is:

1. Use VPC endpoints where a workload VPC needs private access to AWS services
   such as S3 or selected service APIs.
2. Use Route 53 Resolver if a later hybrid or multi-VPC design introduces a
   real private DNS requirement.
3. Use VPC peering only for a very small number of direct VPC relationships
   where no transit behavior is needed.
4. Use Transit Gateway only if the case study grows into a multi-VPC or
   multi-account hub-and-spoke design.
5. Use VPN first for lightweight hybrid proof or backup connectivity; reserve
   Direct Connect for scenarios that genuinely need a more consistent hybrid
   network experience.

This means the near-term study priority is decision quality, not network build
out.

## Carry-Forward Risks

- Do not confuse hybrid transport with hybrid DNS.
- Do not choose Transit Gateway when the simpler answer is peering or a VPC
  endpoint.
- Do not choose peering when the requirement needs transitive routing.
- Do not choose Direct Connect when the scenario prioritizes setup speed over
  long-term consistency.
- Do not treat this note as completion of the later networking milestone; it is
  a compact carry-forward aid for Domain 2.

## References

- AWS PrivateLink overview:
  `https://docs.aws.amazon.com/vpc/latest/privatelink/what-is-privatelink.html`
- VPC peering basics and limitations:
  `https://docs.aws.amazon.com/vpc/latest/peering/vpc-peering-basics.html`
- AWS Transit Gateway overview:
  `https://docs.aws.amazon.com/vpc/latest/tgw/what-is-transit-gateway.html`
- Route 53 VPC Resolver overview:
  `https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/resolver.html`
- AWS Direct Connect + AWS Site-to-Site VPN:
  `https://docs.aws.amazon.com/whitepapers/latest/aws-vpc-connectivity-options/aws-direct-connect-site-to-site-vpn.html`
- AWS Site-to-Site VPN overview:
  `https://docs.aws.amazon.com/vpn/latest/s2svpn/VPC_VPN.html`
