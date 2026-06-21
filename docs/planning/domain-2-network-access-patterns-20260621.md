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

## Comparison Matrix

| Pattern | Use when | Avoid when | Key trade-off | SAP-C02 trap |
|---|---|---|---|---|
| VPC endpoints / AWS PrivateLink | A workload in a VPC needs private access to S3, DynamoDB, AWS APIs, partner services, or a specific producer service without broad network trust | The real need is any-to-any IP routing across whole CIDR ranges | Gives private service consumption rather than general network connectivity | Picking peering or TGW when only one service needs private access |
| VPC peering | Two VPCs need direct private IP connectivity and the topology is small and controlled | You need transitive routing, central hub routing, or overlapping CIDRs | Simple and direct, but scales poorly as connections grow | Treating peering like a transit hub or assuming it solves hybrid transit |
| AWS Transit Gateway | Many VPCs, shared services, or on-premises networks need hub-and-spoke connectivity | Only two VPCs need simple communication | Easier operational scaling, but adds central routing design and attachment cost | Using TGW too early for a tiny topology |
| AWS Site-to-Site VPN | Hybrid connectivity is needed quickly, at lower commitment, or as a backup path | The requirement is stable high-throughput, lower-latency dedicated connectivity | Faster to start, but less consistent than dedicated private connectivity | Assuming VPN and Direct Connect are interchangeable for performance-sensitive designs |
| AWS Direct Connect | A workload needs dedicated private connectivity with more consistent latency and higher bandwidth expectations | The need is temporary, low-volume, or early-stage enough that VPN is sufficient | Stronger operational experience, but more setup and commitment | Choosing DX when the scenario actually prioritizes speed of deployment or lower complexity |
| Route 53 Resolver | AWS and on-premises systems must resolve each other's private DNS names | The problem is only IP transport, not DNS | Solves hybrid DNS with inbound/outbound endpoints and forwarding rules | Choosing AWS Config, TGW, or DX alone for a DNS resolution problem |

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
