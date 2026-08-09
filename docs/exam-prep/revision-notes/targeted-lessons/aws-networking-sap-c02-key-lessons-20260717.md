# AWS Networking Beyond Route 53: SAP-C02 Key Lessons - 2026-07-17

<!-- markdownlint-disable MD013 -->

**Last revised:** 2026-08-08

## Purpose and Scope

**Document role:** source-backed lesson. Return to the
[Exam-Prep Revision Hub](../README.md) to choose a learn, test, review, or audit
workflow.

This document is the networking companion to
[Amazon Route 53 Key SAP-C02 Lessons](route-53-sap-c02-key-lessons-20260715.md).
Use the Route 53 lesson for DNS, routing policies, health checks, hosted zones,
Resolver endpoints and rules, DNS Firewall, and DNSSEC. Use this lesson for the
remaining high-value SAP-C02 networking decisions.

This is a documentation-only study artifact. It does not authorize VPC,
endpoint, load-balancer, firewall, VPN, Direct Connect, Transit Gateway, Cloud
WAN, VPC Lattice, or other AWS changes.

## How to Revise the Two-Document Set

| Time available | Revision route |
|---|---|
| 15 minutes | Read [The Networking Decision Sequence](#the-networking-decision-sequence), [VPC-to-VPC and Service Connectivity](#3-vpc-to-vpc-and-service-connectivity), the three service-selection tables for [hybrid connectivity](#4-hybrid-connectivity-in-depth), [load balancing](#5-elastic-load-balancing), and [global ingress](#6-global-ingress-and-multi-region-networking), then the [High-Value Traps](#10-high-value-sap-c02-traps). |
| 35 minutes | Add VPC routing, IPv6/egress, network security, troubleshooting, and resilience/cost. Then answer the [Recall Check](#recall-check) without notes. |
| 60–90 minutes | Read this companion and the [Route 53 lesson](route-53-sap-c02-key-lessons-20260715.md), draw the main paths, and complete both recall checks blind. |
| Remediation block | Review only missed topics, record genuine misses in `wrong-answers.md`, and retest them after spacing. |

Use this order during a full block:

1. Read this companion through hybrid connectivity.
2. Read the Route 53 lesson and draw the inbound and outbound Resolver flows.
3. Return here for load balancing, global ingress, security, and operations.
4. Answer both Recall Checks without notes.
5. Open the [source-backed scenario review](../../networking-scenario-drill-review-20260715.md)
   only after the recall attempt.

## Topic Navigation

| If the scenario says... | Go to |
|---|---|
| CIDR, subnet, route table, longest prefix, or return path | [VPC Addressing, Subnets, and Route Selection](#1-vpc-addressing-subnets-and-route-selection) |
| Internet Gateway, NAT, IPv6, egress-only, DNS64, or NAT64 | [IPv4, IPv6, and Egress](#2-ipv4-ipv6-and-egress) |
| Peering, TGW, PrivateLink, endpoint, VPC Lattice, or Cloud WAN | [VPC-to-VPC and Service Connectivity](#3-vpc-to-vpc-and-service-connectivity) |
| VPN, Direct Connect, BGP, VIF, VGW, DXGW, or customer gateway | [Hybrid Connectivity in Depth](#4-hybrid-connectivity-in-depth) |
| HTTP routing, static IP, TCP/UDP, or appliance fleet | [Elastic Load Balancing](#5-elastic-load-balancing) |
| CDN, anycast, multi-Region ingress, or global WAN | [Global Ingress and Multi-Region Networking](#6-global-ingress-and-multi-region-networking) |
| SG, NACL, WAF, Shield, firewall, or inspection symmetry | [Network Security and Inspection](#7-network-security-and-inspection) |
| Timeout, flow evidence, path analysis, or packet copy | [Network Operations and Troubleshooting](#8-network-operations-and-troubleshooting) |
| Availability-zone design, tunnels, paid paths, or transfer | [Resilience and Cost Checks](#9-resilience-and-cost-checks) |
| DNS, hosted zone, Resolver, or Route 53 routing policy | [Route 53 companion lesson](route-53-sap-c02-key-lessons-20260715.md) |

## Coverage Audit and Revision Map

The folder already contains a source-backed scenario review and a focused blind
attempt. This companion fills the decision areas those documents do not teach
in enough depth; it does not replace their recall evidence.

| Revision area | Primary document | Supporting practice or design artifact |
|---|---|---|
| Route 53 authoritative DNS, routing policies, health checks, hybrid DNS, DNS Firewall, DNSSEC | [Route 53 key lessons](route-53-sap-c02-key-lessons-20260715.md) | [Hybrid-DNS reading guide](../../../planning/domain-2-route53-resolver-hybrid-dns-20260715.md) |
| VPC addressing, subnet roles, route selection, IPv4 and IPv6 egress | This companion | [VPC/subnet/route-table reading guide](../../../planning/domain-2-vpc-subnet-route-table-diagram-20260715.md) |
| Security groups and network ACLs | This companion provides the decision shortcut | [Security-groups-versus-NACL comparison](../../../planning/domain-2-security-groups-network-acls-comparison-20260715.md) |
| Peering, Transit Gateway, PrivateLink, endpoints, and NAT | This companion provides the consolidated selection rules | [Networking scenario review](../../networking-scenario-drill-review-20260715.md) and [blind attempt](../../networking-scenario-drill-blind-attempt-20260715.md) |
| Direct Connect, VPN, virtual interfaces, gateways, and resilience | This companion | [Direct Connect versus VPN decision table](../../../planning/domain-2-direct-connect-vpn-decision-20260714.md) |
| ALB, NLB, and GWLB | This companion | None; this is the primary revision note |
| CloudFront, Global Accelerator, Cloud WAN, and VPC Lattice | This companion | None; this is the primary revision note |
| WAF, Shield, Network Firewall, appliance inspection | This companion | [Centralized-inspection reading guide](../../../planning/domain-2-centralized-inspection-vpc-architecture-20260715.md) |
| Flow Logs, Reachability Analyzer, and Traffic Mirroring | This companion | None; this is the primary revision note |

## The Networking Decision Sequence

For a long SAP-C02 scenario, make the decisions in this order:

1. **Scope:** one workload, one service, two VPCs, many VPCs, multiple Regions,
   or hybrid networks?
2. **Addressing:** IPv4, IPv6, dual stack, overlapping CIDRs, and route
   summarization constraints?
3. **Reachability:** which route must exist in both directions?
4. **Transport:** internet, AWS private network, VPN, or Direct Connect?
5. **Name resolution:** public DNS, private hosted zone, or hybrid Resolver?
6. **Authorization and inspection:** security group, network ACL, endpoint
   policy, firewall, WAF, or appliance?
7. **Resilience:** multi-AZ, multi-tunnel, multi-connection, or multi-Region?
8. **Operations and cost:** how will the path be proved, observed, and paid for?

Exam rule: no single service answers all eight questions. A Transit Gateway is
not DNS; a private hosted zone is not connectivity; a security group is not a
route; and Direct Connect is not encryption by default.

## 1. VPC Addressing, Subnets, and Route Selection

### Addressing Rules

- Choose non-overlapping CIDR ranges when networks may later peer, attach to a
  Transit Gateway, or connect on premises. NAT or service-level exposure may
  be needed when overlap cannot be removed.
- A subnet belongs to exactly one Availability Zone. Spread independent
  workload and network components across Availability Zones for resilience.
- A **public subnet** has a route to an Internet Gateway. An instance also
  needs a public IPv4 address or an IPv6 address, plus compatible security
  controls, to communicate directly with the internet.
- A **private subnet** has no direct route to an Internet Gateway. It may have
  outbound IPv4 access through a NAT Gateway or private service access through
  endpoints.
- An **isolated subnet** has no internet egress path. It can still communicate
  with explicitly routed private destinations.

Exam trap: public and private describe routing, not merely whether a resource
has a public address.

### Route-Table Rules

1. Every route table contains a local route for each VPC CIDR block.
2. AWS first applies **longest-prefix match**. A `/32` beats a `/24`, which
   beats `0.0.0.0/0`.
3. For equally specific routes, static routes generally take priority over
   propagated routes. Prefix-list routes and identical destinations have
   additional documented priority rules, so avoid ambiguous designs.
4. A route selects the next hop; it does not authorize traffic. Security
   groups, network ACLs, firewalls, and endpoint policies are separate checks.
5. Stateful conversations still need a valid return route. Centralized
   inspection additionally needs symmetric forward and return paths.

### Route Example

| Destination | Target | Meaning |
|---|---|---|
| `10.20.0.0/16` | `local` | Communication inside the VPC |
| `10.50.0.0/16` | Transit Gateway | More-specific private hybrid or spoke route |
| S3 prefix list | Gateway endpoint | Eligible same-Region S3 traffic avoids the default route |
| `0.0.0.0/0` | NAT Gateway | General outbound IPv4 from a private subnet |
| `::/0` | Egress-only Internet Gateway | Outbound-initiated IPv6 internet access |

## 2. IPv4, IPv6, and Egress

| Requirement | First service or route | Important boundary |
|---|---|---|
| Direct public IPv4 ingress and egress | Internet Gateway plus public IPv4 address | The Internet Gateway performs the public/private IPv4 address mapping; routes and controls are still required. |
| Private-subnet outbound IPv4 | Public NAT Gateway, normally in the same AZ as its clients | It is not for unsolicited inbound connections; cross-AZ routing adds dependency and transfer cost. |
| Outbound-only IPv6 internet access | Egress-only Internet Gateway | IPv6 does not use an IPv4 NAT Gateway for ordinary IPv6 internet destinations. |
| IPv6-only workload calls IPv4-only destination | DNS64 plus NAT64 through a NAT Gateway | DNS64 synthesizes an IPv6 answer; NAT64 translates the traffic. Both DNS and routing must be designed. |
| Private AWS service access | Gateway or interface VPC endpoint | Prefer a supported endpoint before general NAT egress when scope and cost fit. |

Key IPv6 distinction: IPv6 addresses are globally unique, but a route through
an Internet Gateway can allow inbound connections. An egress-only Internet
Gateway permits outbound-initiated IPv6 communication without accepting new
internet-initiated flows.

For NAT64, Route 53 Resolver DNS64 can synthesize an address in
`64:ff9b::/96`. The IPv6-only subnet routes that prefix to a NAT Gateway, whose
IPv4 path then reaches the IPv4 destination.

## 3. VPC-to-VPC and Service Connectivity

| Requirement | Best starting point | Why | Common trap |
|---|---|---|---|
| Two VPCs need bilateral private IP routing | VPC peering | Smallest general network-to-network fit | Peering is non-transitive and overlapping CIDRs are not supported. |
| Many VPCs, segmentation, transit, or hybrid routing | Transit Gateway | Regional hub with attachment route tables | It does not perform DNS forwarding or automatically inspect traffic. |
| Consumers need one provider service without broad VPC reachability | AWS PrivateLink | Service-oriented private exposure through endpoint ENIs | It is not general transitive routing. |
| Workloads need supported AWS APIs privately | VPC endpoints | Keeps eligible service traffic off NAT/internet paths | Gateway and interface endpoints differ in reachability, DNS, controls, and price. |
| Application services and resources need policy-based connectivity across VPCs/accounts | VPC Lattice | Managed application networking with service networks, auth policies, and observability | Do not choose it when the requirement is arbitrary Layer 3 network transit. |
| Centrally managed global cloud and branch network | AWS Cloud WAN | Policy-defined Regions, segments, and attachments | A regional Transit Gateway remains the simpler fit for a limited regional hub. |

### VPC Lattice: Application Networking, Not a Bigger Router

Transit Gateway answers a network question: **which IP networks can route
through this hub?** VPC Lattice answers an application question: **which
clients may call this named service, and how should the request reach a healthy
target?**

```text
provider VPC/account
    application targets: EC2, containers, Lambda, IP addresses, or ALB
                           ^
                           | target group
                     listener and rules
                           ^
                           | VPC Lattice service association publishes it
                           |
                    service network
             logical connectivity and policy boundary
                           |
                           | VPC association admits clients
                           v
consumer VPC/account
    client -> service DNS name -> VPC Lattice -> selected healthy target
```

The main components have distinct jobs:

| Component | Mental shortcut | What it does |
|---|---|---|
| Service | A callable application endpoint | Defines listeners, routing rules, and target groups for one application capability |
| Service network | A controlled catalogue of callable services and resources | Groups services/resources with client VPCs; it supplies the connectivity boundary and can supply auth-policy boundaries for services |
| Service association | Publish this service | Makes a service available through the service network |
| VPC association | Admit clients from this VPC | Allows workloads in the associated VPC to reach services/resources in the service network when authorization also permits it |
| Listener and rules | Receive and route requests | Match HTTP, HTTPS, or TLS traffic and forward it to an appropriate target group |
| Target group | The service back end | Contains the EC2 instances, IP addresses, Lambda functions, ALBs, or supported container targets that serve requests |
| Resource configuration | Publish a private resource rather than a service | Represents an IP address, domain-name target, RDS database, or resource group reached through a resource gateway; it is not a service target group |
| Auth policy | Who may call which service? | Applies resource-based authorization at the service-network or service boundary; VPC Lattice auth policies do not apply to resource configurations |
| AWS RAM share | Cross-account publication | Shares a service network, service, or resource configuration across accounts where supported |

The client uses the service's generated DNS name. VPC Lattice performs service
discovery and request routing; the client does not need a route to every
provider VPC CIDR. This is why VPC Lattice can connect application services
across accounts even when VPC CIDR ranges overlap.

With auth type `AWS_IAM`, access is not granted merely because the VPC and
service are associated. The caller's identity policy and the applicable VPC
Lattice auth policies must explicitly allow the request; an explicit deny
wins. A security group attached to the VPC association remains a separate
network control, while VPC Lattice access logs and metrics provide application
request evidence.

#### VPC Lattice Versus Nearby Answers

| Requirement | Better starting point | Why VPC Lattice does or does not fit |
|---|---|---|
| Many VPCs need arbitrary bidirectional IP connectivity or transitive hybrid routing | Transit Gateway | TGW routes packets between attached networks; VPC Lattice is limited to published application services/resources and supported listener protocols |
| Many application teams need service discovery, request routing, IAM authorization, and monitoring across VPCs/accounts | VPC Lattice | These application-layer controls are the service's purpose; no peering mesh or load balancer per consumer is required |
| One provider exposes one private service to many consumers without broad network reachability | AWS PrivateLink | PrivateLink is the narrower endpoint-based provider/consumer pattern; VPC Lattice becomes stronger when several services need a shared service-network and policy model |
| A VPC workload needs a private path to an AWS API such as S3 or Systems Manager | VPC endpoint | The endpoint is the direct AWS-service access pattern; VPC Lattice is for application services and supported resources |
| Branches and VPCs need a centrally governed global WAN | AWS Cloud WAN | Cloud WAN manages the wider network topology; VPC Lattice manages application-service access |

High-value exam cues for VPC Lattice are **service-to-service connectivity**,
**multiple VPCs or accounts**, **service discovery**, **request routing**,
**IAM-based authorization**, **observability**, and sometimes **overlapping
CIDRs**. If the requirement instead says arbitrary Layer 3 connectivity,
propagated routes, or transitive routing, start with Transit Gateway.

### Endpoint Shortcut

- **Gateway endpoint:** S3 or DynamoDB; route-table target; no endpoint ENI or
  security group; no additional endpoint charge; VPC-local route-table model.
- **Interface endpoint:** AWS PrivateLink ENIs in selected subnets; security
  groups; private DNS considerations; hourly per-AZ and data-processing cost;
  can support broader private access patterns when the service permits them.
- **Endpoint policy:** an additional authorization boundary, not a replacement
  for identity policies or resource policies.

## 4. Hybrid Connectivity in Depth

### VPN and Direct Connect Selection

| Requirement | Starting design |
|---|---|
| Encrypted connectivity quickly, with internet-path variability acceptable | Site-to-Site VPN |
| More consistent private bandwidth and latency | Direct Connect |
| Consistent underlay plus IPsec encryption | VPN over Direct Connect, or Direct Connect plus a separate VPN path |
| Fast initial path followed by dedicated connectivity | VPN first, then Direct Connect |
| High resilience | Redundant devices, tunnels, connections, and locations according to the failure requirement |

AWS Site-to-Site VPN provides two tunnels. Configure and monitor both whenever
the customer gateway supports it. Dynamic routing uses BGP and adapts to route
changes; static routing is a narrower fit when BGP is unavailable.

Direct Connect is a dedicated network connection, but it does not automatically
encrypt traffic. MACsec is available only for supported dedicated connections
and configurations. An IPsec VPN supplies the usual exam answer when encryption
must be layered over private Direct Connect connectivity.

### BGP Mental Model for SAP-C02

Border Gateway Protocol exchanges **reachable IP prefixes and path
attributes** between autonomous systems. In AWS hybrid designs it is the
dynamic-routing control plane commonly used by Direct Connect virtual
interfaces and dynamically routed Site-to-Site VPN connections.

```text
on-premises router
    -- advertises on-premises prefixes --> AWS
    <-- receives AWS/VPC prefixes ------- AWS

BGP chooses candidate paths
    -> route-table and gateway rules still apply
    -> security and return routing remain separate
```

Keep direction explicit:

- To influence **AWS-to-on-premises** traffic, change the attributes of the
  on-premises prefixes advertised to AWS. Direct Connect local-preference
  communities and AS-path prepending are common controls.
- To influence **on-premises-to-AWS** traffic, apply routing policy on the
  customer routers to the prefixes AWS advertises. AWS cannot force the
  customer's local preference.
- A healthy BGP session proves route exchange, not application reachability.
  Route tables, return routes, security controls, MTU, and application health
  can still fail.

### Direct Connect BGP Path Selection

For routes received over private or transit virtual interfaces, evaluate the
destination prefix before BGP path attributes:

1. **Longest-prefix match wins.** A more-specific advertised prefix is selected
   before local preference, AS path, or MED is considered.
2. For equally specific candidates, AWS then evaluates Direct Connect routing
   attributes in its documented order, including local preference, AS path
   length, and MED where applicable.

Therefore, to make one Direct Connect path active only for a subset of an
on-premises network, advertise that subset as a more-specific prefix over the
preferred BGP session. AS-path prepending cannot defeat a competing route with
a longer prefix.

Exam trap: route specificity is evaluated before “Direct Connect versus VPN”
or BGP-path tuning. Compare prefixes first, then compare attributes only among
equally specific routes.

For private and transit virtual interfaces, use this exam-order model for
routes AWS receives from on premises:

```text
1. longest prefix
2. Direct Connect local preference
3. shortest AS_PATH
4. lowest MED, when the preceding attributes tie
5. ECMP when eligible paths remain equal
```

AWS recommends local-preference communities rather than MED for deliberate
active/passive Direct Connect designs. Do not apply this list blindly to every
AWS route table or gateway: VPC route-table priority, virtual-private-gateway
priority, Transit Gateway route priority, and tunnel health introduce their
own rules.

### What the Main Attributes Mean

| Attribute or mechanism | Mental shortcut | Higher-value exam use |
|---|---|---|
| Prefix length | Exact destination wins first | A `/24` beats a `/16`, regardless of a more attractive attribute on the `/16` |
| Local preference | Which path should AWS prefer for the same prefix? | Direct Connect communities `7224:7100` low, `7224:7200` medium, and `7224:7300` high for private/transit VIF advertisements |
| AS_PATH | How many autonomous-system hops are represented? | With equal prefix and local preference, the shorter path wins; prepend the backup path to make it less attractive |
| MED | Hint about the preferred entry point | Lower is preferred only after prefix, local preference, and AS_PATH tie; AWS does not recommend it as the primary Direct Connect control |
| BGP community | Metadata that invokes an AWS routing policy | Use supported communities for Direct Connect preference or public-prefix propagation scope |
| ECMP | Use multiple equal paths | Requires equivalent prefixes and compatible attributes; design applications and firewalls for asymmetric possibilities |

### Active/Active Versus Active/Passive

| Desired outcome | Advertisement pattern |
|---|---|
| Active/active Direct Connect | Advertise the same prefix with equivalent AS_PATH/MED and the same local-preference community on eligible paths so ECMP can apply |
| Active/passive, same prefix length | Apply a higher local-preference community to the primary and a lower one to the backup; AS-path prepending is a later-order alternative |
| Prefer one path for only a subnet | Advertise that subnet as a more-specific prefix over the preferred path |
| VPN resilience | Configure both tunnels; with Transit Gateway, BGP and ECMP can use multiple equal-cost tunnels, whereas a VGW selects one tunnel across its VPN connections |

Never create accidental active/passive behaviour by advertising different
prefix lengths and then expecting communities or AS-path prepending to
override longest-prefix match.

### Direct Connect Communities Worth Recognizing

Private and transit VIF local-preference communities affect how AWS sends
traffic toward customer-advertised prefixes:

| Community | AWS preference |
|---|---|
| `7224:7100` | Low |
| `7224:7200` | Medium |
| `7224:7300` | High |

Public VIF scope communities control how widely AWS propagates a
customer-owned public prefix:

| Community | Propagation scope |
|---|---|
| `7224:9100` | Local AWS Region |
| `7224:9200` | AWS Regions in the associated continent |
| `7224:9300` | All public AWS Regions |

Without a public-VIF scope community, AWS advertises the prefix globally by
default. AWS-advertised public prefixes carry `NO_EXPORT`; do not treat a
public VIF as internet transit.

The numbers are recognition-level material. The decision pattern—scope versus
preference, and the direction of influence—is more important than rote recall.

### Direct Connect, VPN, and Route-Table Boundaries

- Longest-prefix match applies before equal-prefix tie breaking.
- In a VPC route table, an identical-prefix eligible static route takes
  priority over a propagated VPN or Direct Connect route.
- For equal prefixes known by a virtual private gateway, a BGP-propagated
  Direct Connect route is preferred over a static VPN route, which is preferred
  over a BGP-propagated VPN route. Tunnel health takes precedence.
- These VGW rules are not a universal ordering for Transit Gateway. In a TGW
  route table, static routes take priority over propagated routes for the same
  destination, and attachment-specific propagation priorities apply.
- A Direct Connect gateway **allowed prefix** is a filter for what associated
  resources may advertise; it does not create reachability by itself.

### Prefix Limits and Failure Diagnosis

- A private or transit VIF BGP session accepts up to 100 advertised IPv4 routes
  and 100 advertised IPv6 routes from on premises by default. Exceeding the
  limit can put the BGP session into an idle/down state.
- Prefer route summarization where it preserves the required routing and
  security boundaries.
- If the physical Direct Connect connection is up but the VIF is down, check
  VLAN, BGP peer IPs, ASN, BGP MD5 authentication, and advertised-prefix
  limits before investigating the application.
- BFD can accelerate failure detection when configured on the customer router;
  it does not replace redundant connections and locations.

### Direct Connect SiteLink

SiteLink lets supported private or transit VIFs exchange traffic between
Direct Connect points of presence over the AWS global network without routing
the traffic through an AWS Region. It is an on-premises-site connectivity
pattern, not a replacement for VPC/TGW routing.

Exam boundaries:

- public VIFs do not support SiteLink;
- a private VIF attached directly to a VGW does not support SiteLink;
- SiteLink has separate pricing and route-prefix limits; and
- when SiteLink is enabled, AS-path behaviour can alter location preference,
  so validate the intended active/active or active/passive advertisements.

### Direct Connect Virtual Interfaces

| Virtual interface | Reaches | Gateway relationship |
|---|---|---|
| Private VIF | Private VPC addresses | Virtual private gateway directly, or Direct Connect gateway associated with virtual private gateways |
| Transit VIF | One or more Transit Gateways | Direct Connect gateway associated with Transit Gateways |
| Public VIF | AWS public service endpoints using public IP addressing | Does not provide arbitrary internet transit |

A Direct Connect gateway is a global resource that connects eligible virtual
interfaces to associated virtual private gateways, Transit Gateways, or Cloud
WAN core networks. It is not a general router between VPCs attached through
virtual private gateways.

### Hybrid Gateway Shortcut

- **Virtual private gateway (VGW):** VPC-level VPN/DX termination; suitable for
  a smaller single-VPC pattern.
- **Transit Gateway (TGW):** regional multi-VPC and hybrid routing hub.
- **Direct Connect gateway (DXGW):** connects Direct Connect virtual interfaces
  to supported gateways across permitted Regions/accounts; it does not replace
  the TGW's routing role.
- **Customer gateway:** represents the customer-side VPN device and routing
  information.

Exam trap: design DNS independently. VPN or Direct Connect carries DNS packets,
whereas Route 53 Resolver endpoints and rules decide how private names resolve.

## 5. Elastic Load Balancing

| Load balancer | Layer and protocols | Choose it when | High-value cue |
|---|---|---|---|
| Application Load Balancer (ALB) | Layer 7 HTTP/HTTPS | Host/path/header/query routing, redirects, authentication integration, or web application delivery | Request-aware rules and AWS WAF integration |
| Network Load Balancer (NLB) | Layer 4 TCP/TLS/UDP and supported QUIC combinations | Very high-performance connection handling, static IP requirements, source-IP behavior, or non-HTTP protocols | One IP per enabled AZ; Elastic IP support for internet-facing IPv4 |
| Gateway Load Balancer (GWLB) | Layer 3 IP packets using GENEVE | Scale and insert fleets of virtual firewalls, IDS/IPS, or other appliances | GWLB endpoints steer traffic privately to the appliance service |
| Classic Load Balancer | Legacy Layer 4/7 behavior | Existing legacy design only | Prefer the current ALB/NLB/GWLB family for new designs |

Do not choose by the word *load*. Choose by protocol and function:

- web request routing: **ALB**;
- connection or transport routing: **NLB**;
- transparent appliance fleet insertion: **GWLB**.

Load-balancer health checks remove unhealthy targets from load-balancer routing.
Route 53 health checks influence DNS answers. Neither mechanism replicates
application data or guarantees recovery correctness.

## 6. Global Ingress and Multi-Region Networking

| Requirement | Service | Decision cue |
|---|---|---|
| Cache and deliver HTTP content near users | CloudFront | CDN, edge caching, origin access, HTTP security, edge functions |
| Static global anycast IPs and accelerated TCP/UDP routing to healthy regional endpoints | Global Accelerator | Fixed entry IPs, AWS global network, fast regional endpoint failover |
| DNS-based regional or policy routing | Route 53 | DNS answer selection; convergence is affected by caching and TTL |
| Policy-managed global VPC/branch WAN | Cloud WAN | Global core network, segments, attachment policies, centralized management |
| A few Regions with operator-managed hubs | Inter-Region TGW peering | Explicit regional TGWs and route management |

CloudFront and Global Accelerator are not interchangeable. CloudFront is an
HTTP content-delivery network and can cache content. Global Accelerator uses
static anycast IP addresses and optimized AWS network paths for supported
regional endpoints; it does not provide CDN object caching.

Route 53 can direct a new DNS resolution toward a Region, but clients may keep
cached answers. Global Accelerator keeps the same anycast entry addresses and
changes endpoint routing within the service.

## 7. Network Security and Inspection

### Control Selection

| Control | Scope | Stateful? | Best use |
|---|---|:---:|---|
| Security group | ENI/resource | Yes | Allow required inbound and outbound workload flows |
| Network ACL | Subnet boundary | No | Coarse allow/deny guardrail with numbered rule order |
| AWS Network Firewall | Routed VPC paths | Both stateless and stateful rule engines | Managed network inspection, domain/IP/protocol controls, centralized egress or east-west inspection |
| Gateway Load Balancer | Routed appliance insertion | Depends on appliance | Scale third-party or custom virtual appliances transparently |
| AWS WAF | Supported HTTP applications | Request-aware | Block or allow web requests using Layer 7 rules |
| AWS Shield | Supported public resources | DDoS protection | Shield Standard baseline; Shield Advanced adds expanded protections and response capabilities |
| Route 53 Resolver DNS Firewall | VPC DNS queries | DNS policy | Allow, block, or alert on domains resolved through VPC Resolver |

### Security Group and Network ACL Shortcut

- Security groups contain allow rules only, evaluate all rules together, and
  automatically allow return traffic for an allowed stateful flow.
- Network ACLs support allow and deny, evaluate lowest rule number first, and
  require explicit return-path rules, including ephemeral ports where needed.
- Default security groups allow inbound only from the same security group and
  allow all outbound. Default network ACLs allow all; custom network ACLs deny
  all until rules are added.

### Centralized Inspection Rules

Stateful inspection requires symmetry. For Transit Gateway designs, use
deliberate attachment route tables, inspection routes, Availability Zone-aware
placement, and appliance mode where required so both directions of a flow use
the same stateful appliance path. Prevent bypass routes rather than assuming
that creating a firewall automatically inserts it.

## 8. Network Operations and Troubleshooting

| Question | Tool | What it proves | What it does not prove |
|---|---|---|---|
| What IP flows were accepted or rejected? | VPC Flow Logs | Metadata records for flows at a VPC, subnet, or ENI scope | Packet payloads or complete application behavior |
| Should configuration permit this path? | Reachability Analyzer | Static, hop-by-hop configuration analysis and a blocking component | It does not send packets or inspect the live data plane |
| What packets does an appliance need to inspect? | Traffic Mirroring | Copies selected ENI traffic to an out-of-band monitoring target | It does not place the appliance inline or block the original flow |
| Are resources and links healthy over time? | CloudWatch metrics, logs, alarms, and Network Manager where applicable | Operational trend and event evidence | Correct architecture without explicit path tests |

### Troubleshooting Order

1. Confirm source and destination addresses, protocol, and ports.
2. Confirm DNS returns the intended address; then separate DNS from reachability.
3. Check the most-specific forward route and the return route.
4. Check security groups, then both directions of network ACL rules.
5. Check endpoint policy, resource policy, and identity policy for private API
   access.
6. Check firewall state, route symmetry, target health, and Availability Zone
   placement.
7. Use Reachability Analyzer for supported configuration paths and Flow Logs
   for observed metadata. Use Traffic Mirroring only when packet inspection is
   actually required.

Exam trap: a timeout is not automatically a security-group problem. DNS,
routes, stateless return rules, appliance symmetry, endpoint policy, target
health, and application listeners are separate possible failure boundaries.

## 9. Resilience and Cost Checks

| Design | Resilience check | Cost check |
|---|---|---|
| NAT Gateway | One per active AZ when AZ independence is required; route clients locally | Gateway-hours, processed bytes, cross-AZ transfer, and removable S3/DynamoDB traffic |
| Interface endpoints | Select enough AZs for the availability requirement | Endpoint-hours multiply by service and AZ, plus processed data |
| Transit Gateway | Attachment and route-table failure domains; multi-Region needs peering or Cloud WAN | Attachment-hours, processed data, inter-Region transfer |
| Direct Connect | Redundant connections and locations; VPN backup if required | Port-hours, provider/cross-connect, transfer, VPN overlay |
| VPN | Use both tunnels and redundant customer devices for the stated requirement | Connection-hours and transfer; performance remains internet/path dependent unless private-IP VPN over DX is used |
| Load balancers | Enable multiple AZs and keep healthy targets per AZ | Load-balancer hours/capacity units, processed traffic, cross-zone behavior |

Cost questions should follow the traffic path. First remove traffic that does
not need the paid shared path, then compare the remaining hourly, per-AZ,
processing, and transfer charges. Do not assume that private always means free.

## 10. High-Value SAP-C02 Traps

1. **Public subnet:** defined by its route to an Internet Gateway, not by its
   name or only by the presence of a public address.
2. **Most-specific route:** beats a default route, so an endpoint or private
   route can win even when `0.0.0.0/0` exists.
3. **IPv6 egress:** use an egress-only Internet Gateway for outbound-only IPv6;
   NAT64/DNS64 is for reaching IPv4-only destinations.
4. **Direct Connect encryption:** dedicated connectivity is not IPsec by
   default.
5. **Two VPN tunnels:** one tunnel is not the intended resilient design.
6. **Peering:** is not transitive and does not solve overlapping CIDRs.
7. **PrivateLink:** exposes services, not arbitrary bidirectional VPC routing.
8. **VPC Lattice versus TGW:** Lattice publishes and authorizes application
   services; TGW supplies general Layer 3 routing between attached networks.
9. **TGW versus Cloud WAN:** TGW is a regional hub; Cloud WAN adds managed
   global policy, segmentation, and automation.
10. **ALB versus NLB versus GWLB:** application request, transport connection,
   and appliance insertion are three different jobs.
11. **CloudFront versus Global Accelerator:** content caching versus static
    anycast accelerated network entry.
12. **WAF versus Network Firewall:** web-request control versus routed network
    inspection.
13. **Reachability Analyzer:** configuration analysis, not a packet test.
14. **BGP attribute order:** a more-specific prefix wins before local
    preference, AS_PATH, or MED.
15. **BGP direction:** customer advertisements influence AWS-to-on-premises
    return routing; customer-router policy influences on-premises-to-AWS
    routing.
16. **Active/passive Direct Connect:** use different local-preference
    communities only when the advertised prefix lengths are the same.
17. **BGP session up:** proves adjacency and route exchange, not end-to-end
    reachability or authorization.

## Recall Check

Answer without looking above:

1. What makes a subnet public?
2. Which route wins between `10.0.2.0/24` and `0.0.0.0/0` for destination
   `10.0.2.25`?
3. What are the separate IPv6 uses of an egress-only Internet Gateway and
   NAT64/DNS64?
4. Why should a NAT Gateway normally be in the same AZ as its private-subnet
   clients?
5. When is peering a smaller fit than Transit Gateway?
6. Why does PrivateLink not replace Transit Gateway?
7. What is the difference between a private, transit, and public Direct Connect
   virtual interface?
8. Why might a design run VPN over Direct Connect?
9. Which load balancer fits host/path routing, non-HTTP transport, and appliance
   insertion respectively?
10. When should CloudFront be chosen instead of Global Accelerator?
11. What additional global-network problem does Cloud WAN solve beyond one
    regional Transit Gateway?
12. Which controls are stateful: security groups, network ACLs, and Network
    Firewall?
13. Why can a correctly configured firewall still be bypassed?
14. What is the difference between Flow Logs, Reachability Analyzer, and
    Traffic Mirroring?
15. Why must DNS, routing, and authorization be tested separately?
16. In what order does AWS evaluate prefix length, Direct Connect local
    preference, AS_PATH, and MED for equal candidate classes?
17. Which direction of traffic is influenced by adding a Direct Connect
    local-preference community to a prefix advertised from on premises?
18. How would you advertise the same prefix for active/active versus
    active/passive Direct Connect?
19. Why can a more-specific backup advertisement unexpectedly attract traffic
    even when its AS path is longer?
20. What does SiteLink connect, and what does it not replace?
21. What do a VPC Lattice service network, service association, and VPC
    association each do?
22. Why does VPC Lattice not replace Transit Gateway, and which requirement
    makes VPC Lattice the better answer?
23. With VPC Lattice auth type `AWS_IAM`, which policy layers must allow a
    request?

## Lakehouse Application Boundary

The Energy Data Lakehouse currently needs private AWS service access, governed
VPC routing, and cost-aware network choices. This lesson does not prove a live
need for Direct Connect, VPN, Transit Gateway, Cloud WAN, VPC Lattice, global
ingress, or centralized network appliances. Promote a pattern only when a named
Lakehouse requirement and tracker-authorized evidence gate justify it.

## Official AWS References

- [Route priority in VPC route tables](https://docs.aws.amazon.com/vpc/latest/userguide/route-tables-priority.html)
- [Internet gateways](https://docs.aws.amazon.com/vpc/latest/userguide/VPC_Internet_Gateway.html)
- [DNS64 and NAT64](https://docs.aws.amazon.com/vpc/latest/userguide/nat-gateway-nat64-dns64.html)
- [Gateway endpoints](https://docs.aws.amazon.com/vpc/latest/privatelink/gateway-endpoints.html)
- [AWS PrivateLink concepts](https://docs.aws.amazon.com/vpc/latest/privatelink/concepts.html)
- [AWS Transit Gateway](https://docs.aws.amazon.com/vpc/latest/tgw/what-is-transit-gateway.html)
- [Amazon VPC Lattice](https://docs.aws.amazon.com/vpc-lattice/latest/ug/what-is-vpc-lattice.html)
- [VPC Lattice service network associations](https://docs.aws.amazon.com/vpc-lattice/latest/ug/service-network-associations.html)
- [VPC Lattice auth policies](https://docs.aws.amazon.com/vpc-lattice/latest/ug/auth-policies.html)
- [AWS Cloud WAN](https://docs.aws.amazon.com/network-manager/latest/cloudwan/what-is-cloudwan.html)
- [Direct Connect virtual interfaces](https://docs.aws.amazon.com/directconnect/latest/UserGuide/WorkingWithVirtualInterfaces.html)
- [Direct Connect gateways](https://docs.aws.amazon.com/directconnect/latest/UserGuide/direct-connect-gateways-intro.html)
- [Direct Connect routing policies and BGP communities](https://docs.aws.amazon.com/directconnect/latest/UserGuide/routing-and-bgp.html)
- [Direct Connect virtual interfaces and SiteLink](https://docs.aws.amazon.com/directconnect/latest/UserGuide/WorkingWithVirtualInterfaces.html)
- [Direct Connect quotas](https://docs.aws.amazon.com/directconnect/latest/UserGuide/limits.html)
- [Site-to-Site VPN route priority](https://docs.aws.amazon.com/vpn/latest/s2svpn/vpn-route-priority.html)
- [Transit Gateway routing behaviour](https://docs.aws.amazon.com/vpc/latest/tgw/how-transit-gateways-work.html)
- [AWS Site-to-Site VPN](https://docs.aws.amazon.com/vpn/latest/s2svpn/VPC_VPN.html)
- [Application Load Balancers](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/introduction.html)
- [Network Load Balancers](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/network-load-balancers.html)
- [Gateway Load Balancers](https://docs.aws.amazon.com/elasticloadbalancing/latest/gateway/introduction.html)
- [How CloudFront delivers content](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/HowCloudFrontWorks.html)
- [AWS Global Accelerator](https://docs.aws.amazon.com/global-accelerator/latest/dg/what-is-global-accelerator.html)
- [AWS Network Firewall components](https://docs.aws.amazon.com/network-firewall/latest/developerguide/firewall-components.html)
- [AWS WAF and AWS Shield](https://docs.aws.amazon.com/waf/latest/developerguide/what-is-aws-waf.html)
- [VPC Flow Logs](https://docs.aws.amazon.com/vpc/latest/userguide/flow-logs-basics.html)
- [Reachability Analyzer](https://docs.aws.amazon.com/vpc/latest/userguide/reachability-analyzer.html)
- [Traffic Mirroring](https://docs.aws.amazon.com/vpc/latest/mirroring/what-is-traffic-mirroring.html)
