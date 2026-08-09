# Amazon Route 53 Key SAP-C02 Lessons - 2026-07-15

<!-- markdownlint-disable MD013 -->

**Last revised:** 2026-08-08

## Purpose and Scope

**Document role:** source-backed lesson. Return to the
[Exam-Prep Revision Hub](../README.md) to choose a learn, test, review, or audit
workflow.

This source-backed lesson consolidates the Amazon Route 53 decisions most
likely to matter in SAP-C02 scenarios. It builds on the repository's verified
hybrid-DNS diagram and the learner's completed first wrong-answer review cycle.

This is a documentation-only study artifact. It does not authorize hosted-zone,
record, health-check, Resolver endpoint, DNS Firewall, VPC, VPN, Direct Connect,
Transit Gateway, or other AWS changes.

For the rest of the Networking domain, use the accompanying
[AWS Networking Beyond Route 53: SAP-C02 Key Lessons](aws-networking-sap-c02-key-lessons-20260717.md).
Together, the two lessons cover DNS plus the principal VPC, hybrid-connectivity,
load-balancing, global-ingress, inspection, and troubleshooting decisions.

## How to Revise This Lesson

| Time available | Revision route |
|---|---|
| 10 minutes | Read [The Core Mental Model](#the-core-mental-model), [Routing-Policy Decision Shortcuts](#routing-policy-decision-shortcuts), the [Directional Rule](#directional-rule), and [High-Value SAP-C02 Traps](#high-value-sap-c02-traps). Then answer the [Recall Check](#recall-check). |
| 25 minutes | Add [Public and Private Hosted Zones](#public-and-private-hosted-zones), [Alias Versus CNAME](#alias-versus-cname), [Health Checks and DNS Failover](#health-checks-and-dns-failover), and [Resolver Rule Types](#resolver-rule-types). |
| 45 minutes | Read the full lesson, draw both hybrid-DNS flows without notes, then answer all recall questions before opening supporting artifacts. |
| Full Networking block | Complete this lesson, then use [AWS Networking Beyond Route 53](aws-networking-sap-c02-key-lessons-20260717.md) for the rest of the domain. |

Keep revision and testing separate: stop at the Recall Check, answer on blank
paper or in a blind-attempt document, and only then return to the explanatory
sections to review misses.

## Topic Navigation

| If the scenario says... | Go to |
|---|---|
| Public, private, split-view, delegation, or caching | [DNS Foundations](#dns-foundations) and [Public and Private Hosted Zones](#public-and-private-hosted-zones) |
| Zone apex, alias, CNAME, or record type | [Record Types and Alias Records](#record-types-and-alias-records) |
| Percentage, latency, location, bias, CIDR, or active-passive | [Route 53 Routing Policies](#route-53-routing-policies) |
| Endpoint health, failover, or TTL convergence | [Health Checks and DNS Failover](#health-checks-and-dns-failover) |
| AWS resolves on premises or on premises resolves AWS | [Route 53 VPC Resolver and Hybrid DNS](#route-53-vpc-resolver-and-hybrid-dns) |
| Domain filtering, DNS logs, or DNSSEC | [Resolver DNS Firewall, Logging, and Security](#resolver-dns-firewall-logging-and-security) and [DNSSEC](#dnssec) |

## The Core Mental Model

Route 53 has several related but distinct jobs:

1. **Domain registration** registers a domain name.
2. **Authoritative DNS** uses public or private hosted zones and their records
   to answer queries for namespaces hosted by Route 53.
3. **Traffic routing** applies the routing policy configured on Route 53
   records. When multiple records share the queried name and type, the policy
   determines which eligible record value or alias target Route 53 returns.
4. **Health checking** influences whether a record or alias target remains
   eligible for normal DNS-answer selection.
5. **VPC Resolver** provides recursive DNS inside VPCs and hybrid DNS through
   inbound and outbound endpoints and Resolver rules.
6. **Resolver DNS Firewall** filters DNS queries handled by VPC Resolver; it is
   not a network firewall and does not replace DNS resolution.

Routing policy and health are separate but can work together. The routing
policy selects among candidate records, while health configuration can remove
unhealthy candidates from normal selection.

Exam rule: first identify which Route 53 job the scenario requires. Then identify
the queried namespace, the record name and type, the possible answer targets,
the routing policy, and any health or hybrid-resolution requirements.

### What “record” Means in Route 53

A Route 53 record is a DNS resource record set inside a hosted zone. It
identifies a DNS name and type and contains either one or more record values or
an alias target.

Depending on the record, it can also include:

- a TTL;
- a routing policy;
- routing-policy-specific attributes, such as weight, Region, failover role,
  or set identifier; and
- health-check or target-health configuration.

With non-simple routing policies, multiple record entries can have the same
name and type but point to different resources. A set identifier distinguishes
those entries.

For example:

| Name | Type | Target | Policy | Weight |
|---|---|---|---|---:|
| `api.example.com` | Alias `A` | London ALB | Weighted | 80 |
| `api.example.com` | Alias `A` | Frankfurt ALB | Weighted | 20 |

Route 53 matches the queried name and type, evaluates the routing policy and
health configuration, and returns an eligible DNS answer.

## DNS Foundations

| Concept | SAP-C02 meaning | Common trap |
|---|---|---|
| Authoritative DNS | The hosted zone contains records and Route 53 answers for that namespace. | Confusing an authoritative name server with the recursive resolver used by clients. |
| Recursive resolver | Finds an answer for a client, using cache or by querying DNS authorities. VPC Resolver provides this function inside a VPC. | Assuming a hosted zone itself forwards arbitrary hybrid queries. |
| Delegation | The parent zone points to the child zone's authoritative name servers using NS records. | Creating a child hosted zone but not delegating the subdomain from its parent. |
| TTL | Controls how long recursive resolvers cache a record answer. | Expecting an immediate global change while an older answer remains cached. |
| Negative caching | Resolvers can also cache negative answers according to the zone's SOA settings. | Troubleshooting only positive-record TTLs after a name previously returned no answer. |

Shorter TTLs speed planned changes and failover convergence but increase query
volume. Longer TTLs improve cache efficiency but extend the time that clients
may use an old answer.

## Public and Private Hosted Zones

| Hosted zone | Reachability | Use when | Key design check |
|---|---|---|---|
| Public hosted zone | Answers are available through public DNS. The target can still be public or otherwise access-controlled. | Internet clients must resolve the name. | DNS visibility does not itself make the target reachable or secure. |
| Private hosted zone | Answers are available through VPC Resolver to associated VPCs, and through an inbound Resolver endpoint in a hybrid design. | Internal workloads need private names. | Associate every intended VPC; design cross-account association explicitly. |
| Split-view DNS | The same name exists in public and private hosted zones with different answers. | Internal and external clients need different views of a namespace. | Test both resolution paths and avoid assuming the public record is a fallback for a matching private namespace. |

Exam trap: a private hosted zone is a DNS visibility boundary, not a network
connectivity service. Routes, security controls, and a hybrid transport path
must still permit the client to reach the returned address.

## Record Types and Alias Records

| Record | Purpose | SAP-C02 cue |
|---|---|---|
| `A` | Maps a name to an IPv4 address. | IPv4 endpoint. |
| `AAAA` | Maps a name to an IPv6 address. | IPv6 endpoint. |
| `CNAME` | Maps one name to another name. | Use below the zone apex; it cannot be created at the zone apex. |
| Route 53 alias | Route 53 extension that maps a name to a supported AWS resource or another Route 53 record. | Can be used at the zone apex and tracks supported AWS resource address changes. |
| `MX` | Identifies mail servers. | Email routing. |
| `TXT` | Stores text values. | Domain verification, email-policy, or other text-based proof. |
| `NS` | Delegates a zone to authoritative name servers. | Parent-to-child delegation. |
| `SOA` | Contains authoritative-zone metadata and negative-caching values. | Zone control and caching behavior. |
| `PTR` | Reverse lookup from an address to a name. | Reverse-DNS requirement. |
| `CAA` | Restricts which certificate authorities may issue certificates for a domain. | Certificate-issuance governance. |

### Alias Versus CNAME

- Prefer a Route 53 alias when the target is a supported AWS resource such as
  an Elastic Load Balancing load balancer, CloudFront distribution, or
  configured S3 website endpoint.
- An alias can be used at the zone apex; a CNAME cannot.
- An alias to an AWS resource uses the resource's TTL rather than a TTL set on
  the alias record.
- `Evaluate Target Health` makes Route 53 evaluate the health of the supported
  AWS resource or Route 53 record branch referenced by the alias, according to
  the target type's health-evaluation rules.
- A CNAME can point to a wider range of DNS names but adds another DNS lookup
  and is not valid at the apex.

Exam trap: an alias is still DNS. It does not grant application access, create
private connectivity, or replace a load balancer.

## Route 53 Routing Policies

| Policy | Choose it when | Deciding input | Key trap |
|---|---|---|---|
| Simple | One resource, or one record containing a basic unordered set of values, is sufficient. | No Route 53 traffic-steering calculation. | Multiple values may be returned without health-aware selection; this is not failover or load balancing. |
| Weighted | Traffic should be divided by configured proportions. | Relative record weights. | Weights are relative, not percentages; use health checks if unhealthy endpoints must be excluded. |
| Latency | Users should reach the AWS Region that gives them the best measured latency. | AWS latency measurements between users and Regions. | It does not mean the geographically closest Region. |
| Failover | Active-passive routing is required. | Primary/secondary role plus health. | DNS failover is affected by caching; it is not an instantaneous connection-level failover mechanism. |
| Geolocation | Content or endpoints are selected by the user's geographic location. | User location such as continent, country, or US state. | Create a default record for unmatched or unmapped locations. |
| Geoproximity | Route according to geographic distance between users and resources, with optional bias to expand or contract each resource's geographic catchment area. | User and resource locations plus optional bias. | Bias changes how much traffic a resource attracts; this is different from fixed geolocation rules. |
| IP-based | Known client CIDR ranges should map to chosen endpoints. | Reusable CIDR collections based on source IP. | This is operator-supplied IP knowledge, not Route 53 latency or geography data; it is not supported in private hosted zones. |
| Multivalue answer | Route 53 should return up to eight eligible healthy record values, with the client or resolver subsequently selecting an address to use. | Record health and random selection. | It is not a substitute for an Elastic Load Balancing load balancer. |

### Routing-Policy Decision Shortcuts

- Gradual release or blue/green split: **weighted**.
- Best measured regional response: **latency**.
- Primary then disaster-recovery secondary: **failover**.
- Compliance or localization based on the user's country: **geolocation**.
- Geographic proximity with adjustable traffic-shifting bias: **geoproximity**.
- Known ISP/client CIDRs must use chosen endpoints: **IP-based**.
- Several healthy IP answers without a load balancer: **multivalue answer**.

## Health Checks and DNS Failover

Route 53 can monitor:

- a public endpoint using HTTP, HTTPS, or TCP;
- other health checks through a calculated health check; or
- a CloudWatch alarm's data stream.

For a non-alias record, associate an explicit Route 53 health check. For an
alias record to a supported AWS resource or another record branch, normally use
`Evaluate Target Health = Yes`.

Important behavior:

- Health checks run periodically; they are not performed when each DNS query
  arrives.
- Records without health checks are treated as healthy.
- If all applicable records are unhealthy, Route 53 uses last-resort behavior
  and treats the records as eligible rather than returning no answer solely
  because every health check failed.
- In a failover pair, Route 53 returns the healthy primary; when the primary is
  unhealthy and the secondary is healthy, it returns the secondary.
- TTL and client-side caching affect how quickly users observe a changed DNS
  answer.
- Health-check reachability must match the design. A standard public Route 53
  endpoint health check cannot directly probe an endpoint that is reachable
  only through private VPC addressing.

Exam trap: health checking affects DNS answers. It does not move data,
replicate state, or prove that the application can safely fail over.

### Amazon Application Recovery Controller Routing Controls

An ARC routing control is a highly available operator-controlled on/off switch,
not an endpoint monitor. The control is hosted on an ARC cluster. Changing its
state through one of the cluster's Regional data-plane endpoints changes the
state of an associated Route 53 health check, which then makes the configured
DNS record eligible or ineligible.

```text
operator or automation
    -> ARC cluster data-plane endpoint
    -> routing-control state
    -> Route 53 health-check state
    -> DNS failover record selection
    -> new client connections
```

The highly available ARC data plane makes the **control operation** dependable;
it does not bypass DNS. Route 53 still returns DNS answers, so resolver and
client caching, record TTLs, connection reuse, and long-lived connections can
delay complete traffic movement. For a requirement that truly demands static
anycast IPs and endpoint failover independent of DNS-cache expiry, compare AWS
Global Accelerator rather than claiming ARC removes the DNS boundary.

Exam trap: “ARC routing control” correctly identifies the managed failover
switch, but “without relying on DNS caching expiration” is a false premise for
ARC routing controls.

## Route 53 VPC Resolver and Hybrid DNS

### Directional Rule

| Requirement | Required DNS component | Direction |
|---|---|---|
| On-premises clients must resolve AWS private names. | Inbound Resolver endpoint plus an on-premises conditional forwarder. | Queries enter the VPC Resolver. |
| AWS workloads must resolve on-premises private names. | Outbound Resolver endpoint plus a Resolver forwarding rule for the on-premises suffix. | Queries leave the VPC Resolver. |
| Both sides must resolve private names. | Design both flows independently. | Inbound and outbound endpoints are separate roles. |

```text
On premises -> conditional forwarder -> inbound endpoint -> VPC Resolver

AWS workload -> VPC Resolver rule -> outbound endpoint -> on-premises DNS
```

The DNS path still requires network transport such as Site-to-Site VPN or
Direct Connect. Transit Gateway can provide scalable routing between attached
networks, but none of these transport services performs DNS forwarding.

### Resolver Rule Types

VPC Resolver uses four rule types, although customers directly configure only
forward, system, and delegate rules. Recursive rules are created and managed by
Resolver.

| Rule type | Purpose |
|---|---|
| Forward | Sends matching suffix queries through an outbound endpoint to specified DNS resolver IP addresses. |
| System | Overrides a broader forwarding rule and makes VPC Resolver resolve the matching name using its normal AWS behavior. |
| Recursive | Built-in recursive behavior used by VPC Resolver when no custom or automatically defined rule takes precedence; customers do not create this rule type. |
| Delegate | Uses an outbound endpoint to reach delegated authoritative name servers when returned NS records match the configured delegation. |

Resolver selects the most specific matching domain rule. For example, a rule
for `dev.corp.example` takes precedence over a rule for `corp.example`.
Forwarding rules can be associated with multiple VPCs and shared across
accounts with AWS Resource Access Manager.

### Endpoint Design Checks

- Configure at least two endpoint IP addresses in different Availability Zones.
  Each IP is a separate ENI and DNS target; this is the minimum resilient
  Resolver-endpoint shape, not merely an optional optimisation.
- Security groups and network controls must permit both UDP and TCP DNS traffic
  on port 53 unless a deliberate alternative port is configured.
- Size and monitor endpoint query capacity.
- Keep namespace ownership and conditional-forwarder ownership explicit.
- Avoid broad forwarding rules that unintentionally capture AWS service names.
- Prevent forwarding loops by making each side's authoritative responsibility
  unambiguous.

Exam trap: an inbound endpoint is for queries entering AWS; an outbound
endpoint is for queries leaving the VPC toward another DNS system.

## Resolver DNS Firewall, Logging, and Security

Resolver DNS Firewall evaluates DNS queries that pass through VPC Resolver.
Reusable rule groups can allow, block, or alert on matching domains and can use
AWS-managed or custom domain lists. Advanced protections can detect patterns
such as DNS tunnelling or domain-generation algorithms.

Do not confuse DNS Firewall with AWS Network Firewall:

- DNS Firewall controls DNS queries and domain-level resolution behavior.
- Network Firewall filters network and application traffic on routed paths.
- Blocking a name does not block direct traffic to the resolved IP address.

Operational evidence can include:

- public hosted-zone query logging;
- VPC Resolver query logging;
- DNS Firewall logs and metrics;
- CloudTrail for Route 53 control-plane API activity; and
- CloudWatch alarms for health checks and DNSSEC problems.

## DNSSEC

DNSSEC provides authenticity and integrity for DNS answers; it does not encrypt
DNS traffic.

- Route 53 can sign a public hosted zone.
- Route 53 uses a key-signing key backed by an asymmetric customer managed AWS
  KMS key; Route 53 manages the zone-signing key.
- The parent zone needs the delegation signer record to establish the chain of
  trust.
- VPC Resolver can validate public signed names when DNSSEC validation is
  enabled for a VPC.
- If VPC Resolver forwards a query to another recursive resolver, that other
  resolver is responsible for DNSSEC validation.
- DNSSEC misconfiguration can make a zone unresolvable, so alarms, staged
  enablement, and rollback planning matter.

Exam trap: DNSSEC proves that the answer is authentic and unmodified. Use TLS
or another application/network security mechanism when confidentiality is
required.

## High-Value SAP-C02 Traps

1. **DNS versus transport:** Resolver endpoints solve name resolution; VPN,
   Direct Connect, and Transit Gateway carry packets.
2. **Inbound versus outbound:** name the query's starting side before choosing
   the endpoint.
3. **Alias versus CNAME:** use alias at the zone apex for supported targets.
4. **Latency versus geography:** latency uses measured network performance;
   geolocation uses the user's location; geoproximity uses resource/user
   locations and optional bias.
5. **Multivalue versus load balancer:** multivalue returns several healthy DNS
   answers but is not a load-balancing proxy.
6. **Health versus recovery:** DNS health checks steer answers; they do not
   replicate or repair application state.
7. **Private hosted zone versus private connectivity:** private DNS visibility
   does not create a route to the returned address.
8. **DNSSEC versus encryption:** DNSSEC authenticates answers; it does not hide
   queries or application data.
9. **TTL versus instant failover:** cached answers can outlive a routing change.
10. **ARC versus DNS-cache bypass:** ARC controls Route 53 health-check state;
    the resulting traffic shift is still DNS-based.

## Recall Check

Answer these without looking above:

1. Which routing policy supports a controlled 90/10 deployment split?
2. Which policy selects endpoints from explicit client CIDR mappings?
3. What is the difference between geolocation and geoproximity?
4. Why can an alias be used at the zone apex while a CNAME cannot?
5. Which endpoint handles queries from on premises into AWS private DNS?
6. Which endpoint and rule handle queries from AWS to an on-premises suffix?
7. What does `Evaluate Target Health` do?
8. Why is multivalue answer routing not a replacement for a load balancer?
9. What are the separate jobs of Direct Connect and Route 53 Resolver?
10. What security property does DNSSEC provide, and what does it not provide?
11. Why do ARC routing-control cluster endpoints improve control-plane
    reliability without eliminating DNS TTL and connection-reuse effects?

## Lakehouse Application Boundary

The current Energy Data Lakehouse has no evidenced requirement for public Route
53 traffic steering or hybrid private DNS. Use this lesson as exam and future
architecture evidence only. Revisit implementation if a named private-domain,
multi-Region traffic, failover, or hybrid-connectivity requirement appears and
the tracker authorizes a bounded change.

## Official AWS References

- [Amazon Route 53 concepts](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/route-53-concepts.html)
- [Choosing between alias and non-alias records](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/resource-record-sets-choosing-alias-non-alias.html)
- [Supported DNS record types](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/ResourceRecordTypes.html)
- [Working with private hosted zones](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/hosted-zones-private.html)
- [Route 53 VPC Resolver](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/resolver.html)
- [Resolver endpoint high availability](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/best-practices-resolver-endpoint-high-availability.html)
- [Managing Resolver forwarding rules](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/resolver-rules-managing.html)
- [Route 53 health-check record selection](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/health-checks-how-route-53-chooses-records.html)
- [Amazon Application Recovery Controller routing controls](https://docs.aws.amazon.com/r53recovery/latest/dg/routing-control.html)
- [ARC routing-control traffic-shift behaviour](https://docs.aws.amazon.com/r53recovery/latest/dg/routing-control.about.html)
- [Resolver DNS Firewall](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/resolver-dns-firewall-overview.html)
- [DNSSEC signing](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/dns-configuring-dnssec.html)
- [DNSSEC validation in VPC Resolver](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/resolver-dnssec-validation.html)
