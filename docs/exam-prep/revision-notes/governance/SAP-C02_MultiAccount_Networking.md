# SAP-C02 Study Guide: Multi-Account Networking

<!-- markdownlint-disable MD022 MD032 MD033 MD036 MD040 MD060 -->

*Companion to the Organizations / Identity Center / IAM guide.*
*Prepared for: AWS Infrastructure Solution Architect (Beginner) with 20+ years in Financial Services and Energy*

---

## Legend — Acronyms and Terms Used in This Document

### Connectivity hubs and gateways

| Acronym | Meaning | Quick context |
| --- | --- | --- |
| **ALB** | Application Load Balancer | Layer-7 load balancer (HTTP/HTTPS). |
| **Cloud WAN** | (Service name, not an acronym) | AWS's global, policy-driven multi-Region WAN. |
| **DX** | Direct Connect | Dedicated private fibre into AWS (1/10/100 Gbps). |
| **DXGW** | Direct Connect Gateway | Global construct linking a DX to VGWs/TGWs in any Region. |
| **GWLB** | Gateway Load Balancer | Transparent L3 load balancer for inserting 3rd-party security appliances. |
| **GWLBe** | GWLB endpoint | Consumer-side VPC endpoint that sends traffic to a GWLB. |
| **IGW** | Internet Gateway | VPC component that enables internet access. |
| **NAT GW** | Network Address Translation Gateway | Managed outbound-only internet egress for private subnets. |
| **NLB** | Network Load Balancer | Layer-4 load balancer (TCP/UDP/TLS). |
| **TGW** | Transit Gateway | Regional hub-and-spoke router for VPCs, VPNs, and DX. |
| **VGW** | Virtual Private Gateway | Legacy single-VPC VPN/DX termination point. |
| **VIF** | Virtual Interface | Logical interface on a DX connection. Three kinds: Private, Public, Transit. |
| **VPC** | Virtual Private Cloud | Logically isolated network in AWS. |
| **VPN** | Virtual Private Network | Encrypted tunnel; AWS uses IPsec for Site-to-Site VPN. |

### Networking protocols and concepts

| Acronym | Meaning | Quick context |
| --- | --- | --- |
| **ASN** | Autonomous System Number | Identifier for a BGP-speaking network. |
| **AZ** | Availability Zone | An isolated data-centre cluster within a Region. |
| **BGP** | Border Gateway Protocol | The routing protocol used by DX and BGP-enabled VPNs. |
| **CIDR** | Classless Inter-Domain Routing | The `10.0.0.0/16` style IP-block notation. |
| **DMVPN** | Dynamic Multipoint VPN | Cisco hub-and-spoke VPN tech, referenced as on-prem analogue. |
| **ECMP** | Equal-Cost Multi-Path | Load-balancing across multiple equal-cost paths (e.g., VPN tunnels). |
| **ENI** | Elastic Network Interface | A virtual network card attached to AWS resources. |
| **GENEVE** | Generic Network Virtualization Encapsulation | Encapsulation used by GWLB for appliance insertion. |
| **GRE** | Generic Routing Encapsulation | Tunnelling protocol used by TGW Connect attachments. |
| **IPsec** | Internet Protocol Security | Encryption protocol used by Site-to-Site VPN. |
| **ISP** | Internet Service Provider | Carrier providing internet transit. |
| **MACsec** | Media Access Control Security | IEEE 802.1AE Layer-2 encryption on 10/100 Gbps DX connections. |
| **MPLS / PE** | Multiprotocol Label Switching / Provider Edge | Carrier WAN technology referenced as on-prem analogue. |
| **NACL** | Network Access Control List | Stateless subnet-level packet filter. |
| **SD-WAN** | Software-Defined Wide Area Network | Modern WAN overlay typically integrated via TGW Connect or Cloud WAN. |
| **SG** | Security Group | Stateful ENI-level packet filter. |
| **VRF** | Virtual Routing and Forwarding | Independent routing table within a router; conceptual analogue of TGW route tables and Cloud WAN segments. |

### DNS

| Acronym | Meaning | Quick context |
| --- | --- | --- |
| **BIND** | Berkeley Internet Name Domain | Open-source on-prem DNS server software. |
| **DGA** | Domain Generation Algorithm | Malware technique generating many random domains; detected by Route 53 DNS Firewall. |
| **DNS** | Domain Name System | Name resolution. |
| **DNS Firewall** | Route 53 Resolver DNS Firewall | Allow/block-list filtering for DNS queries from VPCs. |
| **FQDN** | Fully Qualified Domain Name | A complete domain name (e.g., `host1.aws.corp.local`). |
| **PHZ** | Private Hosted Zone | A Route 53 zone visible only inside associated VPCs. |
| **QPS** | Queries Per Second | DNS throughput metric. |

### PrivateLink and endpoints

| Acronym | Meaning | Quick context |
| --- | --- | --- |
| **Gateway endpoint** | (Endpoint type) | Free route-table-based endpoint, S3/DynamoDB only. |
| **Interface endpoint** | (Endpoint type) | PrivateLink ENI-based endpoint, most AWS services. |
| **PrivateLink** | (Service name) | AWS's private-IP service exposure technology. |
| **Resource endpoint** | (Endpoint type, Nov 2024) | PrivateLink to a specific resource without an NLB. |
| **vpce** | VPC endpoint identifier prefix | Used in ARNs and `aws:SourceVpce` condition key. |
| **XRPL** | Cross-Region PrivateLink | Interface endpoints targeting services in other Regions (GA Nov 2025). |

### Sharing, security, and inspection

| Acronym | Meaning | Quick context |
| --- | --- | --- |
| **ACM** | AWS Certificate Manager | Managed TLS certificate service. |
| **IPS** | Intrusion Prevention System | Inline threat detection and blocking. |
| **L2 / L3 / L4 / L7** | OSI Layers (Data Link / Network / Transport / Application) | Used to characterise where a control operates. |
| **Network Firewall** | AWS Network Firewall | Managed stateful firewall (Suricata-compatible). |
| **NGFW** | Next-Generation Firewall | Modern firewall with app awareness, IPS, TLS inspection. |
| **RAM** | Resource Access Manager | Cross-account resource-sharing service. |
| **TLS / mTLS** | Transport Layer Security / mutual TLS | Encryption in transit; mTLS authenticates both peers. |
| **WAF** | Web Application Firewall | Layer-7 protection for web apps. |

### Compute, storage, and platform services referenced

| Acronym | Meaning | Quick context |
| --- | --- | --- |
| **EC2** | Elastic Compute Cloud | AWS virtual machines. |
| **ECR** | Elastic Container Registry | Private container image registry. |
| **ECS / EKS** | Elastic Container Service / Elastic Kubernetes Service | Container orchestration platforms. |
| **Fargate** | (Service name) | Serverless compute for ECS/EKS. |
| **KMS** | Key Management Service | Managed encryption keys. |
| **NSX Edge** | VMware NSX Edge | VMware Cloud on AWS edge router. |
| **RDS** | Relational Database Service | Managed relational databases. |
| **S3** | Simple Storage Service | Object storage. |
| **SDDC** | Software-Defined Data Center | VMware-managed environment unit on VMC on AWS. |
| **Secrets Manager / SSM** | AWS Secrets Manager / Systems Manager | Secret storage; ops automation. |
| **STS** | Security Token Service | Issues temporary credentials. |
| **VMC** | VMware Cloud (on AWS) | Managed VMware service on AWS. |

### Resilience, scaling, and DR

| Acronym | Meaning | Quick context |
| --- | --- | --- |
| **API** | Application Programming Interface | Programmatic interface. |
| **DR** | Disaster Recovery | Recovery from larger-scale outages (Region, site). |
| **HA** | High Availability | Architecture that survives single-point failures. |
| **IP** | Internet Protocol | Layer-3 addressing. |
| **JSON** | JavaScript Object Notation | The format Cloud WAN policy documents (and AWS policies) use. |
| **SaaS** | Software as a Service | Hosted application delivery model. |
| **TCP / UDP** | Transmission Control Protocol / User Datagram Protocol | Layer-4 transport protocols. |

---

## 1. Why This Is the Heaviest Single Topic on SAP-C02

Multi-account networking touches **all four exam domains**:

- **Domain 1 (26%)** — explicit task: *"Architect network connectivity strategies"* and *"Design a multi-account AWS environment"*.
- **Domain 2 (29%)** — new-solution designs almost always include a connectivity choice.
- **Domain 3 (25%)** — improvements often involve replacing VPC peering with TGW, adding inspection, or migrating to Cloud WAN.
- **Domain 4 (20%)** — migration scenarios require hybrid connectivity (DX, VPN) sized correctly.

Realistic estimate: **15–20% of questions hinge on getting a networking choice right**. The good news for your background — financial services and energy are exactly the verticals where the *Networking Account* + *Transit Gateway* + *Direct Connect* + *centralised inspection* pattern was forged. The exam tests the patterns you'd defend in any enterprise architecture review.

---

## 2. The Reference Architecture: The Networking Account Pattern

This is the mental picture every SAP-C02 networking question is testing against. Once you see it, every multi-account question becomes "which piece of this diagram are they asking about?"

```
              On-prem DC #1            On-prem DC #2
                  │                         │
              Direct Connect            Direct Connect
                  │                         │   (redundant location)
                  ▼                         ▼
        ┌─────────────────────────────────────────────┐
        │   Direct Connect Gateway (global, free)     │
        └─────────────────────────────────────────────┘
                            │
                  (Transit VIF / Private VIF)
                            │
        ┌───────────────────┴──────────────────┐
        │      Network Services Account        │
        │  ┌────────────────────────────────┐  │
        │  │  Transit Gateway (hub)         │  │
        │  │  + Route tables (segmentation) │  │
        │  └────────────────────────────────┘  │
        │  ┌────────────────────────────────┐  │
        │  │  Shared Services VPC:          │  │
        │  │  • Route 53 Resolver in/out    │  │
        │  │  • PHZs (or Route 53 Profiles) │  │
        │  │  • Centralised PrivateLink     │  │
        │  │    interface endpoints         │  │
        │  └────────────────────────────────┘  │
        │  ┌────────────────────────────────┐  │
        │  │  Inspection VPC (optional):    │  │
        │  │  • AWS Network Firewall  OR    │  │
        │  │  • GWLB + 3rd-party appliances │  │
        │  └────────────────────────────────┘  │
        │  ┌────────────────────────────────┐  │
        │  │  Egress VPC: NAT GW, IGW       │  │
        │  └────────────────────────────────┘  │
        │                                      │
        │  TGW shared to OUs via AWS RAM       │
        └──────────────────────────────────────┘
              │                  │
       ┌──────┴──┐         ┌─────┴────┐
       ▼         ▼         ▼          ▼
   Prod-VPC  Prod-VPC  NonProd-VPC  Sandbox-VPC
   (acct A)  (acct B)  (acct C)    (acct D)
```

**Key principle:** all cross-cutting network resources (TGW, DX, DXGW, DNS, central endpoints, inspection) live in a **Network Services / Networking account** under the Infrastructure OU, then are shared out to spoke accounts via **AWS RAM** and **Route 53 PHZ associations**.

---

## 3. The Building Blocks (Deep Dive)

### 3.1 VPC Peering — Still Useful, But Bounded

- **One-to-one, non-transitive.** A↔B and B↔C does **not** give you A↔C.
- **No transit through a peering VPC**; even if a route exists, it won't forward.
- Free for traffic in same Region (data transfer charges still apply); inter-Region peering carries cross-Region data transfer.
- **When still right on the exam:** 2–3 VPCs, no transitivity needed, cost-sensitive. The moment a fourth VPC, hybrid connectivity, or transitive routing appears, the right answer pivots to TGW or Cloud WAN.

### 3.2 AWS Transit Gateway (TGW) — The Regional Hub

- **Regional, hub-and-spoke.** Single hub interconnecting many VPCs, VPNs, Direct Connect Gateways, and other TGWs (via peering).
- **Attachments:**
  - **VPC attachment** — ENIs in one subnet per AZ.
  - **VPN attachment** — IPsec, supports BGP, supports **ECMP** for bandwidth aggregation.
  - **Direct Connect Gateway attachment** — via a transit VIF.
  - **Peering attachment** — same-Region or inter-Region TGW peering.
  - **Connect attachment** — GRE + BGP over an existing VPC or DX attachment, for SD-WAN integration (up to 20 Gbps per Connect attachment).
- **TGW route tables** — *the* mechanism for segmentation:
  - Each attachment is **associated** with one route table.
  - Routes are **propagated** (dynamically learned from the attachment) or **static**.
  - You can build:
    - **Full-mesh** — one shared RT, everything reaches everything.
    - **Segmented** — separate Prod, NonProd, Shared-Services RTs to enforce traffic boundaries (e.g., NonProd cannot reach Prod).
    - **Centralised inspection** — spoke RT default-routes to an inspection VPC.
- **Appliance mode** — required when traffic between two VPCs traverses a stateful appliance in a third VPC across multiple AZs; ensures flow symmetry (request and response use the same AZ ENI). Forgetting this is a classic exam trap.
- **Sharing:** create the TGW in the Network Services account; share via **AWS RAM** to OUs/accounts; spoke accounts then create their own VPC attachments to it.
- **Cost levers:** charged per attachment-hour and per-GB processed — **both** sides of the hub. Centralising endpoints behind TGW saves PrivateLink ENI cost but adds TGW data-processing cost; do the math.

### 3.3 AWS Cloud WAN — The Global, Policy-Driven WAN

Newer (GA late 2022, increasingly tested). Think *"managed multi-region multi-segment WAN with a JSON policy"*.

- **Core network** spans **multiple Regions**; AWS deploys a **Core Network Edge (CNE)** in each Region you list.
- **Segments** ≈ VRFs — Prod, NonProd, Shared — declared in the policy document.
- **Attachments:** VPC, Site-to-Site VPN, Connect (SD-WAN), and **TGW route-table attachments** (Cloud WAN can federate existing TGWs).
- **Direct Connect** integrates via TGW peering (Cloud WAN doesn't yet natively terminate DX).
- **Policy document (JSON)** centralises segment definitions, sharing actions, attachment policies (auto-mapping by tag), and Network Function Group routing for inspection.

**TGW vs Cloud WAN — when which?**

| Need | Best fit |
|---|---|
| Single Region, many VPCs | **TGW** |
| Multi-Region global mesh | **Cloud WAN** (or TGW peering if simple) |
| Policy-as-code, tag-based attachment | **Cloud WAN** |
| Lowest cost, simplest scope | **TGW** |
| Existing complex TGW, want incremental global expansion | **TGW + Cloud WAN federation** |

### 3.4 AWS Resource Access Manager (RAM)

- The sharing fabric for cross-account resources within an Organization.
- **Shareable highlights** (memorise the big ones):
  - **Transit Gateway** — share the TGW so spoke accounts can attach.
  - **VPC subnets (Shared VPC)** — *the* pattern where the owner account hosts the VPC and participant accounts run EC2/Lambda/RDS in it. Reduces VPC sprawl, but participants can't modify VPC-level resources.
  - **Route 53 Resolver rules** — sharing forwarding rules across accounts.
  - **License Manager configurations**, **Aurora DB clusters**, **CodeBuild project/report groups**, **AWS PCA**, **Outposts**, **Resource Configurations (for Resource Endpoints)**.
- Sharing inside an Org can auto-accept (no invitation handshake), enabled at the Org level.

### 3.5 Hybrid Connectivity

#### Site-to-Site VPN
- IPsec over the public internet (or over a DX public VIF / private IP VPN over transit VIF).
- Terminates on a **Virtual Private Gateway (VGW)** (single VPC) or on a **Transit Gateway** (many VPCs).
- TGW-terminated VPN supports **ECMP** — up to ~4× 1.25 Gbps tunnels = ~5 Gbps aggregate; same TGW VPN attachment can scale further.
- **Exam default:** terminate on TGW unless the scenario is a one-VPC test/dev case.

#### AWS Direct Connect (DX)
- Dedicated physical connection from a DX Location: 1, 10, 100 Gbps (and hosted sub-rate options).
- **Three Virtual Interfaces (VIFs):**
  - **Private VIF** — to a single VGW or DXGW → up to one VPC per VGW.
  - **Public VIF** — to AWS public IP space (e.g., S3 public endpoint, VPN public endpoints).
  - **Transit VIF** — to a Direct Connect Gateway associated with a Transit Gateway → many VPCs across many accounts/Regions.
- **Direct Connect Gateway (DXGW)** — global construct, free. Lets one DX connection reach VPCs/TGWs in **multiple Regions**.
  - DXGW can be associated with **VGWs (one or more)** OR **a Transit Gateway** — not both at the same DXGW. (When you need both, use two DXGWs — common exam answer.)
- **MACsec** for L2 encryption on 10/100 Gbps dedicated connections.
- **Private IP VPN over Transit VIF** — IPsec encryption over DX with private IPs; the answer when you need DX bandwidth *and* end-to-end encryption *and* don't want public IPs.
- **Resilience tiers (AWS published model):**
  - Development & test — one connection, one location.
  - High resilience — two connections at different DX locations.
  - Maximum resilience — multiple connections, multiple locations, separate devices.

#### Combined / failover patterns
- **DX primary + Site-to-Site VPN backup** — both attached to the same TGW. BGP local-pref or `AS_PATH` prepending steers traffic; failover is automatic. This is the standard enterprise answer.
- **Cross-Region**: one DX in eu-west-1 with DXGW reaching VPCs in eu-west-2 (active/passive DR).

### 3.6 Route 53 Resolver — Hybrid DNS

The single most misunderstood SAP-C02 topic. Get this clear and you'll pick up easy points.

**The two endpoints (both live in a Shared Services VPC):**

| Endpoint | Direction | Purpose |
| --- | --- | --- |
| **Inbound endpoint** | On-prem → AWS | Lets on-prem DNS forward queries for AWS-side names (e.g., `*.aws.corp.local`) to AWS so they can be resolved against private hosted zones. |
| **Outbound endpoint** | AWS → On-prem | Lets VPC workloads resolve on-prem names (e.g., `*.corp.local`) by forwarding to the on-prem DNS via a **Resolver rule**. |

**Resolver forwarding rules** — created in the Network Services account; each rule says "for queries matching domain X, forward to these IPs via this outbound endpoint." Rules are then **shared via AWS RAM** with spoke accounts, which associate them with their own VPCs. **The outbound endpoint itself is not shared — it's reachable indirectly via the shared rule.**

**Private Hosted Zones (PHZ)** — domain namespaces visible inside associated VPCs.
- **Cross-account PHZ association** — create the PHZ in account A, associate with VPCs in accounts B/C/D. The Shared Services VPC is typically associated with all PHZs to be the central resolution point.
- **Overlapping namespaces** — Resolver picks the **most specific match**.

**HA & scale:**
- Two IP addresses across two AZs per endpoint = HA.
- **10,000 QPS per ENI / IP** is the soft limit; add more IPs/ENIs to scale.
- AWS recommends **one set of inbound + one set of outbound endpoints per Region** in a Shared Services VPC — adding more per VPC is sprawl and waste.

**Route 53 Profiles (2024)** — a newer way to bundle PHZs, Resolver rules, and DNS Firewall configs into a reusable **profile**, share via RAM, and attach to many VPCs. For new multi-account environments, this beats individually associating PHZs and rules per VPC.

**Route 53 Resolver DNS Firewall** — allow/block lists for DNS at the VPC level (malware domain blocking, DGA detection, exfiltration prevention). Distinct from Network Firewall.

### 3.7 PrivateLink and VPC Endpoints

The "keep AWS service traffic off the public internet" toolkit.

**Three families of VPC endpoints — know them cold:**

| Type | Powered by | Cost model | Used for |
| --- | --- | --- | --- |
| **Gateway endpoint** | Route table prefix list | **Free** | Only **S3 and DynamoDB** |
| **Interface endpoint** | AWS PrivateLink (ENI in subnet) | $0.01/hr/AZ + $0.01/GB | Most AWS services + your own services + SaaS partners |
| **Resource endpoint** *(GA Nov 2024)* | AWS PrivateLink (resource gateway) | Per-endpoint + per-GB | A **specific resource** in another VPC/on-prem (RDS instance, EC2 by IP, custom TCP target) without needing an NLB |
| **Gateway Load Balancer endpoint** | GWLB | $0.0125/hr/AZ + per-GB | Inserting 3rd-party security appliances in the traffic path |

**Endpoint policies** — IAM-style policies on an endpoint to restrict which API calls, which buckets, which principals can use it. Combined with **`aws:SourceVpce`** and **`aws:SourceVpc`** condition keys in resource policies, this gives you a **data perimeter** at the network level.

**Centralised vs distributed interface endpoints:**

- **Distributed** — each VPC creates its own interface endpoints. Best for **least-privilege** (per-VPC endpoint policy) and for low-VPC-count environments. Cost: every VPC pays the per-AZ-hour fee.
- **Centralised** — interface endpoints live in the Shared Services VPC; spoke VPCs reach them over **TGW**, and DNS is solved with **Private Hosted Zones** for each endpoint (replicating the AWS-managed private DNS entry) shared cross-account.
  - **Trade-off:** saves money at scale (one endpoint instead of many), but **adds TGW data-processing charges** on every call and a **larger blast radius** for endpoint policy mistakes. The official whitepaper *Building a Scalable and Secure Multi-VPC AWS Network Infrastructure* covers this in detail.

**New things the exam is starting to test:**
- **Resource endpoints (Nov 2024)** — reach an RDS instance or any TCP target in another VPC without exposing the whole network or running an NLB.
- **Cross-Region PrivateLink (GA Nov 2025)** — interface endpoints can now target services in other Regions. Permission key: `vpce:AllowMultiRegion`.
- **S3 access via interface endpoint with private DNS for inbound endpoint** — newer 2024+ feature that simplifies the "on-prem hits S3 over DX privately" pattern.

**AWS PrivateLink for *your own* services (Endpoint Services):**
- Provider puts service behind an NLB (or GWLB), creates an endpoint service, authorises consumer account principals.
- Consumer creates an interface endpoint to the published service name.
- **No CIDR overlap concerns**, **no VPC peering / TGW required**, traffic stays on the AWS backbone.

### 3.8 Centralised Inspection (East-West and Egress)

For "all VPC-to-VPC and VPC-to-internet traffic must pass through a firewall":

**AWS Network Firewall (managed, AWS-native)**
- Stateful + stateless rule groups; Suricata-compatible signatures; TLS inspection.
- Deployed in an **Inspection VPC** with a firewall endpoint per AZ.
- TGW spoke routes default to the inspection VPC's TGW attachment → firewall endpoints in each AZ → egress VPC / NAT GW → internet, or → destination spoke VPC.
- **Appliance mode** is required on the inspection VPC's TGW attachment to avoid asymmetric flows.

**Gateway Load Balancer (GWLB)**
- Layer-3 transparent insertion of **3rd-party appliances** (Palo Alto, Fortinet, Check Point, etc.).
- Uses **GENEVE encapsulation**; appliances must support it.
- Consumers use a **GWLB endpoint (GWLBe)** to send traffic to the appliance fleet.
- Choose GWLB when policy requires a specific vendor or when extending an existing on-prem firewall licence.

**Choose Network Firewall when:**
- You want AWS-managed, no vendor licensing, integrated with Suricata signatures, simplest operational overhead.

**Choose GWLB when:**
- You must use a specific vendor's NGFW or IPS.

### 3.9 VPC Lattice (briefly, for context)

A newer **application-layer** networking service for service-to-service communication across VPCs and accounts, using **service networks**. Handles routing, mTLS, authorisation policies (IAM), and observability without TGW configuration. Increasingly mentioned in modern microservices scenarios. For SAP-C02, know it as the alternative to TGW + PrivateLink for **app-layer connectivity with service identity-based access** rather than IP-routing.

---

## 4. SAP-C02 Examination Patterns

### What the exam consistently rewards

1. **Hub-and-spoke over mesh.** Beyond 3–4 VPCs, the answer is TGW (regional) or Cloud WAN (global).
2. **Shared Services VPC in a Network Services account** for DNS, central endpoints, inspection.
3. **AWS RAM for sharing** (TGW, subnets, Resolver rules, Resource Configurations) — never "create the resource in every account."
4. **PrivateLink/VPC endpoints to keep AWS service traffic private** — especially for S3 (Gateway = free in same Region) and STS/ECR/Secrets Manager (Interface).
5. **Direct Connect for predictable throughput/latency**, with VPN as backup attached to the same TGW.
6. **Route 53 Resolver inbound + outbound endpoints**, with rules shared via RAM. PHZs cross-account associated to the Shared Services VPC.
7. **Network Firewall or GWLB+appliances** in a dedicated Inspection VPC, with TGW appliance mode.

### Common traps and how to spot them

| Trap | Why it's wrong |
|---|---|
| "Use VPC peering between 12 VPCs" | Pairwise count explodes; no transitivity. Use TGW. |
| "Put a VPN connection in every account" | Operational nightmare. Use TGW in a Network Services account, terminate one VPN there, share via RAM. |
| "Create an interface endpoint in every VPC" | Cost prohibitive at scale. Centralise in Shared Services VPC. |
| "Associate a DXGW with both a VGW and a TGW" | Not supported on the same DXGW. Use two DXGWs. |
| "Cross-Region traffic over VPC peering will be cheaper than TGW peering" | Peering avoids per-attachment-hour but loses transitivity; rarely the right SAP-C02 answer. |
| "Use TGW for global multi-Region with 8 Regions" | Cloud WAN scales better with policy. |
| "Skip appliance mode on the inspection VPC" | Causes asymmetric routing for stateful appliances. |
| Suggesting an **NLB-based PrivateLink** for a single RDS instance | Resource Endpoints (Nov 2024) do this without an NLB. |
| "Run a DNS server EC2 fleet in a shared VPC" | Anti-pattern. Use Route 53 Resolver endpoints. |

### Key qualifiers to underline in the question
- *"MOST cost-effective"* → favours Gateway endpoints (free), centralised endpoints, VPN over DX where bandwidth isn't critical, VPC peering for 2–3 VPCs.
- *"LEAST operational overhead"* → favours managed services (TGW vs self-managed transit VPC, Network Firewall vs DIY EC2 firewalls, Cloud WAN over hand-coded TGW peering).
- *"MOST resilient"* → DX at two locations + VPN backup; multi-AZ endpoints; cross-Region patterns.
- *"FASTEST to implement"* → RAM + existing TGW; Control Tower account factory; Cloud WAN over building TGW-peering by hand.
- *"PRIVATE"* / *"never traverses the internet"* → PrivateLink/endpoints, DX Private VIF, Private IP VPN.

---

## 5. Realistic Practice Questions

> Same SAP-C02 format. Long scenarios, four options, decisive qualifiers, detailed rationale.

---

### Question 1 — Hub design for scale (Domain 1)

A bank has 42 AWS accounts in one Organization across two AWS Regions. They run 110 VPCs today and expect 60 more in the next 12 months. Currently every VPC pair that needs to communicate uses VPC peering, and on-premises connectivity is via a single Site-to-Site VPN terminated on a virtual private gateway in each VPC that needs it. The bank wants to **simplify connectivity, support transitive routing, allow new accounts to onboard without touching existing VPCs**, and consolidate connectivity to on-premises. They have an AWS Direct Connect 10 Gbps connection in each Region.

**Which design meets the requirements with the LEAST operational overhead?**

A. Deploy an AWS Transit Gateway in a Network Services account in each Region. Share the TGWs to other accounts using AWS RAM. Connect on-prem via a Direct Connect Gateway with a transit VIF associated with each TGW. Replace VGW-terminated VPNs with a single VPN attachment per TGW as backup.

B. Continue using VPC peering but write Lambda automation to create new peerings whenever an account is added. Maintain VGW-terminated VPNs per VPC for high availability.

C. Deploy AWS Cloud WAN with one core network covering both Regions. Migrate all VPCs to Cloud WAN attachments. Connect on-prem directly to Cloud WAN through Direct Connect.

D. Use VPC peering plus a centralised Shared VPC shared via AWS RAM into which all workloads are migrated. Terminate the VPNs on the Shared VPC's VGW.

<details>
<summary><b>Answer & rationale</b></summary>

**Correct: A.**

- **A** is the canonical landing-zone pattern: TGW per Region in a Network Services account, RAM-shared, DX via DXGW + transit VIF, VPN as resilient backup. Onboarding a new account is now "create VPC → attach to shared TGW" — minutes, not days.
- **B** keeps the mesh problem. With ~170 VPCs the pairwise count is enormous and peerings aren't transitive — so even fully meshed you still cannot route via a hub.
- **C** is tempting because Cloud WAN is the future, **but** Cloud WAN **does not yet natively support Direct Connect attachments**. You'd still need a TGW to terminate DX, then peer with Cloud WAN — more moving parts than the question warrants. Cloud WAN shines once you go to 4+ Regions or want policy-as-code; for two Regions, TGW is the lower-overhead answer.
- **D** forces every workload into one VPC's CIDR; CIDR collisions and the Shared VPC participant restrictions make this impractical at 170 VPCs.

**Exam takeaway:** for regional or two-Region hub-and-spoke, **TGW + DXGW + VPN backup**, shared via RAM, is the default winner.
</details>

---

### Question 2 — Centralised vs distributed VPC endpoints (Domain 1 / Domain 3)

A fintech runs 60 application VPCs across 25 accounts. Every VPC needs private access to S3, ECR, STS, KMS, and Secrets Manager. Today each VPC has its own interface endpoints for these services, costing roughly $0.01/hour per AZ across three AZs per endpoint. Monthly endpoint costs are now significant. The architect wants to **reduce endpoint cost while keeping traffic off the public internet** and maintaining DNS resolution for the standard AWS service hostnames inside each spoke VPC.

**Which approach is the MOST cost-effective for this scale?**

A. Migrate all VPCs to a single Shared VPC owned by the Network Services account and shared via AWS RAM. Recreate workloads inside the Shared VPC and keep one set of interface endpoints.

B. In the Network Services account, replace each VPC's S3 interface endpoint with an S3 Gateway endpoint. For ECR, STS, KMS, and Secrets Manager, centralise the interface endpoints in a Shared Services VPC. Use private hosted zones to resolve the AWS service hostnames to the centralised endpoints' private IPs. Route spoke VPC traffic through the existing Transit Gateway.

C. Replace the interface endpoints with NAT Gateways in every VPC so traffic egresses to AWS public endpoints. This is cheaper than maintaining interface endpoints.

D. Use VPC peering between every spoke VPC and a Shared Services VPC that hosts the interface endpoints.

<details>
<summary><b>Answer & rationale</b></summary>

**Correct: B.**

- **B** is the canonical centralised endpoint pattern from the AWS networking whitepaper. **Gateway endpoints (S3, DynamoDB) are free** — always use them when same-Region. For the other services, centralising the interface endpoints in a Shared Services VPC, plus PHZs replicating the AWS-managed private DNS names, dramatically reduces per-AZ-hour endpoint cost. The trade-off — added TGW data-processing charges — is usually still a net saving at this scale, and there's no internet exposure.
- **A** is unrealistic operationally: migrating 60 VPCs into one Shared VPC with no CIDR collisions and re-platforming Participants' resources is far more expensive than the endpoint bill.
- **C** sends traffic over the public internet — violates the requirement.
- **D** doesn't scale (60 peering connections per Shared Services VPC) and provides nothing TGW doesn't already.

**Exam takeaway:** S3/DynamoDB → **Gateway endpoint (free)**. Many other AWS services across many VPCs → **centralise interface endpoints + PHZ + TGW**.
</details>

---

### Question 3 — Hybrid DNS (Domain 1)

An energy company has on-prem data centres using domain `corp.example.com` and an AWS environment using `aws.corp.example.com`. They have 18 AWS accounts. Workloads in any account must resolve names in both `corp.example.com` (forwarded to on-prem BIND servers) and any other account's `aws.corp.example.com` subdomains. On-prem hosts must resolve `aws.corp.example.com` names too. DX connectivity is already in place through a Network Services account hosting a TGW.

**What is the MOST scalable and operationally efficient design?**

A. In every account, create both inbound and outbound Route 53 Resolver endpoints, configure forwarding rules locally, and set up the on-prem BIND server with 36 conditional forwarders.

B. In the Network Services account's Shared Services VPC, deploy one inbound and one outbound Resolver endpoint. Create a Resolver rule forwarding `corp.example.com` to on-prem and share it via AWS RAM with all 18 accounts. Each account creates its `aws.corp.example.com` PHZ and associates it with the Shared Services VPC; configure on-prem BIND to forward `aws.corp.example.com` to the inbound endpoint IPs.

C. Run a self-managed BIND DNS cluster in EC2 in the Shared Services VPC and point all VPCs and on-prem at it.

D. Use Route 53 public hosted zones for all internal names so no Resolver endpoints are needed.

<details>
<summary><b>Answer & rationale</b></summary>

**Correct: B.**

- **B** is exactly the AWS-published "centralised DNS for hybrid in a multi-account environment" pattern. **One set of endpoints per Region** in the Shared Services VPC. The outbound endpoint serves all accounts via a RAM-shared rule. The inbound endpoint serves on-prem from one set of IPs. Cross-account **PHZ associations** to the Shared Services VPC give a unified resolution view. New accounts onboard by associating their PHZ — minimal toil.
- **A** is endpoint sprawl: 36 endpoints, 36 sets of rules, brittle on-prem config.
- **C** abandons the managed Resolver service for an undifferentiated EC2 BIND fleet — high operational overhead and a single failure domain unless engineered carefully. Anti-pattern.
- **D** exposes internal names on the public internet — unacceptable for any enterprise, especially energy/financial services.

**Exam takeaway:** Hybrid DNS at scale = **one inbound + one outbound endpoint** in Shared Services VPC, **rules shared via RAM**, **PHZs cross-account associated**.
</details>

---

### Question 4 — Hybrid connectivity resilience (Domain 1 / Domain 2)

A trading firm requires hybrid connectivity to AWS with **99.99% availability**, **encrypted in transit**, and **predictable latency under 5 ms** to a single AWS Region. Throughput requirement is 8 Gbps sustained. They are willing to invest in carrier-diverse network paths.

**Which architecture best meets these requirements?**

A. A single 10 Gbps AWS Direct Connect dedicated connection with MACsec at one DX location, terminating on a Transit Gateway via a Direct Connect Gateway and transit VIF.

B. Two 10 Gbps AWS Direct Connect dedicated connections at **two different DX locations**, each with MACsec, both terminating on the same Direct Connect Gateway associated with a Transit Gateway. Configure BGP with active/active ECMP, and additionally configure a Site-to-Site VPN as a tertiary backup over the internet attached to the same TGW.

C. Three Site-to-Site VPN tunnels using ECMP terminating on a Transit Gateway, over the public internet, with carrier-diverse ISPs.

D. A single AWS Direct Connect connection with a Site-to-Site VPN over the same connection's public VIF as a backup.

<details>
<summary><b>Answer & rationale</b></summary>

**Correct: B.**

- **B** matches AWS's published **"maximum resilience"** pattern for Direct Connect: connections at multiple DX locations, distinct devices, BGP-controlled failover, plus a VPN as a final fallback. MACsec encrypts at L2 on each DX, satisfying "encrypted in transit." Two 10 Gbps connections in active/active ECMP easily covers 8 Gbps sustained with headroom even if one path fails. 99.99% requires both connection redundancy *and* location redundancy — a single DX location is still a single point of failure (fibre cut, building outage).
- **A** fails 99.99% — single DX location is one failure domain.
- **C** can't guarantee predictable <5 ms latency over the public internet; throughput per tunnel is ~1.25 Gbps so even with ECMP it's marginal for 8 Gbps sustained.
- **D** has both the primary and the backup riding on the same physical connection — a useless "backup."

**Exam takeaway:** For 99.99% hybrid availability → **two DX connections at two DX locations + VPN backup**, terminating on the same TGW. MACsec satisfies "encrypted in transit" for DX without requiring VPN overlay.
</details>

---

### Question 5 — Centralised east-west inspection (Domain 1 / Domain 3)

A regulated insurer requires that **all traffic between Production and Non-Production VPCs**, and **all egress to the internet from any VPC**, be inspected by a stateful firewall. The security team has standardised on AWS Network Firewall and wants centralised management, full visibility, and no asymmetric flow drops. The company has 30 VPCs attached to a single regional Transit Gateway.

**Which design meets the requirements?**

A. Deploy an AWS Network Firewall in every spoke VPC. Update each VPC's route tables to send all 0.0.0.0/0 traffic to the firewall endpoints. Use VPC Flow Logs to detect asymmetric drops.

B. Deploy a centralised Inspection VPC with AWS Network Firewall endpoints in each AZ. Attach the Inspection VPC to the TGW with **appliance mode** enabled. Use TGW route tables to send Production-to-NonProduction traffic and all 0.0.0.0/0 default routes from spokes through the Inspection VPC, then to an Egress VPC for internet-bound traffic.

C. Use NACLs and Security Groups in every VPC to block east-west traffic. For egress, route everything through a single NAT Gateway in a shared VPC.

D. Implement AWS Network Firewall in a single VPC and use VPC peering from every spoke VPC to that inspection VPC. Configure routing to send all traffic through the peering connections.

<details>
<summary><b>Answer & rationale</b></summary>

**Correct: B.**

- **B** is the textbook centralised inspection pattern. The Inspection VPC contains the firewall endpoints in each AZ; the TGW directs east-west traffic via separate route tables (Prod RT default-routes to the Inspection VPC attachment) and the Inspection VPC sends inspected traffic onward to the Egress VPC (NAT GW) or back to the destination spoke. **Appliance mode** on the Inspection VPC's TGW attachment is mandatory to keep request and response flows on the same AZ ENI — without it, stateful inspection drops asymmetric flows.
- **A** is operationally untenable (30 firewall deployments) and doesn't centralise visibility.
- **C** is not a stateful firewall control and doesn't meet the inspection requirement; NACLs are stateless and SGs are L3/L4 only.
- **D** uses peering, which is non-transitive — you'd need a peering from every spoke to the inspection VPC, and routing all traffic through it doesn't scale and bypasses TGW.

**Exam takeaway:** Centralised inspection = **Inspection VPC + Network Firewall (or GWLB+appliance) + TGW appliance mode + segmented TGW route tables**.
</details>

---

### Question 6 — Exposing an internal service across accounts (Domain 2)

A platform team in Account A operates a containerised fraud-scoring API used by 14 application teams in 14 separate AWS accounts in the same Region and Organization. The API must be reachable **privately, without VPC peering, without TGW connectivity, with each consumer account explicitly authorised**, and with CIDR overlaps between consumer VPCs and the provider VPC being acceptable.

**Which approach meets the requirements?**

A. Place the API behind a Network Load Balancer in Account A. Create a VPC endpoint service from the NLB. Authorise the 14 consumer accounts' principals on the endpoint service. Each consumer creates an interface endpoint in their VPC and calls the API via the endpoint's DNS name.

B. Expose the API on a public Application Load Balancer and rely on TLS plus a WAF rule restricting source IPs.

C. Create VPC peering connections between Account A's VPC and each of the 14 consumer VPCs.

D. Share Account A's VPC with all 14 consumer accounts using AWS RAM and have them deploy clients into the shared VPC.

<details>
<summary><b>Answer & rationale</b></summary>

**Correct: A.**

- **A** is exactly what PrivateLink endpoint services exist for: provider behind an NLB, consumers create interface endpoints to a service name. **No CIDR overlap concerns** (PrivateLink doesn't route — it presents the service via an ENI in the consumer VPC), **no peering or TGW**, and the provider explicitly authorises consumer account principals.
- **B** exposes the API on the public internet — fails "privately."
- **C** doesn't work if any consumer CIDR overlaps with the provider VPC; also operational overhead × 14.
- **D** forces all 14 consumer teams to migrate their workloads into Account A's VPC — completely impractical.

**Exam takeaway:** *"Expose one service to many consumers, privately, no full network connectivity"* → **PrivateLink endpoint service**. If exposing a single resource (a DB instance, an EC2 by IP) without an NLB, consider the newer **Resource Endpoints** (Nov 2024).
</details>

---

## 6. Study Strategy

### Analogies that map to your background
- **Transit Gateway ≈ regional fibre core / MPLS PE router** — every site attaches once, the core handles transit.
- **Cloud WAN ≈ global SD-WAN orchestrator with policy-as-code.**
- **TGW route tables ≈ VRFs** at the trading-room / business-unit boundary.
- **Direct Connect ≈ a leased line into a colocation facility** that lets you ride AWS's backbone to any Region globally.
- **PrivateLink ≈ a dedicated service VLAN** delivered into each consumer's environment.
- **Route 53 Resolver inbound/outbound endpoints ≈ corporate stub resolvers** with conditional forwarders.
- **Network Firewall in an Inspection VPC ≈ the perimeter firewall cluster every regulated firm runs in its data centre** — only here it sits between VPCs.

### Recommended primary sources
1. **AWS Whitepaper:** *Building a Scalable and Secure Multi-VPC AWS Network Infrastructure* — the canonical reference; multiple SAP-C02 questions trace directly to it.
2. **AWS Whitepaper:** *Hybrid Cloud DNS Options for Amazon VPC*.
3. **AWS Whitepaper:** *AWS Security Reference Architecture (SRA)* — networking sections.
4. **AWS Well-Architected:** *Hybrid Networking Lens*.
5. **AWS Networking & Content Delivery Blog** — RSS-worthy; the SAP-C02 emerging-content topics often start here.
6. **AWS Prescriptive Guidance:** *Set up DNS resolution for hybrid networks in a multi-account environment*.

### Hands-on labs (in order of value)
1. Build a TGW in a Network Services account, share via RAM, attach two spoke VPCs in different accounts, confirm transitive routing.
2. Add a Site-to-Site VPN to the TGW with two tunnels and ECMP; simulate failover.
3. Build a centralised Route 53 Resolver pair (inbound + outbound) in a Shared Services VPC; share a forwarding rule via RAM; verify resolution from a spoke VPC.
4. Place an S3 Gateway endpoint and an interface endpoint for KMS in a Shared Services VPC; build a PHZ for the KMS endpoint and verify a spoke VPC resolves to the central ENI.
5. Add an Inspection VPC with AWS Network Firewall and TGW appliance mode; route all spoke egress through it.
6. Publish a sample service from one account via PrivateLink endpoint service; consume it from a second account.

---

*Document scope: This is an overview, not a complete networking curriculum. Be sure to also cover edge services (CloudFront, Global Accelerator, Route 53 routing policies), AWS WAF + Shield, IPv6 patterns, and Outposts/Local Zones/Wavelength for SAP-C02 readiness.*
