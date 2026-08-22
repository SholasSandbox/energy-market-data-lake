# 08 - Networking, Hybrid Connectivity, and Private Access

**Last revised:** 2026-08-20<br>
**Latest revision scope:** Added the Direct Connect acronym and VIF recall legend.

Networking questions in SAP-C02 are usually about scale, isolation, transitivity, routing, hybrid connectivity, and cost.

## Core chooser

| Requirement | Service/pattern |
|---|---|
| Isolated network boundary | VPC |
| Private subnet outbound internet | NAT Gateway |
| Private access to AWS services | VPC endpoints |
| Many VPCs/accounts hub connectivity | Transit Gateway |
| Simple VPC-to-VPC private link | VPC peering |
| Private service exposure to consumers | PrivateLink |
| Dedicated private hybrid connectivity | Direct Connect |
| Encrypted tunnel over internet | Site-to-Site VPN |
| Client remote access VPN | Client VPN |
| Central network firewalling | AWS Network Firewall / GWLB appliance pattern |
| DNS inside VPC | Route 53 Resolver/private hosted zones |

## VPC basics

Important objects:

- CIDR block
- Subnets
- Route tables
- Internet Gateway
- NAT Gateway
- Security Groups
- Network ACLs
- VPC endpoints
- DHCP options
- Route 53 Resolver

## Public vs private subnet

| Subnet type | Required route |
|---|---|
| Public subnet | Its route table has a route to an Internet Gateway |
| Private subnet with outbound internet | Route to NAT Gateway |
| Isolated private subnet | No internet route; use VPC endpoints/private connectivity |

Trap: A subnet is classified as public because of its route to an Internet
Gateway, not because of its name or because every resource has a public IP. For
direct IPv4 internet communication, the resource also needs a public IPv4 or
Elastic IP and security controls must allow the traffic. An ALB can be
internet-facing while its targets retain only private addresses.

## Security Groups vs NACLs

| Control | Type | State | Use |
|---|---|---|---|
| Security Group | Instance/ENI-level | Stateful | Primary workload firewall |
| Network ACL | Subnet-level | Stateless | Broad subnet guardrail, explicit deny |

Trap: Security Groups do not support explicit deny; NACLs do.

### Read flow logs as directional tuples

For TCP, a client opens the connection from a high ephemeral source port to the
server's listening port:

```text
request:  client:20641 -> server:5001
response: server:5001  -> client:20641
```

Security groups are stateful, so an allowed request automatically permits its
response. Network ACLs are stateless and must allow both directions. If the
request is `ACCEPT` but the reverse tuple is `REJECT` on the server ENI/subnet,
allow outbound traffic to the client's ephemeral port in the server subnet's
NACL. Do not open the ephemeral port as a server inbound port; the application
still listens on 5001.

## VPC endpoints

| Endpoint type | Use |
|---|---|
| Gateway endpoint | S3 and DynamoDB private access via route table |
| Interface endpoint | PrivateLink-powered ENI endpoint for many AWS services |
| Gateway Load Balancer endpoint | Appliance insertion pattern |

Benefits:

- keep traffic off public internet
- reduce NAT Gateway dependency/cost for AWS API traffic
- improve security posture with endpoint policies
- enable private subnet access to AWS services

Trap: Endpoint policy, IAM policy, resource policy, and KMS key policy may all need alignment.

## NAT Gateway cost trap

NAT Gateway has hourly and data processing costs. Architectures with many private workloads pulling large data from S3/ECR/CloudWatch through NAT can become expensive.

Mitigation:

- S3 Gateway endpoint
- DynamoDB Gateway endpoint
- ECR API/Docker interface endpoints
- CloudWatch Logs interface endpoint
- Secrets Manager/SSM/KMS interface endpoints where needed
- regional workload placement and routing review

### NAT Gateway availability boundary

A NAT Gateway is resilient within one Availability Zone, not across AZs. If
private subnets in several AZs all route through one NAT Gateway, loss of that
NAT Gateway's AZ removes their outbound path.

For an AZ-resilient design, deploy one NAT Gateway per used AZ and route each
private subnet to the NAT Gateway in the same AZ. This also avoids unnecessary
cross-AZ data processing. A second Internet Gateway is not the answer: an
Internet Gateway is already a horizontally scaled Regional VPC component, and
a VPC attaches to only one Internet Gateway.

### Private cross-Region access to one S3 bucket

S3 gateway endpoints are free and preferred for same-Region access, but they
cannot be consumed through VPC peering, Transit Gateway, or from a VPC in
another Region. S3 interface endpoints use private IP addresses and can be
reached from another Region through VPC peering or Transit Gateway.

Use the traffic and storage facts to choose:

| Scenario | Better pattern |
|---|---|
| Repeated/high-volume reads in another Region | Replicate to a local S3 bucket, then use a local gateway endpoint |
| Infrequent/low-volume reads of a large existing dataset | Reach an S3 interface endpoint privately through inter-Region connectivity; avoid duplicating the whole dataset |

The interface-endpoint pattern is billed and needs routing, security-group,
endpoint-policy, bucket-policy and endpoint-specific DNS alignment. It is not a
universal replacement for the free gateway endpoint.

## VPC peering

Choose peering when:

- small number of VPCs need direct private connectivity
- no transitive routing is required
- CIDRs do not overlap

Avoid peering when:

- many VPCs/accounts create mesh complexity
- transitive routing is required
- centralized inspection/hub architecture is needed

## Transit Gateway

Choose Transit Gateway when:

- many VPCs/accounts need hub-and-spoke routing
- hybrid connectivity should be centralized
- route tables must segment environments
- Direct Connect/VPN should attach to a central hub
- network team needs centralized routing control

Trap: Transit Gateway adds cost and route complexity. For two VPCs, peering may be simpler.

When many VPCs already attach to separate Transit Gateways in the same Region
but now require one routing domain, consolidate the smaller set onto one
Transit Gateway and update its attachment and VPC route tables. Creating
service endpoints does not provide VPC-to-VPC routing, and creating a third
Transit Gateway in another Region adds unjustified peering and operations.

### Transit Gateway segmentation

Each attachment is associated with one Transit Gateway route table and can
propagate routes to selected route tables. Build separate routing domains when
environments must not communicate:

```text
production attachments -> production TGW route table -> production routes only
development attachments -> development TGW route table -> development routes only
```

Remove association and propagation from the permissive default route table
before associating the attachments with their intended environment table.
Attachment tags are classification metadata unless automation turns them into
routing changes; tags alone do not enforce isolation. Security-group changes
are a distributed workload control and are not a substitute for removing the
cross-environment route.

## PrivateLink

Choose PrivateLink when:

- provider exposes a private service to consumer VPCs/accounts
- consumer should not access provider VPC CIDR broadly
- overlapping CIDRs exist
- SaaS/private service model is needed
- NLB-backed endpoint service pattern fits

Trap: PrivateLink is not transitive network routing. It exposes a service endpoint, not full VPC connectivity.

## Direct Connect and VPN

### Direct Connect recall legend

| Acronym or term | Meaning | Fast recall cue |
|---|---|---|
| DX | AWS Direct Connect | Dedicated private network connection into AWS; not encrypted by default |
| VIF | Virtual interface | Logical interface carried over a DX connection |
| Private VIF | Private virtual interface | Private VPC addressing through a VGW directly or a DXGW associated with VGWs |
| Public VIF | Public virtual interface | AWS public service endpoints using public IP addressing; not general internet transit |
| Transit VIF | Transit virtual interface | DXGW-to-TGW path for centralized access to many VPCs |
| DXGW | Direct Connect gateway | Global gateway linking eligible VIFs to VGWs or TGWs |
| VGW | Virtual private gateway | VPC-level termination for the private-VIF path |
| TGW | Transit Gateway | Regional routing hub reached through a transit VIF and DXGW |
| BGP | Border Gateway Protocol | Exchanges prefixes and path attributes across the hybrid connection |

| Requirement | Choice |
|---|---|
| Consistent bandwidth/private connectivity | Direct Connect |
| Encrypted tunnel quickly over internet | Site-to-Site VPN |
| Backup path for Direct Connect | VPN |
| Multiple VPC access from DX | Direct Connect gateway plus the correct virtual-interface and gateway path |
| Remote users to VPC | Client VPN |

Trap: Direct Connect is private but not encrypted by default. Use MACsec where supported or VPN over DX if encryption requirement exists.

### Direct Connect gateway paths

Reconstruct the named attachment path before selecting an answer:

```text
many VPCs through a routing hub
  -> transit VIF
  -> Direct Connect gateway
  -> Transit Gateway

VPCs reached through a virtual private gateway
  -> private VIF
  -> Direct Connect gateway
  -> virtual private gateway
```

A transit VIF is the Transit Gateway path. A private VIF reaches a VPC through
a virtual private gateway or can access supported private AWS resources; it is
not attached directly to a Transit Gateway. A public VIF is for AWS public
service endpoints and is not the private VPC-routing answer.

### Direct Connect BGP route choice

AWS evaluates the destination prefix first. A more-specific prefix wins by longest-prefix match before local preference, AS-path length, or MED is considered. Advertise a more-specific prefix over the preferred Direct Connect BGP session when only that address range should favor the path.

Trap: AS-path prepending influences equally specific BGP routes; it cannot defeat a competing longer prefix.

## Route 53 Resolver

Use for hybrid DNS:

- inbound Resolver endpoints allow on-prem DNS to resolve AWS private zones
- outbound Resolver endpoints allow VPC resources to query on-prem DNS
- Resolver rules forward specific domains

Trap: DNS resolution must be designed explicitly in hybrid architectures. Network connectivity alone does not solve name resolution.

## Administrative access path selection

| Requirement | First choice | Important boundary |
|---|---|---|
| Interactive shell to managed instances without bastion, SSH keys, or inbound port 22 | Systems Manager Session Manager | IAM controls session start; normal shell sessions can be logged to S3/CloudWatch Logs |
| Forward a local port to a managed node or a remote host reachable from it | Session Manager port forwarding | The tunnel contents and commands are not session-logged |
| Run the same command across many managed nodes without interactive login | Systems Manager Run Command | Not a shell or port-forwarding replacement |
| Give remote clients general network connectivity into VPC subnets | AWS Client VPN | VPN connection logs do not record commands inside SSH/application protocols |
| Existing SSH workflow must be retained without public bastion exposure | EC2 Instance Connect Endpoint or SSH through Session Manager, if the named requirements fit | SSH through Session Manager is tunnelled and its command contents are not logged by Session Manager |

Exam conjunction rule: if the stem requires **no inbound SSH**, **interactive
access**, **command audit**, and the **ability** to forward ports, Session
Manager is the service family. Use normal Session Manager shell sessions for
command logging and separate port-forwarding sessions where required; do not
claim the forwarded traffic is logged.

## Centralized inspection

Common patterns:

```text
Spoke VPCs
  -> Transit Gateway
  -> Inspection VPC
  -> AWS Network Firewall / GWLB appliances
  -> egress/on-prem/internet
```

Watch for:

- asymmetric routing
- route table segmentation
- appliance scaling
- fail-open/fail-closed requirements
- logging and compliance

## Exam traps

| Trap | Correction |
|---|---|
| “VPC peering is transitive” | It is not. |
| “Private subnet means no outbound access” | It can have outbound via NAT or endpoints. |
| “NAT Gateway is needed for S3 access” | Gateway endpoints can provide private S3 access. |
| “Direct Connect encrypts traffic automatically” | It is private connectivity, not automatically encrypted. |
| “PrivateLink connects whole VPCs” | It exposes services through endpoints. |
| “Security Group blocks are stateless” | Security Groups are stateful. |
| “NACLs are only allow rules” | NACLs support allow and deny and are stateless. |
| “Client VPN provides an SSH command audit” | It provides network reachability; use Session Manager shell logging for command content. |
| “A public subnet gives every instance internet reachability” | The subnet needs an Internet Gateway route; an instance also needs a public IPv4/EIP for direct IPv4 communication. |
| “Any Direct Connect VIF can attach to Transit Gateway” | Use a transit VIF through a Direct Connect gateway for the Transit Gateway path. |

## Additional references

- Session Manager capabilities: <https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager.html>
- Session Manager logging limitations: <https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-logging.html>
- Direct Connect virtual interfaces: <https://docs.aws.amazon.com/directconnect/latest/UserGuide/WorkingWithVirtualInterfaces.html>
- Direct Connect gateways: <https://docs.aws.amazon.com/directconnect/latest/UserGuide/direct-connect-gateways-intro.html>
