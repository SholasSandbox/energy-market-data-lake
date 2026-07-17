# Domain 2 Security Groups Versus Network ACLs - 2026-07-15

<!-- markdownlint-disable MD013 -->

## Scope

This source-backed comparison closes the tracker-required security-groups-versus-network-ACL study note. It supports SAP-C02 VPC security, troubleshooting, and defense-in-depth decisions.

This is a documentation-only study artifact. It does not authorize security-group, network-ACL, subnet, route-table, VPC, or other AWS changes.

## Decision Summary

Use **security groups** as the primary workload-level control on supported resources and elastic network interfaces. They are stateful, contain allow rules only, and automatically allow response traffic for a permitted flow.

Use **network ACLs** when a subnet-level, stateless boundary or an explicit CIDR-based deny is required. They evaluate numbered allow and deny rules in order and require the return direction to be permitted explicitly.

These controls are complementary. A packet must satisfy the applicable route, network ACL, security group, and any service-specific policy. A route selects a path; it does not authorize traffic.

## Comparison Matrix

| Decision point | Security group | Network ACL | SAP-C02 implication |
|---|---|---|---|
| Attachment level | Associated with supported resources or network interfaces. | Associated with a subnet; one network ACL applies to a subnet at a time. | Choose based on whether the requirement is workload-level or subnet-level. |
| State | Stateful through connection tracking. | Stateless. | Security-group return traffic is automatically permitted; network ACL return traffic needs an explicit rule. |
| Rule actions | Allow rules only. | Allow and deny rules. | Choose a network ACL when the requirement explicitly needs a subnet-level deny for a CIDR or port. |
| Rule evaluation | Rules from all attached security groups are aggregated; there is no rule ordering. | Rules are processed from the lowest rule number upward; the first match wins. | A lower-numbered deny can override a broader higher-numbered allow. |
| Source or destination | CIDR, prefix list, or security-group reference where supported. | CIDR-based source for inbound and destination for outbound. | Use security-group references for identity-like workload relationships; network ACLs do not reference security groups. |
| Return traffic | Automatically allowed for a tracked permitted flow, regardless of a missing reverse-direction rule. | Must be explicitly allowed in the reverse direction. | Ephemeral-port omissions commonly break network-ACL designs. |
| Multiple controls | Multiple security groups can be associated and their allow rules combine. | A subnet has one associated network ACL, although one ACL can cover multiple subnets. | Adding a restrictive security group does not cancel an allow from another attached security group. |
| Default behavior | A new security group has no inbound allow rules and normally starts with an allow-all outbound rule. | The default VPC network ACL allows traffic; a new custom network ACL denies unmatched inbound and outbound traffic. | Do not confuse the default ACL with a newly created custom ACL. |
| Best fit | Least-privilege access to a workload, tier, load balancer, database, or interface endpoint. | Defense-in-depth boundary across every resource in a subnet or an explicit deny list. | Security groups are normally the primary control; network ACLs are a coarse additional boundary. |

## Packet-Flow Examples

### Internet Client Reaches an HTTPS Server

Assume routing is already correct and the server listens on TCP 443.

1. The subnet's inbound network ACL must allow TCP 443 from the intended client CIDR.
2. The server's security group must allow inbound TCP 443 from the intended source.
3. The server response is automatically allowed by the stateful security group.
4. The subnet's outbound network ACL must allow the client's ephemeral destination port range.

Exam trap: adding an outbound TCP 443 network-ACL rule does not permit the response. The response destination is the client's ephemeral port, not the server's listening port.

### Private Instance Initiates HTTPS Outbound

Assume routing provides an approved path to the destination.

1. The instance security group must allow outbound TCP 443 to the destination.
2. The subnet's outbound network ACL must allow TCP 443 to the destination.
3. The subnet's inbound network ACL must allow the return traffic to the client's ephemeral port range.
4. The stateful security group automatically permits the response to the tracked outbound flow.

Ephemeral-port ranges vary by client operating system and AWS service. Use the range required by the actual clients; a broad `1024-65535` range is common when several client types must be supported, but it should not be assumed without examining the scenario.

## Rule-Evaluation Example

Suppose an inbound network ACL has these rules:

| Rule number | Source | Port | Action |
|---:|---|---:|---|
| 100 | `198.51.100.25/32` | All | Deny |
| 200 | `0.0.0.0/0` | 443 | Allow |
| `*` | `0.0.0.0/0` | All | Deny |

Traffic from `198.51.100.25` matches rule 100 first and is denied, even when it targets TCP 443. Other IPv4 clients can match rule 200 for TCP 443. Number specificity does not determine precedence; the lowest numbered matching rule does.

Security groups do not work this way. If any aggregated security-group rule allows the traffic, there is no separate security-group deny rule that overrides it.

## High-Value SAP-C02 Decision Rules

| Scenario wording | Prefer | Why |
|---|---|---|
| Allow the application tier to reach the database tier on one port. | Security-group-to-security-group reference. | Expresses the workload relationship without maintaining client IP ranges. |
| Block one known malicious CIDR for every resource in a subnet. | Network ACL deny rule with deliberate rule ordering. | Security groups cannot express deny rules. |
| Permit responses to an already allowed connection. | Rely on security-group state, but configure both network-ACL directions. | The controls have different return-traffic behavior. |
| Apply a coarse backup control if a workload is launched with the wrong security group. | Network ACL as defense in depth. | The subnet boundary applies across resources in that subnet. |
| Filter DNS queries sent to VPC Resolver. | Route 53 Resolver DNS Firewall. | Security groups cannot block DNS requests to the Amazon-provided VPC Resolver. |
| Diagnose an apparently correct route that still fails. | Check security groups, both network-ACL directions, ports, CIDRs, and return routing. | Route existence does not prove authorization or a valid return path. |

## Common Exam Traps

1. **Stateful does not mean unrestricted.** The initiating direction must match a security-group allow rule; only response traffic benefits from connection state.
2. **A network ACL needs both directions.** Permit the service port in the request direction and the correct ephemeral ports in the response direction.
3. **First match wins for network ACLs.** Rule numbers define evaluation order; a later, more specific-looking rule is irrelevant after an earlier match.
4. **Security-group rules aggregate.** Attaching a second restrictive security group does not override permissions granted by another attached group.
5. **Default and custom network ACLs differ.** The default ACL initially allows traffic, while a new custom ACL denies unmatched traffic.
6. **Neither control creates reachability.** Routes, gateways, endpoints, and return paths remain separate decisions.
7. **Network ACLs are coarse.** They apply to the subnet and use CIDRs, while security groups can express workload relationships through group references.
8. **DNS filtering is separate.** Use Resolver DNS Firewall for queries through the VPC Resolver rather than trying to deny the Amazon-provided DNS service with a security group.

## Operational and Troubleshooting Checks

- Use VPC Flow Logs to observe accepted and rejected traffic at supported interfaces and boundaries.
- Use VPC Reachability Analyzer for static configuration analysis of the path and to identify a blocking component.
- Check IPv4 and IPv6 rules separately; an IPv4 allow does not authorize IPv6 traffic.
- Verify load-balancer health-check paths and ephemeral ports before tightening a custom network ACL.
- Leave gaps between network-ACL rule numbers so a higher-priority rule can be inserted safely.
- Review overly broad security-group sources such as `0.0.0.0/0` and `::/0`, especially on administrative ports.

## Lakehouse Application Boundary

This comparison is an exam and future-architecture decision aid. It does not prove that the current Energy Data Lakehouse requires a new subnet boundary or security-control change. Any future implementation must start from current VPC evidence, identify the exact flow, preserve rollback access, and receive explicit approval before AWS modification.

## Recall Check

Answer without looking above:

1. Which control is stateful, and what does that mean for response traffic?
2. Which control can explicitly deny a source CIDR?
3. How are network-ACL rules evaluated?
4. Why might inbound HTTPS succeed but the response still fail at a network ACL?
5. Why does attaching a restrictive second security group not remove an allow from the first?
6. Which control should express application-tier-to-database-tier access when both tiers use security groups?
7. What is the difference between the default network ACL and a new custom network ACL?
8. Why can a valid route coexist with a blocked connection?

## Official AWS References

- [Compare security groups and network ACLs](https://docs.aws.amazon.com/vpc/latest/userguide/infrastructure-security.html#VPC_Security_Comparison)
- [Security group rules](https://docs.aws.amazon.com/vpc/latest/userguide/security-group-rules.html)
- [Security group connection tracking](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/security-group-connection-tracking.html)
- [Network ACL rules](https://docs.aws.amazon.com/vpc/latest/userguide/nacl-rules.html)
- [Custom network ACLs and ephemeral ports](https://docs.aws.amazon.com/vpc/latest/userguide/custom-network-acl.html)
- [Default network ACL](https://docs.aws.amazon.com/vpc/latest/userguide/default-network-acl.html)
- [VPC Flow Logs](https://docs.aws.amazon.com/vpc/latest/userguide/flow-logs.html)
- [VPC Reachability Analyzer](https://docs.aws.amazon.com/vpc/latest/reachability/what-is-reachability-analyzer.html)
