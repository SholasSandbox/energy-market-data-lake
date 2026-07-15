# Domain 2 VPC Endpoint Diagram - 2026-07-15

<!-- markdownlint-disable MD013 -->

## Scope

This documentation-only study artifact closes the tracker-required VPC endpoint
diagram. Its companion Mermaid source is `diagrams/vpc-endpoint-study.mmd`.
It compares Amazon S3 and DynamoDB gateway endpoints with interface endpoints,
including their routing, DNS, authorization, service-boundary, and cost
decisions.

It does not authorize an endpoint, route-table, subnet, DNS, security-group,
NAT Gateway, Transit Gateway, VPN, Direct Connect, logging, or cost-management
change in AWS.

## Reading the Diagram

### Gateway endpoint: default for eligible in-VPC S3 or DynamoDB traffic

1. Associate each route table used by eligible workload subnets with the
   gateway endpoint. AWS adds an uneditable route whose destination is the
   service's AWS-managed prefix list and whose target is the gateway endpoint.
2. The associated route table directs same-Region S3 or DynamoDB traffic to
   the gateway endpoint. A default route to a NAT Gateway or internet gateway
   does not win for that service traffic; the endpoint route is more specific.
3. The workload security group still permits the required connection, normally
   HTTPS to the service prefix list. Network ACLs and return traffic remain
   separate checks.
4. IAM and the service resource policy must permit the request. An endpoint
   policy can additionally limit who can use the endpoint; it is not a
   substitute for identity or resource policies.

Gateway endpoints support only S3 and DynamoDB. They use route-table
association rather than interface ENIs or endpoint security groups, have no
additional endpoint charge, and do not extend through on-premises networks,
VPN, Direct Connect, Transit Gateway, or peering paths. They are therefore the
least-broad answer for eligible same-Region VPC traffic, not a general private
networking service.

### Interface endpoint: selective private service access

1. Select at most one subnet in each required Availability Zone. AWS creates
   an endpoint ENI with a private IP address in each selected subnet.
2. Attach a security group to those endpoint ENIs. Its rules govern traffic
   from the workload to the endpoint; allow only the required listener port and
   workload source rather than treating the endpoint as a broadly trusted
   subnet.
3. Use an endpoint policy only when the chosen service supports endpoint
   policies. It is an additional resource-based guardrail, so IAM and the
   target service's resource policy still need to allow the request.
4. For supported AWS services, private DNS can make the normal Regional service
   name resolve to the private endpoint addresses. It requires VPC DNS
   hostnames and DNS resolution. Otherwise, applications use the endpoint's
   service-specific DNS name.

Interface endpoints use AWS PrivateLink and can expose supported AWS services
or published endpoint services through private ENIs. For S3 and DynamoDB, they
can be used where the caller is on premises or must reach the endpoint from
another VPC/Region over the separately designed transport path. This is the
service-boundary difference from a gateway endpoint, not a reason to replace
all gateway endpoints.

### S3 and DynamoDB DNS boundary

Do not generalize private-DNS behavior across services:

- For S3, private DNS can keep ordinary Regional S3 names while a gateway
  endpoint carries in-VPC traffic. The S3 `private DNS only for inbound
  endpoint` option requires an S3 gateway endpoint and lets hybrid callers use
  the interface endpoint while in-VPC callers retain the unbilled gateway path.
- For DynamoDB, AWS documents endpoint-specific interface DNS names for
  PrivateLink access and says not to create private hosted-zone overrides for
  DynamoDB endpoint names. A combined gateway-plus-interface design keeps
  in-VPC callers on the gateway endpoint and directs hybrid callers to the
  interface endpoint's specific DNS name.
- For any other interface service, confirm that service's PrivateLink and
  private-DNS support before assuming the normal public service name becomes
  private. DNS naming does not add transport reachability or authorization.

## Cost Decision Gate

Use current prices for the selected Region in AWS Pricing Calculator; this
diagram deliberately records no fixed price because endpoint and NAT rates vary
by Region and can change.

```text
Gateway endpoint estimate = no additional endpoint charge
                          + ordinary service and applicable transfer charges

Interface endpoint estimate = endpoint types x selected AZs x monthly hours
                            x regional endpoint-hour rate
                            + GB processed x applicable PrivateLink tier
                            + applicable data transfer

NAT estimate = NAT Gateways x active AZs x monthly hours x regional NAT rate
             + GB processed x NAT processing rate
             + applicable data transfer
```

Compare only the destination traffic that a proposed endpoint would displace;
do not claim that an endpoint eliminates all NAT cost. One interface endpoint
type in several Availability Zones creates a charge for each endpoint-AZ hour,
even at low traffic volume. Conversely, high supported-service traffic through
NAT may justify a gateway endpoint or an interface endpoint after counting the
needed service types, AZ coverage, GB processed, and cross-AZ/cross-Region
transfer.

NAT remains justified when private workloads need shared outbound IPv4 access
to destinations that gateway or supported interface endpoints cannot serve, or
when a small, well-understood service set does not justify the cumulative cost
and operational ownership of interface endpoints. Place NAT Gateway capacity
and routes per Availability Zone when zonal independence is required; routing a
workload through another AZ can add both cost and a zonal dependency.

## SAP-C02 Decision Rules

| Requirement signal | Preferred answer | Reject the shortcut because |
|---|---|---|
| In-VPC, same-Region S3 or DynamoDB access | Gateway endpoint with the required route-table associations and policy boundary | NAT is broader and charged; an interface endpoint adds ENIs and charges without a stated hybrid/private-IP need. |
| On-premises, cross-VPC, or cross-Region caller needs S3 or DynamoDB privately | Interface endpoint after transport, service-DNS, security-group, and cost review | Gateway endpoints cannot extend outside their VPC route-table scope. |
| Private access to a supported AWS API or a provider-published service | Interface endpoint | A gateway endpoint only serves S3 and DynamoDB. |
| General outbound IPv4 access to unsupported public destinations | NAT Gateway after cost and resilience review | VPC endpoints are service-specific and do not provide general internet egress. |
| Requirement says "private" without naming destination or service support | Clarify the service, caller location, DNS, authorization, AZ, and cost requirements first | PrivateLink does not mean broad VPC-to-VPC routing, and a route alone is not authorization. |

## Acceptance Criteria Met by This Slice

- depicts S3/DynamoDB gateway route-table associations and their prefix-list
  route behavior;
- depicts interface endpoint ENIs, private-DNS decision boundary, security
  groups, and endpoint-policy boundary;
- separates gateway and interface service support from broader routing and
  hybrid transport;
- compares per-AZ interface endpoint-hour and data-processing charges with
  avoided NAT Gateway costs without using stale fixed prices; and
- preserves the documentation-only and unscored-learner-recall boundaries.

## References

- Gateway endpoint routing, security, scope, and pricing: `https://docs.aws.amazon.com/vpc/latest/privatelink/gateway-endpoints.html`
- Interface endpoint ENIs, security groups, endpoint policies, and private DNS: `https://docs.aws.amazon.com/vpc/latest/privatelink/interface-endpoints.html`
- S3 gateway and interface endpoint comparison, including hybrid private-DNS behavior: `https://docs.aws.amazon.com/AmazonS3/latest/userguide/privatelink-interface-endpoints.html`
- DynamoDB gateway and interface endpoint comparison, including DNS boundaries: `https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/privatelink-interface-endpoints.html`
- Endpoint-policy authorization boundary: `https://docs.aws.amazon.com/vpc/latest/privatelink/vpc-endpoints-access.html`
- NAT Gateway pricing and endpoint-first cost guidance: `https://docs.aws.amazon.com/vpc/latest/userguide/nat-gateway-pricing.html`
- AWS PrivateLink pricing: `https://aws.amazon.com/privatelink/pricing/`
