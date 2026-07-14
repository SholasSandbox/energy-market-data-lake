# Domain 2 NAT Gateway Cost Warning - 2026-07-15

<!-- markdownlint-disable MD013 -->

## Accepted Position

NAT Gateway is a last-resort shared IPv4 egress path for this case study, not
the default way to reach AWS services privately. Prefer the least-cost pattern
that meets the routing and security requirement:

1. no egress path when the workload does not require one;
2. gateway VPC endpoints for eligible Amazon S3 and DynamoDB access;
3. interface VPC endpoints for supported services when a documented private
   access, security, or cost case justifies them;
4. an egress-only internet gateway for appropriate outbound-only IPv6 designs;
5. NAT Gateway only for required IPv4 destinations that the narrower options
   cannot serve.

This is a documentation-only study decision. It does not authorize a NAT
Gateway, VPC endpoint, route-table, subnet, internet gateway, logging, or cost
management change in AWS.

## Why NAT Gateway Requires a Cost Gate

NAT Gateway charges combine provisioned gateway-hours and processed data. A
resilient zonal design can require one gateway per active Availability Zone,
while routing traffic across Availability Zones can add avoidable cost and a
zonal dependency. Sending S3, DynamoDB, or supported AWS-service traffic
through NAT can therefore pay for broad egress when a narrower endpoint would
meet the requirement.

Interface endpoints are not automatically cheaper. They charge for every
provisioned endpoint-hour in each selected Availability Zone and for processed
data. Several low-volume service endpoints across several Availability Zones
can cost more than one shared NAT path. The comparison must use the actual
service count, Availability Zone count, traffic, and current regional prices.

Gateway endpoints for S3 and DynamoDB have no additional endpoint charge and
are the first cost comparison for eligible same-VPC traffic.

## Prechange Cost Decision

Before approving NAT Gateway or an interface endpoint, record:

- the destination services and whether gateway/interface endpoints support
  them;
- the number of required NAT Gateways or service endpoints;
- the Availability Zones in which each hourly resource will be provisioned;
- estimated monthly ingress and egress gigabytes by destination;
- cross-Availability-Zone or cross-Region paths;
- availability, encryption, DNS, and endpoint-policy requirements; and
- the current regional hourly, data-processing, and transfer rates.

Use these formulas as a comparison structure, not as fixed price quotations:

```text
NAT estimate = NAT gateways x monthly hours x NAT hourly rate
             + GB processed x NAT processing rate
             + applicable data-transfer charges

Interface endpoint estimate = sum of endpoint-AZ hours x endpoint hourly rate
                            + GB processed x PrivateLink processing tier
                            + applicable cross-Region transfer

Gateway endpoint estimate = no additional endpoint charge
                          + ordinary service and applicable transfer charges
```

Use AWS Pricing Calculator for the prechange forecast. Preserve the input
assumptions so actual usage can be compared with the forecast later.

## Postchange Evidence Gate

If a NAT Gateway or interface endpoint is ever approved and deployed:

1. review actual Cost Explorer cost and usage after 7 to 14 days;
2. repeat the comparison after the first 30 days;
3. continue monthly only while the cost is material or traffic is volatile;
4. avoid permanent biweekly automation unless observed spend or operational
   scale justifies maintaining it; and
5. use Cost and Usage Report or Data Export detail only when Cost Explorer
   cannot isolate the relevant service, usage type, Availability Zone, or
   account evidence.

The current repository baseline does not provide evidence of a NAT Gateway or
interface endpoint deployment. Therefore, no recurring analyzer or live cost
tool change is justified by this study artifact.

## Routing and Resilience Warnings

- A NAT Gateway provides outbound translation; it is not a private service
  endpoint and does not permit unsolicited inbound connections.
- Route each Availability Zone to its own NAT Gateway when the requirement
  justifies zonal independence; do not silently create a cross-AZ dependency.
- Endpoint policies, security groups, DNS behavior, and service support must be
  checked separately from price.
- Do not route S3 or DynamoDB through NAT when a gateway endpoint meets the
  access requirement.
- Do not create many interface endpoints merely because they are private;
  compare their cumulative endpoint-AZ hours with the avoided NAT cost.

## SAP-C02 Decision Rules

| Scenario signal | Preferred answer |
|---|---|
| Private S3 or DynamoDB access from a VPC | Gateway endpoint, subject to route-table and endpoint-policy design |
| Private access to a supported AWS API without broad internet egress | Interface endpoint, after cost and Availability Zone review |
| General outbound IPv4 access to unsupported public destinations | NAT Gateway when the requirement and cost gate justify it |
| Outbound-only IPv6 internet access | Egress-only internet gateway where the workload supports IPv6 |
| High NAT spend dominated by supported AWS-service traffic | Move eligible traffic to endpoints, then measure the realized savings |

## Acceptance Criteria Met by This Slice

- records the endpoint-first design hierarchy;
- distinguishes gateway endpoint, interface endpoint, and NAT charging models;
- defines prechange forecast inputs and 7-to-14-day/30-day evidence gates;
- records zonal routing, resilience, and SAP-C02 traps; and
- preserves the no-AWS-implementation boundary.

Scenario-drill review remains open before the tracker deliverable is treated as
fully complete.

## References

- NAT Gateway pricing guidance:
  `https://docs.aws.amazon.com/vpc/latest/userguide/nat-gateway-pricing.html`
- Gateway endpoint pricing and behavior:
  `https://docs.aws.amazon.com/vpc/latest/privatelink/gateway-endpoints.html`
- AWS PrivateLink interface endpoint pricing:
  `https://aws.amazon.com/privatelink/pricing/`
- AWS Cost Explorer:
  `https://docs.aws.amazon.com/cost-management/latest/userguide/ce-what-is.html`
- AWS Cost and Usage Reports:
  `https://docs.aws.amazon.com/cur/latest/userguide/what-is-cur.html`
