# 03 - Load Balancing, DNS, Edge, and Global Traffic

**Last revised:** 2026-08-12

This chapter covers **ALB**, **NLB**, **Gateway Load Balancer**, **Route 53**,
**CloudFront**, and **AWS Global Accelerator**. SAP-C02 frequently tests these
as pairs.

## Mental model

| Need | Service |
| --- | --- |
| DNS name and policy | Route 53 |
| HTTP/HTTPS routing | ALB |
| TCP/UDP/TLS routing | NLB |
| Appliance insertion | GWLB |
| Edge caching and WAF | CloudFront |
| Global anycast IPs | Global Accelerator |
| API management | API Gateway |

## Route 53

### Service model

Route 53 is DNS. It answers DNS queries using hosted zones and routing
policies.

It is **not** in the HTTP request path after DNS resolution. It does not cache
web content, inspect HTTP headers, terminate TLS, or route by URL path.

### Routing policies

| Policy | Use when | Common trap |
| --- | --- | --- |
| Simple | One answer | No health logic |
| Weighted | Canary or split traffic | TTL makes the split approximate |
| Latency | Lowest-latency region | Not a health-only choice |
| Failover | Active-passive | Cutover depends on TTL |
| Geolocation | Route by user geography | Not app identity |
| Geoproximity | Route by resource and user | Needs Traffic Flow |
| Multivalue answer | Several healthy answers | Not a substitute for ELB |
| IP-based | Known client CIDR ranges | Useful for network ranges |

### Exam traps

- Route 53 is not a proxy.
- TTL matters. Failover is not instantaneous for all clients.
- Weighted routing is not a load balancer.
- Geolocation is based on location, not latency.
- Latency routing is based on latency, not legal residency.
- Private hosted zones are for VPC-internal DNS.
- Alias records are used for ELB, CloudFront, S3 website endpoints, and API
  Gateway custom domains.

### Public apex pattern

For an internet app reached through the zone apex, use a **public** hosted
zone and Route 53 **alias A/AAAA records**, not CNAME records. A CNAME cannot
be created at the zone apex, while an alias can point the apex to an AWS load
balancer.

For active-active ALBs in multiple Regions:

```text
public apex alias records
  + latency routing
  + Evaluate Target Health = Yes
  -> lowest-latency healthy Regional ALB
```

`Evaluate Target Health` derives ALB health from target groups. A private
hosted zone is not suitable for public customers, and latency routing without
health evaluation can keep selecting the closest unhealthy Regional path.

### ARC routing controls

ARC routing controls are highly available on/off switches hosted on ARC
clusters. Updating a routing control changes the associated Route 53 health
check, which changes the eligible DNS answer for a failover record.

#### Control plane versus data plane

Treat ordinary Route 53 configuration calls, such as creating or changing DNS
records and health checks, as **control-plane operations**. Do not make an
incident failover depend only on the normal Route 53 configuration API being
available.

An ARC cluster provides a separate, highly available **data plane** with
endpoints in five AWS Regions. During failover, operators or automation call an
available cluster endpoint to update routing-control state. The associated
routing-control health check then makes the configured Route 53 failover record
eligible or ineligible. The data-plane call does not recreate or directly edit
the DNS record.

```text
normal configuration
    -> Route 53 / ARC control plane
    -> create/modify records, health checks, clusters, and routing controls

incident traffic switch
    -> ARC cluster data-plane endpoint
    -> update routing-control state
    -> routing-control health-check state
    -> Route 53 failover record eligibility
```

The ARC data plane makes the **control action** highly available; it does
**not** bypass DNS. TTLs, resolver/client caching, connection reuse, and
long-lived connections can delay complete traffic movement. If the decisive
requirement is static anycast IPs and failover not driven by DNS expiry,
compare Global Accelerator.

Trap: ARC routing controls drive Route 53 DNS failover. They do not make
client-side DNS caching irrelevant.

Readiness checks and routing controls have different jobs:

```text
readiness check -> recovery resources and config are ready
routing control -> changes Route 53 health-check state used to move traffic
```

Readiness checks do not reroute traffic. When several CloudWatch metrics define
whether a regional microservice deployment is functional, alarms can invoke a
Lambda decision path that toggles ARC routing controls. ARC safety rules should
guard against unsafe combinations such as turning every cell off.

## Application Load Balancer

### ALB service model

ALB is a Layer 7 load balancer for HTTP/HTTPS traffic. It routes requests to
target groups.

### Choose ALB when

- host-based routing is required
- path-based routing is required
- HTTP headers, query strings, methods, and source IP conditions matter
- microservices route by `/orders`, `/payments`, and `/users`
- WebSocket or HTTP/2 is needed
- Lambda targets are useful
- AWS WAF integration at the regional app entry point is required
- ECS/Fargate services need HTTP load balancing

### Target types

| Target type | Use case |
| --- | --- |
| Instance | EC2 instances by instance ID |
| IP | ECS tasks, on-prem IPs, or cross-VPC targets |
| Lambda | HTTP request invokes Lambda |
| ALB behind NLB | NLB entry with ALB Layer 7 routing |

### Patterns

```text
Route 53
  -> CloudFront + WAF
  -> ALB
  -> Target groups:
       /api/* -> ECS Fargate service
       /admin/* -> EC2 ASG
       /webhook/* -> Lambda target
```

### ALB main traps

- ALB does not support UDP.
- ALB is not the right answer for regional static IP requirements.
- ALB health check path must match the real health endpoint.
- Cross-zone behavior and target health affect traffic distribution.
- Security groups must allow traffic from ALB to targets.
- For ECS/Fargate, the target group usually uses IP targets.

## Network Load Balancer

### NLB service model

NLB is a Layer 4 load balancer for TCP/UDP/TLS workloads. It is built for very
high throughput and low latency.

### Choose NLB when

- TCP, UDP, TLS, or very high throughput is required
- static regional IPs are needed
- Elastic IPs per AZ are needed
- PrivateLink provider patterns are required
- source IP preservation matters
- ALB Layer 7 features are not required

### Common patterns

```text
Clients
  -> NLB with static IPs
  -> TCP service on EC2/ECS
```

```text
Consumer VPC
  -> Interface VPC Endpoint
  -> PrivateLink
  -> Provider NLB
  -> Private service
```

### NLB main traps

- NLB does not route by HTTP path or host header.
- NLB has no HTTP-aware WAF integration like ALB or CloudFront.
- Health checks must be correct; unhealthy targets are removed.
- Static IP requirement can point to NLB regional Layer 4 or to Global
  Accelerator for global anycast.

## Gateway Load Balancer

### GWLB service model

GWLB helps deploy and scale third-party network appliances such as firewalls,
IDS/IPS systems, and deep packet inspection tools.

### Choose GWLB when

- traffic must be steered through security appliances
- appliance fleet scaling and health checks are needed
- centralized inspection VPC patterns are required
- Geneve encapsulation or appliance insertion is part of the design

### Trap

Do not choose ALB or NLB for transparent packet inspection appliances.
ALB/NLB route application or transport traffic; GWLB is designed for appliance
insertion.

## CloudFront

### CloudFront service model

CloudFront is a content delivery network. It caches and accelerates HTTP/HTTPS
content at edge locations.

### Choose CloudFront when

- global users need lower latency for web content
- static content should be cached near users
- dynamic content should use an optimized edge path
- AWS WAF should be applied at the edge
- S3 origin should be private via Origin Access Control
- origin failover is required between origins
- signed URLs or cookies are needed for restricted content

### Compare with Route 53 and Global Accelerator

| Requirement | Best fit |
| --- | --- |
| DNS routing decision | Route 53 |
| Edge HTTP cache and CDN | CloudFront |
| Global static anycast IPs | Global Accelerator |
| HTTP path routing | ALB |
| Regional Layer 4 static IP | NLB |

### CloudFront main traps

- CloudFront caches content; Route 53 does not.
- CloudFront is HTTP/HTTPS-focused; it is not a general TCP/UDP accelerator.
- Cache behavior, TTLs, query strings, cookies, and headers determine hit
  ratio.
- Use OAC to keep S3 origins private.
- Use WAF at CloudFront for global edge protection of HTTP apps.

### Device-specific content at the edge

When static website requests differ by device type and origin servers are
overloaded, move the static assets to S3, cache them through CloudFront, and use
an edge request function to select the right object path. Lambda@Edge can
inspect viewer/device headers and rewrite the request URI so CloudFront returns
the mobile, tablet, television, or desktop variant.

Configure the cache key consistently with the device classification. Otherwise,
one device variant can be cached and served to another viewer class.

Do not select these distractors:

- Route 53 cannot route on `User-Agent`; it receives DNS queries, not HTTP
  headers.
- NLB cannot route on `User-Agent`; it is a Layer 4 load balancer.
- Additional ALBs and Auto Scaling groups keep static delivery on EC2 and do
  not remove the origin-load problem.

CloudFront Functions can do lightweight viewer-request header and URI logic in
modern designs. Lambda@Edge remains the answer when it is the offered
edge-compute mechanism or the required processing exceeds CloudFront function
capabilities.

### Restrict direct access to CloudFront origins

Different origin types require different controls:

```text
Viewer
  -> CloudFront
       -> OAC-signed request -> private S3 REST origin
       -> secret header over HTTPS -> public ALB origin
       -> VPC origin -> internal ALB/NLB/EC2 origin
```

| Origin | Pattern | Why nearby answers lose |
| --- | --- | --- |
| Private S3 bucket | OAC policy | ACLs are legacy |
| Internet-facing ALB | Custom header | Direct ALB callers bypass WAF |
| Internal ALB/NLB/EC2 | VPC origin | Public path removed |

For the public-ALB header pattern, require HTTPS from CloudFront to the origin,
treat the header as a credential, rotate it, and optionally restrict the ALB
security group to the AWS-managed CloudFront origin-facing prefix list. The
header is a shared secret, not cryptographic proof of origin.

Exam direction rule:

```text
CloudFront adds the origin header.
The ALB boundary validates it.
```

Do not reverse those actions. CloudFront viewer-facing WAF and ALB
origin-protection WAF solve different problems.

For multiple private S3 origins, associate OAC with each origin and express the
different authorization requirements in bucket policies. The exact OAC policy
principal is the CloudFront service principal `cloudfront.amazonaws.com`,
constrained with `AWS:SourceArn` to the distribution ARN. OAC itself and the
distribution ARN are not IAM principals. A bucket that also permits approved
AWS roles or resources includes those principals separately. A CloudFront-only
bucket omits them. The bucket owner retains administrative control through
account and IAM authority rather than a special OAC exception.

## AWS Global Accelerator

### Global Accelerator service model

Global Accelerator provides static anycast IPs and routes user traffic over the
AWS global network to healthy regional endpoints.

### Choose it when

- applications need static global anycast IPs
- fast regional failover is required without DNS TTL dependency
- TCP/UDP traffic needs acceleration
- endpoints are ALB, NLB, EC2, or Elastic IPs
- users are global and latency/reliability matter

### Avoid it when

- caching content is the requirement -> CloudFront
- DNS policy alone is enough -> Route 53
- HTTP path routing is the requirement -> ALB
- API auth or throttling is the main requirement -> API Gateway

For latency-sensitive global multiplayer traffic, the explicit **UDP** cue
eliminates CloudFront and Lambda@Edge. Deploy game endpoints in multiple
Regions and use Global Accelerator to accept UDP on static anycast addresses
and carry traffic over the AWS global network to a healthy Regional endpoint.
Route 53 latency routing is DNS-based and does not provide the same request
path or static anycast entry point.

### Lambda@Edge origin selection

An origin-request Lambda@Edge function can choose between origins from request
attributes such as cookies, headers, and `CloudFront-Viewer-Country`. This
supports a single distribution where geography supplies the default website
version and an allowlisted user or cookie overrides that default.

CloudFront adds viewer-location headers after the viewer-request stage, so
country-based origin selection belongs on an **origin-request** trigger. Keep
the selected origin and cache-key design aligned so one audience does not
receive another audience's cached version.

## Common architectures

### Public web app, global users

```text
Route 53
  -> CloudFront + WAF
  -> ALB
  -> ECS Fargate / EC2 ASG
  -> RDS/Aurora/DynamoDB
```

### Low-latency global active-passive

```text
Global Accelerator
  -> Region A ALB/NLB
  -> Region B ALB/NLB
  -> health checks and endpoint weights
```

### DNS failover

```text
Route 53 failover record
  -> Primary: ALB in Region A
  -> Secondary: ALB/static site/API in Region B
```

### Private service exposure

```text
Consumer VPC
  -> Interface endpoint
  -> PrivateLink
  -> Provider NLB
  -> ECS/EC2 service
```

## Exam trap recap

| Trap | Correction |
| --- | --- |
| “Zero downtime” | TTL and resolver caching still matter. |
| “Exact weighted control” | DNS weighting is estimate-based. |
| “ALB gives static IPs” | Use NLB or Global Accelerator. |
| “NLB does path routing” | Use ALB. |
| “CloudFront = Global Accelerator” | Different jobs and control points. |
| “CloudFront for raw TCP” | Use NLB or Global Accelerator. |
| “DNS routes `/api`” | URL routing is ALB or CloudFront. |
| “CloudFront WAF blocks ALB” | Direct ALB calls bypass it. |
| “S3 ACL is one distribution” | Use OAC and a bucket policy. |
| “NLB sees `User-Agent`” | HTTP decisions need edge logic or ALB. |

## Source references

- Route 53 routing policies:
  <https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/routing-policy.html>
- ALB target groups:
  <https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-target-groups.html>
- NLB introduction:
  <https://docs.aws.amazon.com/elasticloadbalancing/latest/network/introduction.html>
- Restrict CloudFront access to an ALB:
  <https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/restrict-access-to-load-balancer.html>
- Restrict CloudFront access to an S3 origin:
  <https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/private-content-restricting-access-to-s3.html>
- Lambda@Edge usage patterns:
  <https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/lambda-edge-ways-to-use.html>
