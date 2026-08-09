# 03 - Load Balancing, DNS, Edge, and Global Traffic

**Last revised:** 2026-08-09

This chapter covers **ALB**, **NLB**, **Gateway Load Balancer**, **Route 53**, **CloudFront**, and **AWS Global Accelerator**. SAP-C02 frequently tests these as pairs.

## Mental model

| Need | Service |
|---|---|
| DNS name and routing policy | Route 53 |
| HTTP/HTTPS Layer 7 routing | Application Load Balancer (ALB) |
| TCP/UDP/TLS Layer 4 load balancing and regional static IPs | Network Load Balancer (NLB) |
| Transparent network appliance insertion | Gateway Load Balancer (GWLB) |
| Edge caching, TLS, WAF integration, content acceleration | CloudFront |
| Global static anycast IPs, traffic acceleration to regional endpoints | AWS Global Accelerator |
| API lifecycle, auth, throttling, request transformation | API Gateway |

## Route 53

### What it does

Route 53 is DNS. It answers DNS queries using hosted zones and routing policies.

It is **not** in the HTTP request path after DNS resolution. It does not cache web content, inspect HTTP headers, terminate TLS, or route by URL path.

### Routing policies

| Policy | Use when | Common trap |
|---|---|---|
| Simple | One answer for a name | No health-based intelligent routing unless alias behavior applies |
| Weighted | Controlled traffic split, canary, blue/green by DNS | DNS caching means split is approximate |
| Latency | Route to lowest-latency AWS region | Not a health-only policy; combine with health checks |
| Failover | Active-passive | Cutover affected by TTL/resolvers |
| Geolocation | Route by user geography | Based on DNS resolver/user location signals, not application identity |
| Geoproximity | Route by resource/user location with traffic bias | More advanced, often with Traffic Flow |
| Multivalue answer | Return up to several healthy records | Not a substitute for ELB |
| IP-based | Route based on client CIDR collections | Useful for known network ranges |

### Route 53 exam traps

- Route 53 is not a proxy.
- TTL matters. Failover is not instantaneous for all clients.
- Weighted routing is not a load balancer.
- Geolocation is based on location, not latency.
- Latency routing is based on latency, not legal residency.
- Private hosted zones are for VPC-internal DNS.
- Alias records are used for AWS resources such as ELB, CloudFront, S3 website endpoints, and API Gateway custom domains.

### Public apex name to multi-Region ALBs

For an internet application reached through the zone apex, use a **public**
hosted zone and Route 53 **alias A/AAAA records**, not CNAME records. A CNAME
cannot be created at the zone apex, while an alias can point the apex to an
AWS load balancer.

For active-active ALBs in multiple Regions:

```text
public apex alias records
  + latency routing
  + Evaluate Target Health = Yes
  -> lowest-latency healthy Regional ALB
```

`Evaluate Target Health` derives ALB health from its target groups. A private
hosted zone is not suitable for public customers, and latency routing without
health evaluation can keep selecting the closest unhealthy Regional path.

### Application Recovery Controller routing controls

Amazon Application Recovery Controller (ARC) routing controls are highly available on/off switches hosted on ARC clusters. Updating a routing control changes an associated Route 53 health-check state, which changes the eligible DNS answer for a failover record.

The cluster endpoints make the control action highly available; they do **not** bypass DNS. TTLs, resolver/client caching, connection reuse, and existing long-lived connections can delay complete traffic movement. If the decisive requirement is static anycast IPs and failover that is not driven by DNS-cache expiry, compare Global Accelerator.

Trap: ARC routing controls drive Route 53 DNS failover. They do not make client-side DNS caching irrelevant.

Readiness checks and routing controls have different jobs:

```text
readiness check  -> assesses whether recovery resources/configuration are ready
routing control  -> changes the Route 53 health-check state used to move traffic
```

Readiness checks do not reroute traffic. When several CloudWatch metrics define
whether a Regional microservice deployment is functional, alarms can invoke a
Lambda decision path that toggles ARC routing controls. ARC safety rules should
guard against unsafe combinations such as turning every cell off.

## Application Load Balancer

### What it does

ALB is a Layer 7 load balancer for HTTP/HTTPS traffic. It routes requests to target groups.

### Choose ALB when

- host-based routing is required
- path-based routing is required
- HTTP headers/query/method/source IP conditions matter
- microservices route by `/orders`, `/payments`, `/users`
- WebSocket or HTTP/2 support is needed
- Lambda targets are useful
- AWS WAF integration at regional application entry point is required
- ECS/Fargate services need HTTP load balancing

### Target types

| Target type | Use case |
|---|---|
| Instance | EC2 instances registered by instance ID |
| IP | ECS tasks, on-prem IPs reachable over hybrid network, cross-VPC targets in supported patterns |
| Lambda | HTTP request invokes Lambda function |
| ALB target behind NLB | Used when combining NLB static IP/PrivateLink style entry with ALB Layer 7 routing |

### ALB patterns

```text
Route 53
  -> CloudFront + WAF
  -> ALB
  -> Target groups:
       /api/*      -> ECS Fargate service
       /admin/*    -> EC2 ASG
       /webhook/*  -> Lambda target
```

### ALB traps

- ALB does not support UDP.
- ALB is not the right answer for regional static IP requirements.
- ALB health check path must match the application’s actual health endpoint.
- ALB cross-zone behavior and target health affect traffic distribution.
- Security groups must allow traffic from ALB to targets.
- For ECS/Fargate, the target group usually uses IP targets because Fargate tasks get elastic network interfaces.

## Network Load Balancer

### What it does

NLB is a Layer 4 load balancer for TCP/UDP/TLS-style workloads. It is built for very high throughput and low latency.

### Choose NLB when

- TCP, UDP, TLS, or very high network throughput is required
- static regional IP addresses are needed
- Elastic IPs per Availability Zone are needed
- PrivateLink endpoint service provider pattern is required
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

### NLB traps

- NLB does not route by HTTP path or host header.
- NLB does not provide HTTP-aware WAF integration like ALB/CloudFront.
- NLB health checks must be configured correctly; unhealthy targets are removed.
- Static IP requirement may point to NLB for regional Layer 4, but to Global Accelerator for global anycast.

## Gateway Load Balancer

### What it does

GWLB helps deploy and scale third-party network appliances such as firewalls, intrusion detection/prevention systems, and deep packet inspection appliances.

### Choose GWLB when

- traffic must be transparently steered through security appliances
- appliance fleet scaling and health checks are needed
- centralized inspection VPC pattern is required
- Geneve encapsulation/appliance insertion is part of the architecture

### Trap

Do not choose ALB or NLB for transparent packet inspection appliances. ALB/NLB route application or transport traffic; GWLB is designed for appliance insertion.

## CloudFront

### What it does

CloudFront is a content delivery network (CDN). It caches and accelerates HTTP/HTTPS content at edge locations.

### Choose CloudFront when

- global users need lower latency for web content
- static content should be cached near users
- dynamic content should use optimized edge network path
- AWS WAF should be applied at the edge
- S3 origin should be private via Origin Access Control
- origin failover is required between origins
- signed URLs/cookies are needed for restricted content

### CloudFront vs Route 53 vs Global Accelerator

| Requirement | Best fit |
|---|---|
| DNS routing decision | Route 53 |
| Edge HTTP cache and CDN | CloudFront |
| Global static anycast IPs for TCP/UDP/HTTP endpoints | Global Accelerator |
| HTTP path routing to microservices | ALB |
| Regional Layer 4 load balancing/static IP | NLB |

### CloudFront traps

- CloudFront caches content; Route 53 does not.
- CloudFront is HTTP/HTTPS-focused; it is not a general TCP/UDP accelerator.
- Cache behavior, TTLs, query strings, cookies, and headers determine cache hit ratio.
- Use Origin Access Control to keep S3 origins private.
- Use WAF at CloudFront for global edge protection of HTTP apps.

### Device-specific static content at the edge

When static website requests differ by device type and origin servers are
overloaded, move the static assets to S3, cache them through CloudFront, and use
an edge request function to select the appropriate object path. Lambda@Edge can
inspect viewer/device headers and rewrite the request URI so CloudFront returns
the mobile, tablet, television, or desktop object variant.

Configure the cache key consistently with the device classification. Otherwise
one device variant can be cached and served to another class of viewer.

Do not select these distractors:

- Route 53 cannot route on `User-Agent`; it receives DNS queries, not HTTP
  headers.
- NLB cannot route on `User-Agent`; it is a Layer 4 load balancer.
- Additional ALBs and Auto Scaling groups keep static delivery on EC2 and do
  not remove the origin-load problem.

CloudFront Functions can perform lightweight viewer-request header and URI
logic in modern designs. Lambda@Edge remains the answer when it is the offered
edge-compute mechanism or the required processing exceeds CloudFront Functions
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

| Origin | CloudFront-only pattern | Why nearby answers lose |
|---|---|---|
| Private S3 bucket | Attach Origin Access Control (OAC); use a bucket policy that allows the CloudFront service principal and scopes `AWS:SourceArn` to the distribution | A bucket ACL is not the modern distribution-scoped authorization mechanism; OAI is legacy |
| Internet-facing ALB | Configure CloudFront to add a random custom origin header; validate its name and value at the ALB boundary using an ALB listener rule or a regional WAF web ACL associated with the ALB; reject requests without it | A WAF web ACL associated only with CloudFront does not inspect callers that bypass CloudFront and address the ALB directly |
| Internal ALB/NLB/EC2 origin | Use a CloudFront VPC origin where its supported protocols/features fit | This removes the public origin path rather than authenticating requests to a public endpoint |

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
different authorization requirements in the bucket policies. The exact OAC
policy principal is the CloudFront service principal
`cloudfront.amazonaws.com`, constrained with `AWS:SourceArn` to the distribution
ARN. OAC itself and the distribution ARN are not IAM principals. A bucket that
also permits approved AWS roles/resources includes those principals separately;
a CloudFront-only bucket omits them. The bucket owner retains administrative
control through account/IAM authority rather than a special OAC exception.

## AWS Global Accelerator

### What it does

Global Accelerator provides static anycast IPs and routes user traffic over the AWS global network to healthy regional endpoints.

### Choose it when

- applications need static global anycast IPs
- fast regional failover is required without DNS TTL dependency
- TCP/UDP traffic needs acceleration
- endpoints are ALB, NLB, EC2, or Elastic IPs
- users are global and latency/reliability matter

### Avoid it when

- caching content is the requirement -> CloudFront
- DNS routing policy alone is enough -> Route 53
- HTTP path routing is the requirement -> ALB
- you need API auth/throttling/model validation -> API Gateway

For latency-sensitive global multiplayer traffic, the explicit **UDP** cue
eliminates CloudFront and Lambda@Edge. Deploy game endpoints in multiple
Regions and use Global Accelerator to accept UDP on static anycast addresses
and carry traffic over the AWS global network to a healthy Regional endpoint.
Route 53 latency routing is DNS-based and does not provide the same request
path or static anycast entry point.

### Lambda@Edge dynamic origin selection

An origin-request Lambda@Edge function can choose between origins from request
attributes such as cookies, headers and `CloudFront-Viewer-Country`. This
supports a single distribution where geography supplies the default website
version and an allowlisted user/cookie overrides that default.

CloudFront adds its viewer-location headers after the viewer-request stage, so
country-based origin selection belongs on an **origin-request** trigger. Keep
the selected origin and cache-key design aligned so that one audience does not
receive another audience's cached version.

## Common combined architectures

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

### DNS-based regional failover

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

## Exam traps

| Trap | Correction |
|---|---|
| “Route 53 provides failover, therefore zero downtime” | DNS TTL/resolver behavior affects cutover. |
| “Weighted routing is exact traffic control” | It is DNS answer weighting, not request-level routing. |
| “ALB is enough for static IPs” | Use NLB for regional static IPs or Global Accelerator for global static anycast IPs. |
| “NLB can do path routing” | Use ALB. |
| “CloudFront is the same as Global Accelerator” | CloudFront caches HTTP content; Global Accelerator accelerates traffic to endpoints without caching. |
| “Use CloudFront for raw TCP” | Use Global Accelerator/NLB depending scope. |
| “Use Route 53 for `/api` vs `/static` routing” | Use ALB or CloudFront cache behaviors; DNS does not see URL paths. |
| “WAF at CloudFront prevents direct ALB access” | Direct ALB requests bypass that web ACL; validate the CloudFront-added secret at the ALB boundary or use a private VPC origin. |
| “S3 ACL allows only one CloudFront distribution” | Use OAC and a distribution-scoped bucket policy. |
| “Route 53 or NLB can select content by `User-Agent`” | HTTP-header decisions require an HTTP-aware edge or Layer 7 component; use CloudFront edge logic or ALB according to where the content lives. |

## Source references

- Route 53 routing policies: https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/routing-policy.html
- ALB target groups: https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-target-groups.html
- NLB introduction: https://docs.aws.amazon.com/elasticloadbalancing/latest/network/introduction.html
- Restrict CloudFront access to an ALB: https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/restrict-access-to-load-balancer.html
- Restrict CloudFront access to an S3 origin: https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/private-content-restricting-access-to-s3.html
- Lambda@Edge usage patterns: https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/lambda-edge-ways-to-use.html
