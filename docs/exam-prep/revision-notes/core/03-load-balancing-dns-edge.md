# 03 - Load Balancing, DNS, Edge, and Global Traffic

**Last revised:** 2026-07-28

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

### Application Recovery Controller routing controls

Amazon Application Recovery Controller (ARC) routing controls are highly available on/off switches hosted on ARC clusters. Updating a routing control changes an associated Route 53 health-check state, which changes the eligible DNS answer for a failover record.

The cluster endpoints make the control action highly available; they do **not** bypass DNS. TTLs, resolver/client caching, connection reuse, and existing long-lived connections can delay complete traffic movement. If the decisive requirement is static anycast IPs and failover that is not driven by DNS-cache expiry, compare Global Accelerator.

Trap: ARC routing controls drive Route 53 DNS failover. They do not make client-side DNS caching irrelevant.

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

## Source references

- Route 53 routing policies: https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/routing-policy.html
- ALB target groups: https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-target-groups.html
- NLB introduction: https://docs.aws.amazon.com/elasticloadbalancing/latest/network/introduction.html
