# SAP-C02 Gemini Flash Weak-Area Review - 2026-07-28

<!-- markdownlint-disable MD013 MD060 -->

**Last revised:** 2026-07-28<br>
**Document role:** source-backed external-assessment audit and bounded remediation note.<br>
**Evidence boundary:** the learner supplied six question/result extracts, not the complete assessment, timing, score, distractors, or answer key. This review does not create a practice score.

## Verdict

Do not memorize the supplied explanations as a set. Two are materially wrong,
one uses obsolete product terminology and overstates the agentless limitation,
and one needs a cost/performance qualification.

| Topic | Review verdict | Durable rule |
|---|---|---|
| Region-restriction SCP | Answer conclusion supported; explanation partly wrong | Explicitly exempt required global-service actions such as `route53:*` from the Region deny. IAM and CloudFront are not exempt “by default”; their global-endpoint actions normally need explicit exceptions too. |
| Direct Connect BGP | Supported and already understood | Longest-prefix match is evaluated before BGP path attributes. A more-specific prefix beats AS-path tuning on a less-specific route. |
| ARC routing control | Mechanism partly supported; “without DNS caching” premise false | ARC cluster endpoints provide a resilient way to change routing-control state. The state drives Route 53 health and DNS selection, so TTLs, caches, and existing connections still affect complete traffic movement. |
| DynamoDB global table | Supplied key is obsolete/incorrect | MREC permits a strong read from the local replica table but cannot guarantee freshness relative to a recent remote write. MRSC provides latest-value strong reads on any available replica. A GSI, not a replica table merely because it is global, rejects strong-read semantics. |
| EBS I/O bottleneck | `io2` Block Express can be correct, but the prompt is under-specified | Choose `io2` Block Express for the highest sustained performance, consistent sub-millisecond latency, or higher durability. If a `gp2` volume is only exhausting credits and the requirement fits `gp3`, provisioned `gp3` is usually the simpler cost-aware repair. |
| Migration dependency discovery | Agent answer fits the stated physical-server/deep-telemetry requirement; explanation is outdated if generalized | Discovery Agent captures processes and TCP connections on physical servers and VMs. Current Agentless Collector also has a network module for supported VMware-discovered servers; the older Agentless Discovery Connector name should not be generalized to current agentless capability. |

## Corrected Mental Models

### 1. Region restriction is endpoint-Region evaluation

```text
API request
    -> endpoint receives request in a Region
    -> aws:RequestedRegion evaluates that Region
    -> Deny applies unless Region is approved or action is in NotAction
```

The repository SCP already contains explicit `route53:*` and
`route53domains:*` exceptions. No policy JSON repair was required. If the
global endpoint's Region is itself allowed, the regional deny would not catch
that request; the explicit `NotAction` pattern avoids opening that Region for
unrelated Regional services. The study model was tightened so that “global
service” is never mistaken for “condition key ignored.”

### 2. Route selection starts with the destination prefix

```text
more-specific prefix
    -> wins first
equal prefix length
    -> then evaluate the documented BGP attributes
```

This was a correct answer, not a demonstrated weakness. The Direct Connect note
now makes the route-selection order explicit so it transfers to close
distractors involving AS-path prepending.

### 3. ARC improves failover control reliability, not DNS cache invalidation

```text
ARC data-plane endpoint
    -> routing-control state
    -> Route 53 health-check state
    -> DNS answer changes
    -> clients resolve or reconnect
```

ARC provides highly available cluster endpoints and safety rules for routing
control. It does not push a new address into every resolver or terminate every
existing connection. Low TTLs and bounded connection reuse remain part of the
RTO design.

### 4. DynamoDB consistency has two independent questions

Ask:

1. Is the read surface a table/LSI or a GSI/Stream?
2. Is the global table MREC or MRSC?

| Read | Result |
|---|---|
| MREC replica table with `ConsistentRead=true` | Valid local strong read; remote-region freshness is not guaranteed while replication converges |
| MRSC replica table with `ConsistentRead=true` | Latest committed item across the global table when the replica can service the strong read |
| GSI with `ConsistentRead=true` | Unsupported; GSIs provide eventual reads only |

### 5. EBS selection is requirements-led

Use CloudWatch evidence and workload requirements before choosing a volume:

- credit depletion on `gp2` identifies an unsustainable burst pattern;
- `gp3` removes the `gp2` credit dependency and decouples performance from size;
- `io2` Block Express wins when the database requires its sustained IOPS,
  throughput, latency, or durability envelope; and
- instance-level EBS bandwidth can remain the bottleneck after a volume change.

The phrase “storage is only 30% used” argues against buying capacity merely to
raise `gp2` baseline performance. It does not, by itself, distinguish a
right-sized `gp3` volume from `io2` Block Express.

### 6. Discovery choice depends on host type and evidence depth

For interconnected physical servers and detailed host/process/TCP dependency
evidence, install Discovery Agent. For supported VMware estates, evaluate the
current Agentless Collector modules, including Network Data Collection. Use
Migration Hub to organize and track the discovered migration portfolio; do not
describe it as the collector or migration engine.

For VMware, do not attribute connection-level dependency mapping to vCenter
APIs alone. The VMware module supplies inventory/profile/utilization data. The
separate Network Data Collection module uses the VMware-discovered server list
and collects connection evidence through WinRM for Windows or SNMP for Linux.
This installs no guest agent, but it still requires credentials and permitted
network access.

## Revision Actions

Updated immediately:

- canonical SCP, networking, Route 53/ARC, DynamoDB, storage, and migration
  explanations;
- the compact revision pack chapters for the same topics; and
- the wrong-answer log only for the genuine physical-server discovery miss.

No extra retest is inserted before the pending non-relational database spaced
retest or full mock 002. Test these corrections through the normal two-mock
weekly cadence. If ARC/DNS, Region-SCP exemptions, or discovery tooling recurs
as a miss, generate one small mixed free-response retest after the mock review.

## Official AWS References

- [IAM requested-Region policy example](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_examples_aws_deny-requested-region.html)
- [Direct Connect routing policies and BGP communities](https://docs.aws.amazon.com/directconnect/latest/UserGuide/routing-and-bgp.html)
- [ARC routing-control behaviour](https://docs.aws.amazon.com/r53recovery/latest/dg/routing-control.about.html)
- [ARC routing-control best practices](https://docs.aws.amazon.com/r53recovery/latest/dg/route53-arc-best-practices.regional.html)
- [DynamoDB read consistency](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html)
- [DynamoDB global-table consistency modes](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/V2globaltables_HowItWorks.html)
- [Amazon EBS General Purpose SSD volumes](https://docs.aws.amazon.com/ebs/latest/userguide/general-purpose.html)
- [Amazon EBS Provisioned IOPS SSD volumes](https://docs.aws.amazon.com/ebs/latest/userguide/provisioned-iops.html)
- [Application Discovery Service Agentless Collector](https://docs.aws.amazon.com/application-discovery/latest/userguide/agentless-collector.html)
- [AWS Application Discovery Agent](https://docs.aws.amazon.com/application-discovery/latest/userguide/discovery-agent.html)
- [Application Discovery Service network-dependency data](https://docs.aws.amazon.com/application-discovery/latest/userguide/view-and-explore.html)
