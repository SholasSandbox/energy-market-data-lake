# Domain 2 RTO/RPO Decision Table - 2026-07-18

<!-- markdownlint-disable MD013 MD060 -->

## Purpose and Boundary

**Document role:** business-led decision worksheet and SAP-C02 revision aid.
Use it with the
[Resilience and DR key lesson](../exam-prep/aws-resilience-dr-sap-c02-key-lessons-20260718.md),
which contains the companion DR pattern matrix.

This table helps turn business impact into recovery objectives, then checks
whether dependencies, recovery mechanisms, and tested results can meet them.
It does not approve numeric objectives, select a live Lakehouse DR pattern, or
authorize AWS changes.

## The Three Values That Must Stay Separate

| Value | Meaning | Owner or evidence source | Status rule |
|---|---|---|---|
| Business objective | Maximum acceptable outage (**RTO**) and maximum acceptable age of the last recoverable data point (**RPO**) | Business owner, risk owner, legal/compliance stakeholders, and workload owner | Approved only when accountable stakeholders accept the impact and cost trade-off |
| Designed recovery capability | What the proposed architecture and runbook are expected to achieve | Architecture, service behavior, quotas, automation, dependency analysis, and test plan | Estimate or hypothesis until tested end to end |
| Tested recovery result | Actual elapsed recovery time and actual age of the restored recovery point under a stated failure scenario | Timestamped restore/failover exercise and business validation | Evidence only for the tested scope, scale, dependencies, and conditions |

Do not report a service feature, backup frequency, replication target, or
vendor claim as the workload's RTO or RPO. The objective is the business need;
the tested result is evidence about whether the implemented design meets it.

## Business-Impact Inputs

Do not assign an RTO/RPO tier until the following questions have answers.

| Decision input | Question to answer | Why it changes the decision |
|---|---|---|
| Ownership | Who can accept downtime, data loss, cost, and residual risk? | A technical team cannot invent business tolerance |
| Criticality and impact curve | When does an outage become unacceptable, and is the impact financial, operational, reputational, contractual, or regulatory? | Establishes the maximum acceptable outage rather than a convenient technical target |
| Data-loss tolerance | How old may the last recoverable, internally consistent data point be? | Establishes the RPO and may drive backup or replication frequency |
| Reproducibility | Can lost data be regenerated or reacquired, from which sources, and within what time? | Derived data may tolerate a different recovery mechanism, but source RPO and rebuild time still constrain recovery |
| Failure scope | Is the objective for component, AZ, Region, logical corruption, destructive change, or account compromise? | The same workload can require different controls and objectives for different incidents |
| Dependencies | Which identity, DNS, network, key, secret, source, quota, data, and control-plane dependencies must recover first? | The workload cannot outperform a required dependency without a tested workaround |
| Demand window | Do objectives tighten during deadlines, market events, or other business periods? | A single annual target can hide time-dependent impact |
| Obligations | Are there legal, regulatory, residency, integrity, retention, or contractual requirements? | These can set non-negotiable boundaries |
| Operating model | Who declares a disaster, runs recovery, validates data, redirects traffic, and fails back? | Staffing and authority affect achievable recovery time |
| Cost tolerance | What recurring and recovery-event cost is justified by the avoided impact? | More aggressive objectives normally require more replication, automation, capacity, and testing |

## Decision Workflow

```text
identify owner and unacceptable impact
  -> define failure scope
  -> approve RTO and RPO
  -> align required dependencies
  -> choose the least-complex candidate pattern that can meet the objectives
  -> design recovery and failback
  -> test end to end
  -> compare actual results with the approved objectives
  -> remediate, fund a stronger design, or formally revise the objectives
```

## Objective-to-Pattern Decision Table

These are qualitative SAP-C02 scenario cues, not Lakehouse targets or AWS
service guarantees. Actual capability depends on workload size, consistency,
dependencies, quotas, automation, operator actions, and testing.

| Business cue | First candidate to assess | Why it may fit | What can disqualify it |
|---|---|---|---|
| Hours of outage and data loss are acceptable; lowest standing cost matters | Backup and restore | Minimal recovery-site capacity and policy-based recovery points may satisfy the tolerance | Restore, infrastructure deployment, dependency recovery, or validation exceeds RTO; recovery-point age exceeds RPO |
| Data/core services must remain replicated, but application capacity can be created after declaration | Pilot light | Keeps the critical core available while limiting standing compute | Required provisioning, quota, configuration, or control-plane operations cannot finish inside RTO |
| A complete reduced-capacity environment must already operate and minutes-class recovery is justified | Warm standby | The full stack is running and primarily needs scale-up and traffic redirection | Standby cannot take traffic immediately, replication lag exceeds RPO, or scale-up/dependency recovery exceeds RTO |
| Multiple Regions must actively serve and near-zero interruption/data loss is justified | Multi-site active/active | Avoids constructing a recovery environment after failure | Write conflicts, consistency, shared dependencies, isolation, or cost make the design unable or unjustified |

Choose the least costly and least complex pattern that has a credible path to
the approved objectives. A lower advertised RTO/RPO is not automatically a
better architecture.

## Dependency Constraint Table

The end-to-end objective is constrained by every required recovery dependency.
A dependency must meet or beat the workload objective, or the design needs a
tested workaround that removes it from the critical recovery path.

| Dependency | Recovery question | Evidence needed before claiming the objective |
|---|---|---|
| Identity and emergency access | Can authorized operators enter the recovery account/Region if normal identity paths are impaired? | Tested access path, least-privilege recovery roles, and break-glass controls |
| KMS, secrets, and certificates | Are decrypt permissions, keys, secrets, and certificates usable in the recovery scope? | Recovery-scope configuration and successful decrypt/authentication test |
| DNS and traffic management | Can traffic be redirected safely, and what TTL, health, or operator delay applies? | Tested routing action, health criteria, and rollback path |
| Network and endpoints | Are routes, security controls, service endpoints, and hybrid dependencies available? | Deployed configuration plus connectivity validation |
| Service quotas and capacity | Can missing resources be created or the standby be scaled during a Regional event? | Quota/capacity review and exercised scale or deployment path |
| Source feeds | Will upstream data remain available, replayable, or reacquirable? | Source retention/replay agreement and tested retrieval path |
| Raw data | Is an internally consistent recovery point available for the incident scope? | Version/backup/replica inventory plus restore and integrity evidence |
| Derived data | Can it be restored or deterministically rebuilt within RTO from sources that meet RPO? | Measured rebuild, validation, and source-lineage evidence |
| Infrastructure and application artifacts | Can networks, roles, jobs, configuration, and code be reproduced without unavailable control-plane assumptions? | Versioned artifacts, deployment test, and drift checks |
| Observability and communications | Can operators detect recovery health, validate success, and coordinate decisions? | Recovery-environment telemetry, alarms, logs, contacts, and escalation test |

## Failure-Scope Decision Table

| Failure scope | Objective question | Controls to assess | Trap to avoid |
|---|---|---|---|
| Component or instance | Must service continue without recovery-site activation? | Managed-service resilience, redundancy, replacement, and automated restart | Escalating every local fault into a Regional DR event |
| Availability Zone | Must the workload remain available through loss of one AZ? | Multi-AZ architecture and service failover | Calling Multi-AZ a multi-Region recovery strategy |
| AWS Region | Must the workload operate elsewhere, and is its data available there? | Cross-Region data protection plus backup/restore, pilot light, warm standby, or active/active | Assuming an in-Region backup survives a Regional requirement |
| Logical deletion or corruption | How far back must recovery go, and how is a known-good point selected? | Versioning, point-in-time recovery, backup, immutable artifacts, and integrity validation | Assuming a current replica protects against a bad write that was replicated |
| Account compromise or destructive administration | Is recovery data isolated from the affected authority boundary? | Cross-account copies, restrictive vault/key policy, immutability, emergency access, and audit evidence | Treating geographic separation alone as security isolation |
| Source-feed loss | Can missed events be replayed or reacquired, and for how long? | Source retention, replay, durable landing, gap detection, and reconciliation | Giving the pipeline a tighter RPO than its source can support |
| Identity, KMS, DNS, or control-plane impairment | Can recovery proceed using available data-plane and emergency mechanisms? | Dependency-specific redundancy, pre-provisioning, cached/replicated configuration, and tested manual initiation | Designing an aggressive RTO around untested resource creation or unavailable credentials |

## Lakehouse Objective Worksheet

No accountable business owner has supplied numeric recovery objectives for
this portfolio workload. Every objective below therefore remains deliberately
unset. Current mechanisms are evidence inputs, not approved recovery claims.

| Workload capability or asset | Data role and recovery consideration | Business RTO | Business RPO | Current evidence | Required decision or test |
|---|---|---|---|---|---|
| External ingestion/source feed | Upstream availability, retention, replay, and reacquisition determine whether missed data can be recovered | **Not set - business owner required** | **Not set - business owner required** | Repository does not establish a contractual source replay window | Confirm source retention/replay and measure catch-up behavior |
| Raw S3 objects | Primary landed data; overwrite/delete recovery differs from Regional or account-level recovery | **Not set - business owner required** | **Not set - business owner required** | S3 Versioning and noncurrent-version lifecycle controls are evidenced | Classify irreplaceable versus reacquirable data; define incident scope; test object recovery |
| Curated Parquet data | Derived output may be rebuilt only if raw inputs, transformation code, configuration, and catalog dependencies are recoverable | **Not set - business owner required** | **Not set - business owner required** | Reproducible processing path exists, but no timed full rebuild is DR evidence | Measure rebuild and validation time from an approved source recovery point |
| Glue jobs, scripts, crawlers, and Data Catalog metadata | Processing and metadata must be restored or recreated consistently with data | **Not set - business owner required** | **Not set - business owner required** | Repository-managed definitions and code exist; no recovery drill is recorded | Inventory state outside code, test clean deployment/reconstruction, and validate schema/catalog integrity |
| Athena workgroup and query configuration | Query access depends on data, catalog, permissions, result location, and configuration | **Not set - business owner required** | **Not set - business owner required** | Current configuration is documented; no alternate-Region query test is proved | Recreate configuration and execute validation queries against recovered data |
| IAM and emergency operations | Recovery requires authorized access and policy deployment without broadening privileges | **Not set - business owner required** | Not applicable as a data-age objective; configuration currency still matters | Governance and policy artifacts exist; no end-to-end DR access test is proved | Define recovery roles, configuration source, approval authority, and access test |
| Encryption configuration | Current S3 evidence uses SSE-S3; future KMS adoption would add key-policy and recovery-scope dependencies | **Not set - business owner required** | Not applicable as a data-age objective; key/configuration availability still matters | Current encryption boundary is documented | Preserve current truth; reassess key replication and access only if the design adopts KMS |
| Infrastructure definitions and deployment artifacts | Recovery time includes network, storage, processing, permissions, configuration, and quota readiness | **Not set - business owner required** | Depends on artifact/configuration currency | Versioned infrastructure definitions exist; no complete recovery deployment is timed | Test deployment into an isolated scope and record drift, quota, and manual steps |
| Logs, alarms, and recovery evidence | Operators need detection, audit, integrity, and business-validation evidence during recovery | **Not set - business owner required** | **Not set - business owner required** where log loss is a business concern | Existing operational evidence exists for the primary path | Define retention/isolation needs and verify telemetry during a recovery drill |

### Reproducibility Rule

Calling data "derived" does not make its recovery free or immediate. The
rebuild path inherits the availability and recovery point of every required raw
source, plus the time to restore code, configuration, catalog state, capacity,
and permissions and to validate the result. If the external source cannot
replay the missing interval, the source-feed RPO becomes a hard constraint.

## Reusable Objective Record

Complete one row per workload and failure scope. Do not combine materially
different incident types into an apparently precise single target.

| Workload and owner | Failure scope | Impact/criticality | Approved RTO | Approved RPO | Limiting dependency or workaround | Candidate pattern | Designed capability | Tested result and date | Gap/decision |
|---|---|---|---|---|---|---|---|---|---|
| To be completed by accountable stakeholders | Component/AZ/Region/logical/account/source/dependency | Financial, operational, reputational, legal, or regulatory impact | Unset until approved | Unset until approved | Record the slowest required dependency or tested workaround | Select only after objectives | Estimate, not evidence | Record observed recovery time and recovery-point age | Remediate, accept, or revise |

## Recovery-Test Acceptance Record

A backup, replica, runbook, or infrastructure template is not proof that an
objective is met. For each exercise, record:

1. tested failure scope, data set, workload scale, Region/account, and starting
   conditions;
2. declaration time, authorized initiator, recovery start, service restoration,
   and business-validation timestamps;
3. timestamp and age of the selected recovery point;
4. infrastructure, identity, network, DNS, key, secret, quota, and source-feed
   dependencies exercised;
5. integrity, completeness, schema, reconciliation, and application/query
   validation results;
6. manual steps, control-plane operations, failures, retries, and operator
   assumptions;
7. measured recovery result versus approved RTO and RPO;
8. failback, reverse synchronization, rollback, and cleanup results; and
9. remediation owner, due date, retest trigger, and residual-risk acceptance.

Only the end-to-end business-valid result counts as recovery completion. A
successful restore job that leaves the workload unusable has not met RTO.

## SAP-C02 Decision Traps

1. **RTO is not RPO:** restoration delay versus tolerated recovery-point age.
2. **Objective is not capability:** business tolerance is not a service claim.
3. **Backup interval is not automatically RPO:** failed jobs, copy delay,
   consistency, retention, and restore eligibility still matter.
4. **Replication lag is not the whole RPO:** corruption and deletion may require
   an older known-good recovery point.
5. **Component recovery is not workload recovery:** all critical dependencies
   and business validation count toward RTO.
6. **Multi-AZ is not multi-Region DR:** choose controls for the stated failure
   scope.
7. **Derived data is not disposable by default:** source recovery and rebuild
   time constrain the result.
8. **Near-zero is not automatically best:** unjustified objectives create
   unnecessary standing cost and operational complexity.
9. **Documentation is not testing:** only an exercised path supplies observed
   recovery evidence.
10. **Failover is not the end:** failback, reconciliation, and return to steady
    state must be designed and tested.

## Lakehouse Disposition and Promotion Gate

Current disposition: **recovery objectives not set; live DR pattern not
selected**. The next artifact may map existing Lakehouse components to recovery
mechanisms and gaps, but it must retain this objective boundary.

A live backup, replication, standby, or multi-Region proposal may advance only
when:

- accountable stakeholders approve the relevant RTO/RPO and failure scope;
- dependency and data-reproducibility assumptions are documented;
- at least one candidate design has a credible cost and operational case;
- recovery and failback tests are defined; and
- the user explicitly authorizes any AWS change.

## Official AWS References

- [REL13-BP01: Define recovery objectives for downtime and data loss](https://docs.aws.amazon.com/wellarchitected/latest/framework/rel_planning_for_recovery_objective_defined_recovery.html)
- [REL13-BP02: Use defined recovery strategies to meet the recovery objectives](https://docs.aws.amazon.com/wellarchitected/latest/framework/rel_planning_for_recovery_disaster_recovery.html)
- [REL13-BP03: Test disaster recovery implementation](https://docs.aws.amazon.com/wellarchitected/latest/reliability-pillar/rel_planning_for_recovery_dr_tested.html)
- [REL 13: Plan for disaster recovery](https://docs.aws.amazon.com/wellarchitected/latest/framework/rel-13.html)

## Tracker Mapping and Next Item

This completes the tracker-required RTO/RPO decision table and supports
SAP-C02 Domain 2 recovery-strategy selection and Domain 3 recovery validation.
It remains a planning and revision artifact, not implementation evidence.

The Lakehouse recovery mapping is now complete in
`docs/planning/domain-2-lakehouse-recovery-mapping-20260719.md`, and the
answer-bearing source-backed review is complete in
`docs/exam-prep/resilience-dr-scenario-drill-review-20260719.md`. The focused
learner submission is recorded at 12/12 in
`docs/exam-prep/resilience-dr-scenario-drill-submission-20260720.md`, with
untimed and answer-bearing-source isolation caveats. The next Resilience/DR
gate is a fresh question-only spaced retest no earlier than 2026-07-27.
