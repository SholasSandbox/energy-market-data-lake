# Domain 2 Lakehouse Recovery Mapping - 2026-07-19

<!-- markdownlint-disable MD013 MD060 -->

## Purpose and Boundary

**Document role:** repository-grounded recovery inventory, gap analysis, and
SAP-C02 case-study artifact.

This mapping connects the implemented Energy Data Lakehouse components to the
mechanisms that currently help recovery, the failure scopes those mechanisms
do and do not address, and the evidence required before stronger claims are
made. It covers S3, Glue, Athena, IAM, infrastructure definitions, and the
operational dependencies that constrain end-to-end recovery.

This document does not:

- set or imply a business RTO or RPO;
- select a live disaster-recovery pattern;
- prove a restore, failover, failback, or alternate-Region deployment;
- authorize AWS Backup, S3 replication, standby capacity, DNS, network, IAM,
  or other AWS changes; or
- treat Terraform definitions or previous successful runs as recovery-test
  evidence.

Use the companion
[RTO/RPO decision table](domain-2-rto-rpo-decision-table-20260718.md) to define
objectives and the
[Resilience and DR lesson](../exam-prep/aws-resilience-dr-sap-c02-key-lessons-20260718.md)
to compare candidate patterns. Every Lakehouse objective remains unset until
an accountable owner approves it.

## Evidence Reviewed

The mapping is based only on current repository and recorded Lakehouse
evidence, principally:

- `infra/terraform/lakehouse/` for reproducible workload definitions;
- `infra/terraform/lakehouse/README.md` for the S3 remote-state and rebuild
  boundary;
- `docs/adr/0001-shared-s3-data-bucket.md` for data-zone ownership;
- `docs/adr/0003-s3-versioning-and-tagging.md` for the accepted versioning and
  lifecycle decision;
- `docs/adr/0004-glue-athena-access-boundaries.md` for service and query-role
  boundaries;
- `docs/evidence/s3-versioning-tagging-apply-20260615.md` for live S3
  versioning and lifecycle evidence;
- `docs/evidence/phase9-terraform-import-20260511.md` for the imported remote
  Terraform state boundary;
- `docs/evidence/glue-athena-iam-live-verification-20260615.md` for the latest
  verified raw-to-curated-to-query chain; and
- repository scripts and validation evidence for local build, contract,
  schema, IAM-policy, and dashboard checks.

Evidence of a healthy primary path shows what must be recovered and validated.
It does not show that the path can be recovered within an objective.

## Recovery Posture Summary

| Layer | Current evidence-backed protection | Current recovery posture | Material gap |
|---|---|---|---|
| Source feeds | Ingestion code and source configuration are versioned in the repository | The pipeline can request current or configured backfill data when the upstream source permits it | No contractual retention, replay window, or tested catch-up duration is recorded |
| Raw S3 data | Live bucket versioning; noncurrent-version retention; raw lifecycle; private SSE-S3 bucket | A prior object version may support same-bucket logical recovery inside the retained window | No tested object restore, independent backup, cross-account copy, or cross-Region copy |
| Curated S3 data | Versioning plus reproducible ETL code and retained raw inputs while available | Curated Parquet may be restored from an object version or rebuilt from a valid raw recovery point | No timed full rebuild, integrity reconciliation, or proof that every required raw input remains available |
| Glue | Terraform definitions, repository ETL script, crawlers, job arguments, and a previously successful run | Jobs and crawlers are reconstruction candidates; catalog metadata can be re-created only after the data, role, and target paths are correct | No clean-environment reconstruction test, catalog snapshot/restore evidence, or schema-drift acceptance test |
| Athena | Terraform workgroup and bounded query role; named results prefix; successful representative query | Workgroup and access configuration are reconstruction candidates; query results can normally be regenerated from recovered data and catalog metadata | No alternate-scope reconstruction or query-validation test; saved business-query inventory is incomplete |
| IAM | Terraform-managed Lambda, Glue, orchestration, and Athena policies; governance evidence for operator access | Workload roles can be reconstructed from reviewed code when an authorized deployment path exists | No end-to-end recovery-role exercise; emergency access, trust, SCP, and service-role dependencies are not tested together |
| Infrastructure definitions | Versioned Terraform, application code, build scripts, and remote S3 state | The repository provides a reconstruction basis and the remote state preserves the managed-resource map | No isolated full deployment test; data bucket ownership is split because the current bucket remains externally managed |
| Operations and validation | Logs, alarms, failure artifacts, runbooks, schema checks, contract checks, and public-evidence checks exist | The repository can validate several individual stages after recovery | No single recovery runbook has exercised declaration, dependency recovery, business validation, failback, and cleanup |

The current posture is therefore **recoverability foundations recorded; tested
workload recovery not proved**.

## Component Recovery Mapping

### S3 Data and Artifacts

| Asset or prefix | Role | Current mechanism | Helps with | Does not currently prove | Required recovery evidence |
|---|---|---|---|---|---|
| `raw/` | Primary landed electricity and gas source data | Live S3 Versioning; noncurrent versions retained for 30 days; current raw objects transition and expire under the recorded 180-day lifecycle | Recent accidental overwrite or deletion when a usable prior version remains | Regional loss, account compromise, immutable recovery, recovery after retention expiry, or source completeness | Restore selected versions into an isolated prefix/bucket; verify keys, timestamps, hashes or record counts, schema, and source-window completeness |
| `curated/` | Derived Parquet used by Glue Catalog and Athena | Live S3 Versioning; ETL code can transform recovered raw data | Recent logical-object recovery and rebuild from retained raw inputs | That rebuild time meets RTO, that raw data meets RPO, or that the rebuilt partitions are complete and equivalent | Rebuild a bounded dataset from an approved raw recovery point; compare partitions, schema, row counts, and representative business aggregates |
| `scripts/` | Deployed Glue ETL script | Script source is versioned in the repository and managed as an S3 object by Terraform | Reconstruction of the deployed script from a reviewed source revision | That the correct revision is selected or that dependencies and job arguments are consistent | Deploy into an isolated scope, compare the object hash with the selected repository revision, then run the job |
| `athena-results/` | Query outputs | Workgroup enforces the location and SSE-S3; results derive from queries over curated data | Re-running queries after data and catalog recovery | Durable business-record recovery or preservation of every historical result | Classify whether any results are records; otherwise recreate representative queries and verify result content |
| `dashboard/`, workflow artifacts, and `failed/` | Inputs, validated snapshots, and failure evidence for the maintained managed workflow | Data-bucket versioning plus schema validation and last-known-good publication controls | Recent same-bucket object rollback and deterministic local regeneration for supported artifacts | Regional/account recovery or recovery of external news/source content that is no longer available | Recover a selected snapshot/artifact set, run contract validation, and prove the public snapshot remains valid before publication |
| Separate dashboard/static bucket | Private CloudFront origin and static site | Terraform-managed encryption, versioning, Block Public Access, bucket policy, and publish scripts | Recent object-version recovery and reconstruction from repository build output | Alternate-Region delivery, DNS recovery, or full CloudFront reconstruction timing | Rebuild locally, publish only in an approved isolated scope, verify private-origin access and HTTP/schema checks |
| Terraform state bucket | Durable resource-to-state mapping, separate from Lakehouse data | Documented remote S3 backend with encryption, versioning, public-access controls, and lockfile use; imported-state evidence exists | Recovery from recent state overwrite and continued management of imported resources | Cross-account/Region isolation, tested state-version rollback, or independence from the normal operator path | Identify the authoritative state version, test read-only state access and an isolated state-recovery procedure, then run a no-change/drift review |

The 30-day noncurrent-version window and 180-day raw-object expiry are current
technical retention settings, not business RPO or retention approval. A
replicated current object would also not replace the need for a known-good
historical point after corruption.

### Glue Processing and Catalog

| Component | Repository reconstruction source | Recovery dependency | Gap or risk | Validation required |
|---|---|---|---|---|
| Glue Data Catalog database | Terraform database definition | IAM, Region/account, and recovered data locations | Catalog contents are service state; the repository does not record a catalog backup/restore path | Recreate in an isolated scope and verify database/table ownership and expected names |
| Raw and curated crawlers | Terraform crawler definitions and target prefixes | Glue role, catalog database, S3 paths, representative recovered objects | Crawler inference can produce schema or partition differences from the prior catalog | Run crawlers against an isolated recovered dataset; compare schemas, partitions, classifiers, and table locations |
| Optional energy-specific crawlers | Terraform `for_each` definitions, disabled by default | Explicit enablement decision and correct source/dataset prefixes | Default-disabled resources must not be assumed present in recovery | Record the required crawler set before recovery and validate only the approved set |
| Raw-to-Parquet Glue job | Terraform job definition plus `glue/etl_raw_to_parquet.py` | Glue role, script object, raw inputs, curated destination, capacity, and job arguments | A prior successful job does not prove a clean rebuild or acceptable duration | Deploy the selected script revision, run a bounded rebuild, record duration/DPU use, and reconcile outputs |
| Catalog tables and partitions | Recovered objects plus crawler behavior | Internally consistent raw/curated recovery point and naming conventions | Re-crawling may expose partial or duplicate data if recovery points are inconsistent | Validate schema, partition coverage, row counts, and known electricity/gas queries before opening consumers |

### Athena Query Layer

| Component | Repository reconstruction source | Recovery dependency | Gap or risk | Validation required |
|---|---|---|---|---|
| Workgroup | `infra/terraform/lakehouse/athena.tf` | Region/account, result bucket/prefix, and Terraform deployment path | No clean-scope reconstruction is recorded | Confirm enforced configuration, SSE-S3 results, metrics, and output location |
| Dedicated query role | `infra/terraform/lakehouse/iam.tf` and ADR 0004 | IAM trust, approved assuming principal, Glue Catalog, curated data, and result prefix | Creating the role does not grant an operator or automation permission to assume it | Exercise approved role assumption; prove curated/results access and intended raw-prefix denial |
| Catalog-backed queries | Validation scripts and recorded representative SQL/evidence | Recovered curated objects, catalog schema/partitions, role, and workgroup | The repository does not inventory every business-critical query or expected result | Run the bounded schema validator and representative gas/electricity queries; compare expected schema and aggregates |
| Historical result objects | S3 object versions where retained | Result-object retention and classification | Results may be reproducible outputs rather than recovery records, but this is not formally classified | Decide which results, if any, require retention; regenerate the rest from validated data and queries |

### IAM, Governance, and Emergency Operations

| Dependency | Current evidence | Recovery limitation | Required decision or test |
|---|---|---|---|
| Lambda, Glue, Athena, and orchestration roles | Terraform policies, policy checks, and live Glue/Athena least-privilege evidence | Recreated roles still depend on correct trust, deployment authority, SCP allowance, and target resource names | Test the recovery operator path and representative service assumptions without broadening permissions |
| IAM Identity Center and emergency access | Domain 1 governance artifacts record bounded administrative and emergency paths | No Lakehouse recovery exercise proves access when the normal operator path or workload account is impaired | Name the recovery operator and approver; exercise sign-in, target-account access, and the minimum recovery actions under a separately approved test |
| Organizations and SCPs | The workload account and two narrow guardrails are recorded | A recovery action can fail if its required service/API is denied, while removing guardrails can expand blast radius | Pre-check required recovery actions against SCPs; document exception authority and rollback without assuming an exception is needed |
| Encryption | Current data and query-result evidence uses SSE-S3 | A future SSE-KMS design would add key, policy, grant, Region, and recovery-principal dependencies | Preserve SSE-S3 as current truth; add KMS recovery mapping only if the encryption ADR promotion trigger is met |
| Secrets and source credentials | Sensitive configuration is excluded from committed Terraform values | Repository reconstruction alone cannot recover a missing token or prove an authorized secret-distribution path | Inventory required secrets privately, define the recovery owner and store, and test retrieval/rotation without committing values |

## Infrastructure and Operational Dependencies

The critical recovery path is longer than the S3/Glue/Athena data path.

```text
authorized operator and approved recovery scope
  -> repository revision plus authoritative Terraform state/configuration
  -> Region/account access, quotas, networking, and source credentials
  -> S3 recovery point and bucket controls
  -> IAM service roles and policies
  -> Glue script, job, crawlers, catalog, and curated rebuild
  -> Athena workgroup, query role, schema, and representative queries
  -> optional orchestration/dashboard reconstruction
  -> integrity and business validation
  -> controlled traffic/publication, monitoring, and later failback
```

| Dependency | Why it constrains recovery | Current repository evidence | Open recovery question |
|---|---|---|---|
| Repository revision and build artifacts | A known-good version must be selected and packages/scripts rebuilt reproducibly | Versioned source, tests, build scripts, validation workflow | Which commit is the approved recovery baseline, and can every required package be rebuilt from it? |
| Terraform backend and variables | State, backend configuration, resource names, feature flags, and sensitive inputs control reconstruction | Remote-backend documentation, example configuration, imported-state evidence | Can authorized operators recover state/configuration without relying on one workstation or unavailable credentials? |
| External feeds | Missing raw intervals can be recovered only if providers retain/replay them or another copy exists | Ingestion code supports configured lookback behavior; live primary-path evidence exists | What replay window and rate limits apply, and how long does catch-up take? |
| Region, quotas, and service capacity | Clean reconstruction can fail or exceed RTO if capacity or quotas are unavailable | Region and sizing variables are documented | Which quotas are critical, and is alternate-scope capacity available during the tested scenario? |
| Network and DNS | Lambda source access and operator/service endpoints require working connectivity and name resolution | Primary-path operation and Networking study artifacts exist | What connectivity is required in each recovery scope, and has it been tested? |
| Logs, alarms, and notifications | Operators must identify failure, follow progress, and validate recovery | CloudWatch log groups, metrics, SNS failure path, and recorded evidence exist | Are telemetry and contacts available independently enough for the selected failure scope? |
| Runbooks and authority | Recovery time includes declaration, approval, execution, validation, and failback decisions | Deployment and service runbooks exist for individual components | Who declares recovery, accepts restored data, authorizes publication, and owns failback? |

## Failure-Scope Coverage

| Failure scope | Current strongest mechanism | Current conclusion |
|---|---|---|
| Accidental S3 overwrite/delete | S3 Versioning within the noncurrent-version retention window | Foundation exists; no object-recovery exercise is recorded |
| Bad ETL output or catalog change | Prior S3 versions plus repository ETL/crawler definitions | A rebuild path is plausible; consistent point selection and schema validation are untested |
| Lambda, Glue, Athena, IAM, or orchestration resource deletion | Terraform definitions, remote state, code, and import/deployment runbooks | Reconstruction basis exists; a clean isolated deployment and full dependency validation are untested |
| Availability Zone fault | Mostly managed regional services are used | Service design reduces direct instance-management concerns, but no workload-level AZ-failure test is recorded |
| Regional outage | No recorded cross-Region data or recovery environment | Not covered by current evidence |
| Workload-account compromise | Governance and external security-account foundations exist, but Lakehouse recovery data remains in the workload boundary | No isolated cross-account recovery copy or exercised clean-account recovery path is proved |
| Source-provider outage or historical gap | Configured ingestion/backfill behavior | Provider replay and catch-up constraints are unknown; no recovery claim can be made |
| Logical corruption replicated to current data | Version history may provide an earlier point within retention | Known-good point selection, isolation, and integrity testing are not proved |

## Candidate Test Sequence

This is a documentation-only acceptance outline. Running it against AWS needs a
separately approved scope, cost boundary, identities, target names, rollback,
and cleanup plan.

1. Select one failure scope and obtain an approved RTO/RPO for that scope.
2. Record the approved repository revision, data interval, expected tables,
   schemas, partitions, row counts, and representative query results.
3. Confirm recovery authority, emergency access, SCP impact, credentials,
   Region, quotas, and target isolation.
4. Select and restore a known-good raw recovery point without overwriting the
   surviving source evidence.
5. Reconstruct or verify bucket controls, IAM roles, Glue script/job/crawlers,
   catalog, Athena workgroup, and query role in dependency order.
6. Rebuild curated data and record elapsed time, failures, retries, and manual
   steps.
7. Validate object completeness, schema, partitions, record counts, business
   aggregates, IAM denies, query results, logs, alarms, and optional dashboard
   contracts.
8. Record the actual recovery-point age and total elapsed recovery time, then
   compare them with the approved objectives.
9. Exercise rollback/failback and data reconciliation before declaring the
   test complete.
10. Remove isolated test resources only under the approved cleanup boundary
    and retain public-safe evidence.

## Disposition and Promotion Triggers

No live pattern is selected. For the current low-volume portfolio workload,
the existing versioning and reconstruction foundations make backup-and-restore
the first cost-conscious pattern to assess once objectives exist. This is a
candidate for evaluation, not an architecture decision.

Assess stronger controls only when the named requirement demands them:

| Trigger | Control or design question to assess | Evidence needed before adoption |
|---|---|---|
| Same-bucket version history cannot meet the approved corruption/deletion objective | Independent backup, longer retention, or immutable recovery point | Data classification, retention need, cost, restore procedure, and tested integrity |
| Workload-account compromise is in scope | Cross-account copy and separately administered recovery permissions | Destination ownership, Organizations support, policy/KMS boundary, restore authority, and isolation test |
| Regional outage is in scope | Cross-Region data protection plus backup/restore, pilot light, warm standby, or active-active as objectives justify | Approved Regional RTO/RPO, data-copy behavior, service availability, quotas, cost, recovery/failback test |
| Source data cannot be replayed inside RPO | More durable or frequent protection for irreplaceable raw inputs | Provider constraints, data criticality, point consistency, copy monitoring, and gap-reconciliation test |
| Rebuild duration exceeds RTO | More automation, pre-provisioned dependencies, or a stronger DR pattern | Measured bottleneck, cost comparison, automation test, and revised runbook |
| SSE-KMS becomes required | Recovery-Region/account key design and emergency decrypt path | Accepted encryption decision, key policy/grant design, key availability, and decrypt test |
| Terraform state recovery is too dependent on the primary operator/account | More isolated state protection and tested recovery procedure | State sensitivity review, destination boundary, least-privilege access, version recovery, and drift-safe test |

## Tracker Mapping and Next Item

This artifact completes the tracker-required Lakehouse recovery mapping and
supports:

- SAP-C02 Domain 2 through recovery-aware solution design;
- SAP-C02 Domain 3 through dependency, validation, and improvement planning;
- the Energy Data Lakehouse case-study recovery boundary; and
- the Resilience/DR deliverable sequence without creating AWS resources.

The state transition from component mapping to the **source-backed
resilience/DR scenario review** has occurred in
`../exam-prep/resilience-dr-scenario-drill-review-20260719.md`. The learner's
focused 12/12 submission is recorded in
`../exam-prep/resilience-dr-scenario-drill-submission-20260720.md`, with its
untimed and answer-bearing-source isolation caveats. The next Resilience/DR
gate is a fresh question-only spaced retest no earlier than 2026-07-27.
