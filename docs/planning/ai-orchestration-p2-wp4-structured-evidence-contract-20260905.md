# AI Orchestration P2 WP4 Structured Evidence Contract

<!-- markdownlint-disable MD013 MD060 -->

**Decision date:** 2026-09-05<br>
**Status:** Complete for WP4; P2 remains in progress<br>
**Contract version:** `ai_structured_evidence_v1`<br>
**Schema:** `schemas/ai_structured_evidence_v1.schema.json`<br>
**Access-policy prerequisite:**
`docs/planning/ai-orchestration-p2-wp2-authority-classification-access-rules-20260905.md`<br>
**Freshness-policy prerequisite:**
`docs/planning/ai-orchestration-p2-wp3-freshness-version-conflict-rules-20260905.md`<br>
**AWS changes:** None; this is a local contract and validation package

## Objective

Define the smallest strict, versioned representation for the eight structured
facts selected by WP1. The contract makes identity, semantics, source
provenance, time, policy, freshness and public citation independently
reviewable before document schemas, manifests, evaluation fixtures, retrieval
or model work begins.

The schema is deliberately narrower than the existing Lakehouse record and
Athena-query inventories. It admits only the three selected Elexon BMRS metric
identities, GB scope, query-contract shape 8 compatibility and the bounded
dataset-metadata fact. It contains no SQL, table, column, output-location or
free-form parameter surface.

## Delivered Artifacts

| Artifact | Purpose |
|---|---|
| `schemas/ai_structured_evidence_v1.schema.json` | Draft 2020-12 strict schema with closed objects and bounded enums/parameters |
| `schemas/examples/ai_structured_evidence_v1.example.json` | Valid `SF-08` contract example using the frozen WP1 source fact and assessment |
| `schemas/examples/invalid/ai_structured_evidence_v1.arbitrary_sql.invalid.json` | Proves a query object containing an arbitrary `sql` field is rejected |
| `schemas/examples/invalid/ai_structured_evidence_v1.absent_with_value.invalid.json` | Proves an `absent` observation cannot carry a value |
| `schemas/examples/invalid/ai_structured_evidence_v1.derived_without_operands.invalid.json` | Proves a derived fact without its operands is rejected |
| `scripts/validate_contracts.py` | Registers the valid contract and verifies each known-bad example fails for its named reason |

The valid example's selected value, source URL, response hash, source record
count, policy IDs and frozen freshness result come from the WP1-WP3 records.
Its `acquired_at` and `generated_at` values are fixed contract-example
processing instants preceding the recorded WP1 assessment completion; they do
not assert that a production ingestion or active manifest existed.

## Contract Decisions

### Closed admission boundary

Every object uses `additionalProperties: false`. The schema admits only:

- stable IDs shaped as `SF-nn`, positive evidence and pack versions;
- `authoritative_structured`, `public`, `read_only_evaluation` evidence under
  `ai-evidence-access-policy-v1`;
- the `structured` route for `ST-01` through `ST-04` or the `combined` route
  for `CO-01` through `CO-04`;
- the three WP1 metric identities, their exact version-1 definitions, units,
  precision and normalized dimensions;
- region `GB` and `Europe/London` civil-day semantics where applicable;
- Elexon BMRS as the provider and the two selected source dataset identities;
- query-contract 8 shape compatibility with exact `effective_date` and `GB`
  parameters, or the metadata fact contract with exact `INDO` and `GB`
  parameters; and
- the Elexon licence ID, canonical HTTPS locator and required attribution.

A new metric, region, source, unit, definition, query template, parameter,
classification or access scope requires an explicit contract revision. Merely
adding a field to an object is invalid.

### Semantic value states

`observation` is a tagged union rather than an optional scalar:

| State | Required representation | Meaning |
|---|---|---|
| Present non-zero | `status = present`, a permitted `value_type`, and a non-null value | A selected measurement exists |
| Present zero | `status = present`, `value_type = decimal`, and numeric `value = 0` | Zero is evidence and is never treated as missing |
| Null | `status = null`, an explicit `value_type`, `value = null`, and a reason | The requested field exists with a source-null value |
| Absent | `status = absent`, expected `value_type`, a reason, and no `value` member | The requested metric/field is not present in the selected record |
| Not applicable | `status = not_applicable`, a reason, and neither `value_type` nor `value` | The concept genuinely does not apply |

The schema rejects cross-state mixtures. In particular, an absent observation
cannot carry zero, null or any other value. This preserves the `ST-04`
requirement to return `insufficient_evidence` for a missing requested metric
even when the selected metadata record itself exists.

### Metric and query allowlist

| Metric identity | Source field and aggregation | Unit/precision | Required query reference | Route boundary |
|---|---|---|---|---|
| `daily_average_system_buy_price` | Arithmetic mean of non-null `systemBuyPrice` values | `GBP/MWh`, six decimal places | `query-contract-8` version 1; exact date and `GB`; shape compatibility only; Athena execution false | `combined`, matching `CO-*` only |
| `daily_average_net_imbalance_volume` | Arithmetic mean of non-null `netImbalanceVolume` values | `MWh`, six decimal places | `query-contract-8` version 1; exact date and `GB`; shape compatibility only; Athena execution false | `structured`, matching `ST-*` only |
| `indo_dataset_last_updated_at` | Exact `lastUpdated` value for dataset code `INDO` | RFC 3339 UTC timestamp, second precision | `bmrs-dataset-latest-metadata-v1`; exact `INDO` and `GB`; public-API precomputed fact; Athena execution false | `structured`, exactly `ST-04` |

The schema encodes only template identity and the closed parameter object. It
has no legal location for arbitrary SQL, table names, column names, catalog
discovery, output locations or user-selected metrics. Query-contract 12 remains
`reference_only`; query contracts 9 and the ENTSO-E price route remain rejected.

### Direct and derived facts

A direct fact must use `kind = direct` and `identity-v1`; operands and
calculation parameters are forbidden.

A derived fact must provide:

- the fixed `arithmetic-mean-non-null-round-6dp-v1` calculation rule;
- a declared operand count and excluded-null count;
- every included scalar operand with stable operand ID, settlement period,
  allowlisted source field, numeric value and unit; and
- the fixed null-handling, six-decimal and half-even rounding parameters.

The schema rejects a derived object with no `operands` array. WP8 must also
verify the semantic invariants that JSON Schema cannot express conveniently:
operand IDs and settlement periods are unique, declared count equals array
length, included plus excluded-null counts reconcile to the source record
count, every operand field/unit matches the metric, and recomputation produces
the declared result at the declared precision.

The WP1 pack retained the seven selected daily aggregates, derivation text and
source-response hashes, but not their 48 scalar operands. WP4 therefore does
not silently manufacture operand values or claim those seven records have been
materialized under the new schema. Their admission to a future WP6 manifest is
blocked until exact operands are supplied from an approved source
representation and pass the later semantic validator. The direct `SF-08`
metadata fact is sufficient for the required valid schema example now.

### Time and freshness representation

The contract keeps these meanings separate:

- `effective_time` is either an exact `Europe/London` civil day with explicit
  UTC window boundaries or one RFC 3339 instant;
- `source.source_created_time` is either a known offset-bearing instant or an
  explicit `not_exposed` state; equivalence to the effective instant requires
  a written source basis;
- `processing.acquired_at`, `generated_at` and `validated_at` are separate
  offset-bearing instants; and
- `freshness_assessment` pins rule version, request mode, `as_of`, controlling
  time, threshold, computed age and decision.

Historical evidence uses `valid_for_requested_window` with null maximum age
and age fields. Current/latest structured evidence uses the WP3 36-hour
threshold and records `current`, `stale`, `future` or `time_unknown`. The
schema prevents naive datetimes but WP8 must verify chronological ordering,
the `Europe/London` civil-day boundaries and the age calculation.

Freshness remains a question-relative assessment. A valid historical contract
does not become current because its processing timestamp is recent, and the
example's frozen current result does not authorize a future current answer.

### Provenance and citation separation

`internal_provenance` contains the WP1 selection-record path, exact source
response SHA-256 and record count. It is audit input and must never be emitted
as a public citation.

`public_citation` is a separate closed projection containing only a reader-safe
label, Elexon BMRS source and metric labels, optional display value, effective
time label, canonical HTTPS URL and required attribution. It cannot contain a
repository path, S3 URI, query-result location, account identity, credential,
trace or hash field.

The source and internal-provenance hash/count values are deliberately repeated
so WP8 can assert equality and detect a mismatched projection. JSON Schema
validity alone does not prove that equality or source authenticity.

## Field Dictionary

### Identity, policy and route

| Field | Type and allowed values | Required meaning |
|---|---|---|
| `contract_version` | Constant `ai_structured_evidence_v1` | Parser and compatibility boundary; a breaking semantic change requires a new version |
| `evidence_id` | `SF-nn` string | Stable logical evidence identity; insufficient without version and exact source hash |
| `evidence_version` | Positive integer | Immutable representation version; never incremented silently in place |
| `evidence_pack.id` | Bounded P2 pack ID | Frozen selected-collection identity, not an active manifest |
| `evidence_pack.version` | Positive integer | Exact pack version containing the selection decision |
| `policy.policy_id` | Constant `ai-evidence-access-policy-v1` | WP2 policy applied to the item |
| `policy.selection_status` | Constant `selected` | Confirms WP1 selection; a candidate or rejected item cannot use this contract |
| `policy.authority_class` | Constant `authoritative_structured` | Identifies the only answer-evidence authority represented here |
| `policy.classification` | Constant `public` | Exact admitted field set is public-safe; it does not itself authorize publication |
| `policy.access_scope` | Constant `read_only_evaluation` | Permits only the WP2 local evaluation purpose |
| `policy.lifecycle_status` | `selected`, `quarantined`, `revoked`, `source_deleted` or `superseded` | Current lifecycle decision; only `selected` is answer-eligible after all later checks |
| `route_eligibility.route` | `structured` or `combined` | Only the WP2 route authorized for the item |
| `route_eligibility.p1_case_ids` | Unique allowlisted `ST-*` or `CO-*` array | Exact case-family boundary; no cross-family reuse |

### Fact semantics

| Field | Type and allowed values | Required meaning |
|---|---|---|
| `metric.name` | One of three selected metric IDs | Machine-stable metric identity |
| `metric.definition` | Exact metric-specific constant | Human-reviewable meaning used in conflict identity |
| `metric.definition_version` | Constant `1` | Immutable definition version |
| `observation.status` | `present`, `null`, `absent` or `not_applicable` | Explicit semantic state; never inferred from truthiness or field omission alone |
| `observation.value_type` | `decimal` or `rfc3339_timestamp` when applicable | Expected/actual typed value category |
| `observation.value` | Number, offset-bearing timestamp, explicit null, or prohibited by state | Evidence value; numeric zero is a valid present value |
| `observation.reason` | Non-empty string for null/absent/not-applicable | Why the exceptional semantic state applies |
| `unit` | Metric-specific enum | Unit/basis used in fact comparison and citation |
| `precision` | Decimal-place or timestamp-resolution object | Exact comparison and rounding precision; no undocumented tolerance |
| `region` | Constant `GB` | Geographic applicability and conflict-key component |
| `dimensions.aggregation` | `arithmetic_mean` or `exact_value` | Normalized aggregation semantic |
| `dimensions.source_field` | `systemBuyPrice`, `netImbalanceVolume` or `lastUpdated` | Allowlisted source field; not a user-supplied column name |
| `dimensions.dataset_code` | Constant `INDO` when required | Exact metadata-record dimension; forbidden for daily market facts |

### Time, source and query provenance

| Field | Type and allowed values | Required meaning |
|---|---|---|
| `effective_time` | Closed civil-date or instant object | When the measured fact applies, separate from source/processing time |
| `source.provider_id` | Constant `elexon-bmrs` | Stable source authority ID |
| `source.provider_label` | Constant Elexon API label | Reviewable source name |
| `source.dataset_id` | One of two selected BMRS dataset IDs | Exact source dataset identity |
| `source.canonical_url` | HTTPS URI | Reader-resolvable source endpoint; not an internal locator |
| `source.source_content_sha256` | Lower-case 64-character SHA-256 | Exact source record/result-set representation used by the fact |
| `source.source_record_count` | Integer 1 through 1000 | Bounded source response/result-set count |
| `source.source_created_time` | Known instant with equivalence metadata or explicit `not_exposed` | Source-declared emission/change time; never replaced by ingestion time |
| `source.licence` | Fixed Elexon licence ID, HTTPS URL and attribution | Reuse basis and required attribution for selected structured facts |
| `query.template_id` | `query-contract-8` or metadata-contract constant | Allowlisted query/precomputed-fact shape identity |
| `query.template_version` | Constant `1` | Exact reviewed template version |
| `query.execution_mode` | Template-specific constant | Distinguishes shape compatibility from the direct public metadata fact |
| `query.athena_query_executed` | Constant false | Preserves the truth that WP1/WP4 ran no Athena query |
| `query.parameters` | Exact date-plus-GB or INDO-plus-GB object | Only permitted parameters; arbitrary keys are rejected |

### Derivation, processing and freshness

| Field | Type and allowed values | Required meaning |
|---|---|---|
| `derivation.kind` | `direct` or `derived` | Selects identity preservation or a calculation contract |
| `derivation.calculation_rule_id` | Fixed rule for the selected kind | Versioned deterministic calculation identity |
| `derivation.description` | Non-empty string | Human-readable calculation/replay explanation |
| `derivation.declared_operand_count` | Integer 1 through 50 for derived facts | Declared number of included operands; WP8 reconciles it |
| `derivation.excluded_null_count` | Integer 0 through 50 for derived facts | Source-null rows excluded under the fixed rule; missing is not zero |
| `derivation.operands[]` | Closed scalar-operand objects | Every included value, settlement period, source field and unit used by the result |
| `derivation.calculation_parameters` | Fixed ignore-null, six-place, half-even object | Prevents model-selected or caller-selected calculation behaviour |
| `processing.acquired_at` | Offset-bearing RFC 3339 instant | When the source representation entered the preparation path |
| `processing.generated_at` | Offset-bearing RFC 3339 instant | When this fact representation was deterministically created |
| `processing.validated_at` | Offset-bearing RFC 3339 instant | When contract, policy, identity and hash checks passed |
| `freshness_assessment.rule_version` | Constant WP3 rule ID | Exact time/conflict rule applied |
| `freshness_assessment.request_mode` | `exact_historical` or `current_latest` | Explicit temporal intent; ambiguity is rejected before evidence assembly |
| `freshness_assessment.status` | Mode-bounded WP3 status | Question-relative freshness decision, not a permanent object label |
| `freshness_assessment.as_of` | Offset-bearing RFC 3339 instant | Frozen evaluation instant |
| `freshness_assessment.controlling_time` | Offset-bearing instant or null only for `time_unknown` | Time from which the rule evaluates applicability/age |
| `freshness_assessment.maximum_age_hours` | `36` for current/latest, otherwise null | WP3 structured threshold; an engineering assumption, not an SLA |
| `freshness_assessment.age_seconds` | Number for calculable current/latest, otherwise null | Exact age at the pinned `as_of`; negative values require `future` treatment |
| `freshness_assessment.reason` | Non-empty string for `time_unknown` | Explicit reason time evaluation failed closed |

### Output separation

| Field | Type and allowed values | Required meaning |
|---|---|---|
| `internal_provenance.selection_record` | Fixed repository-relative WP1 evidence path | Internal audit pointer; prohibited from the public citation projection |
| `internal_provenance.source_response_sha256` | Lower-case 64-character SHA-256 | Audit copy of the exact source hash; WP8 verifies equality with `source` |
| `internal_provenance.source_record_count` | Integer 1 through 1000 | Audit copy of source count; WP8 verifies equality with `source` |
| `public_citation.citation_label` | Non-empty string | Reader-facing summary of the cited fact/source |
| `public_citation.source_name` | Constant `Elexon BMRS` | Public-safe source label |
| `public_citation.metric_label` | Non-empty string | Reader-facing metric name |
| `public_citation.value_label` | Optional non-empty string | Display value only when the answer may disclose one; omitted for absent/not-applicable states |
| `public_citation.effective_time_label` | Non-empty string | Reader-facing effective date/window |
| `public_citation.canonical_url` | HTTPS URI | Reader-resolvable source link |
| `public_citation.attribution` | Fixed Elexon attribution | Mandatory reuse attribution |

## Machine Validation Boundary

JSON Schema v1 validates object closure, required fields, types, formats,
allowlists, tagged value states, metric-specific shapes, template-specific
parameters and known-bad rejection. It does not claim to prove:

- source URL reachability, licence continuity or source authority;
- source/public/internal hash equality or the bytes behind a hash;
- timestamp ordering, age arithmetic or civil-day boundary correctness;
- operand/count/result recomputation and uniqueness beyond exact duplicate
  array objects;
- evidence-pack membership, lifecycle eligibility or future manifest
  completeness; or
- that a public citation supports a material answer claim.

Those are deterministic WP8 semantic-validator duties after WP5-WP7 define
the document, manifest and fixture contracts. Schema validity is necessary but
never sufficient for answer eligibility.

## Invalid Example Expectations

| Example | Required rejection reason |
|---|---|
| `ai_structured_evidence_v1.arbitrary_sql.invalid.json` | Closed query object reports the unexpected `sql` field |
| `ai_structured_evidence_v1.absent_with_value.invalid.json` | Absent-state branch reports the unexpected `value` field |
| `ai_structured_evidence_v1.derived_without_operands.invalid.json` | Derived branch reports missing required `operands` |

`scripts/validate_contracts.py --check-failures` verifies both that these files
fail and that the nested JSON Schema diagnostics contain the intended reason.
A known-bad file rejected only for some unrelated defect does not pass this
check.

## WP4 Completion Assessment

| Requirement | Result | Evidence |
|---|---|---|
| Strict versioned schema | Pass | Draft 2020-12 schema has constant contract version and closed objects throughout |
| Identity and metric semantics | Pass | Stable evidence/pack versions, exact metric definitions, typed values, units, precision, region, dimensions and time are required |
| Bounded source/query provenance | Pass | Two dataset IDs, two template IDs and exact parameter objects are allowlisted; SQL/table/column/output-location fields are absent and rejected |
| Version/time/policy integration | Pass | WP2 policy and WP3 freshness rule IDs, lifecycle, timestamps and frozen assessment fields are explicit |
| Null/absent/zero/not-applicable distinction | Pass | Tagged-union observation branches enforce separate representations |
| Derived-fact integrity boundary | Pass | Rule, counts, every included scalar operand and fixed calculation parameters are required; WP8 semantic checks are named |
| Internal/public separation | Pass | Closed internal provenance and public citation objects have disjoint permitted fields |
| Examples and negative proof | Pass | One valid example passes; three known-bad examples fail for their named reasons |
| Scope stayed inside WP4 | Pass | No document schema, manifest, evaluation fixture, retrieval, model, publication, network, Athena or AWS action occurred |

WP4 is complete. P2 remains incomplete until WP5-WP8 define and validate the
document contract, manifest, 28-case fixtures and full local corpus decision.

## Alternatives And Trade-Offs

### Selected: a narrow metric-specific tagged contract

This produces stronger deterministic validation and clearer interview evidence
than a generic `{metric, value, metadata}` envelope. It requires a new contract
version when scope expands, which is desirable under the current small-corpus
and deny-by-default boundary.

### Rejected: extend `energy_input_v1` directly

That schema is an operational input baseline with nullable wide records and an
internal `source_reference`; it lacks stable evidence identity, policy,
question-relative freshness, derivation operands and separated public citation.
Changing it would also risk the verified managed-AI baseline.

### Rejected: store arbitrary SQL plus parameters

This would make query authority depend on caller/model text and reopen table,
column, catalog, scan-cost and injection boundaries. Fixed template IDs and
closed parameters preserve least privilege and auditability.

### Rejected: use optional `value` alone for exceptional states

Omission cannot distinguish absent, not applicable and serialization error;
null cannot distinguish source-null from missing; truthiness can collapse zero.
The explicit tagged union is required.

### Deferred: materialize all seven daily aggregate contracts

Doing so now would require exact scalar operands that are not present in the
frozen WP1 pack. Inventing values or refetching without a separately reviewed
evidence action would weaken reproducibility. WP4 records the blocker without
expanding into WP6/WP7 or external acquisition.

## Tracker And Interview Mapping

- **Tracker:** advances only the active interview-linked AI orchestration P2
  sequence and produces the required schema/evidence artifact.
- **Lakehouse:** establishes a stable structured adapter boundary over the
  selected public BMRS facts without changing the verified operational input
  or managed-AI workflow.
- **SAP-C02 Domain 1:** applies least privilege, deny by default, data
  classification, explicit access scope and internal/public separation.
- **SAP-C02 Domain 2:** immutable versions, hashes, deterministic replay inputs
  and fail-closed lifecycle fields support resilient evidence handling.
- **SAP-C02 Domain 3:** strict validation, bounded query shapes, timestamp
  semantics and explicit data-quality states support reproducible operation.
- **Interview:** provides concrete System Architecture, GenAI Fluency, Invent
  and Simplify, Ownership and Customer Obsession evidence without claiming a
  production retrieval path or realized customer outcome.

## Next Tracker-Ordered Priority

The state transition from WP3 rule design to WP4 structured-contract design
has occurred, and WP4 remains complete. WP5 is complete in
`docs/planning/ai-orchestration-p2-wp5-document-evidence-contract-20260905.md`.
WP6 is complete in
`docs/planning/ai-orchestration-p2-wp6-corpus-manifest-exclusion-contract-20260905.md`.
Execute P2 WP7 next: instantiate the exact 28-case set and separate holdout
gold.

Do not start WP8 final validation, P3 retrieval comparisons, embedding/model
selection, a managed service or AWS deployment before the preceding tracker
gates pass.
