# AI Orchestration P2 WP6 Corpus Manifest And Exclusion Contract

<!-- markdownlint-disable MD013 MD060 -->

**Decision date:** 2026-09-05<br>
**Status:** Complete for WP6; P2 remains in progress<br>
**Manifest version:** `ai-p2-corpus-manifest-v1`<br>
**Access scope:** Local read-only evaluation only<br>
**AWS changes:** None; no network, query, model, retrieval or publication action

## Objective

Create one immutable, locally active corpus manifest that pins the exact
answer-eligible evidence, records every selected-but-blocked or rejected class,
and implements the WP2 lifecycle plus WP3 completeness and fallback rules. The
manifest must remain truthful about the WP4 operand blocker and must not imply
that P1 evaluation coverage, a retrieval index or a deployed runtime exists.

## Delivered Artifacts

| Artifact | Purpose |
|---|---|
| `schemas/ai_corpus_manifest_v1.schema.json` | Closed Draft 2020-12 schema for immutable manifest identity, state, counts, active evidence, exclusions, tombstones, fallback and coverage |
| `schemas/ai_corpus_exclusions_v1.schema.json` | Closed exclusion-register schema with bounded target IDs, reason codes, retention modes and adversarial-fixture policy |
| `evaluation/ai-orchestration/p2/corpus-manifest-v1.json` | Initial complete, validated and locally active manifest instance |
| `evaluation/ai-orchestration/p2/exclusions-v1.json` | Immutable exclusion register pinned by the manifest |
| Three `ai_corpus_manifest_v1.*.invalid.json` mutation fixtures | Prove incomplete-active, revoked-active-entry and unpinned-exclusion states fail |
| `ai_corpus_exclusions_v1.answer_eligible.invalid.json` | Proves an excluded entry cannot become answer-eligible |
| `scripts/validate_contracts.py` | Always validates the two corpus records and materializes each compact negative mutation for reason-checked rejection |

## Decision Summary

WP6 distinguishes **manifest completeness** from **P1 evaluation coverage**.
The initial manifest is structurally `complete_validated` for its declared
active set and can be selected atomically for local read-only evaluation. Its
evaluation coverage remains `partial_blocked`.

The active set contains:

- `SF-08` version 1, the only WP1 structured fact currently materialized under
  the strict WP4 contract;
- `DOC-01` through `DOC-08`, each at version 1 with its exact bounded-document
  hash; and
- `DP-01` through `DP-08`, each at version 1 with exact passage and chunk
  identity.

`SF-01` through `SF-07` remain selected in the frozen WP1 pack but are excluded
from active answer evidence by `EX-001`. The pack retained their aggregate
values and source-response hashes, but not the 48 scalar operands required by
WP4. The manifest does not invent operands, relax the schema or silently admit
aggregate-only records.

Consequently, the current manifest can support the evidence boundary for
`ST-04` and `DO-01` through `DO-04`. `ST-01` through `ST-03` and the structured
half of `CO-01` through `CO-04` remain blocked. WP7 may instantiate the case
contracts and negative-policy fixtures, but it must preserve this blocked
state; it cannot label the seven cases answer-ready.

## Immutable Identity And Hashing

The exclusion register hash is the lowercase SHA-256 of UTF-8 JSON containing
the entire register except `register_hash`, serialized with sorted keys,
compact separators and non-ASCII characters preserved. Its rule ID is
`ai-corpus-exclusions-canonical-json-v1`.

The manifest hash uses the same serialization rule over the entire manifest
except `manifest_hash`. Its rule ID is
`ai-corpus-manifest-canonical-json-v1`. Because the manifest contains the
exclusion-register hash, any exclusion change also requires a new manifest
version and hash.

Version 1 values are:

| Record | SHA-256 |
|---|---|
| Exclusion register | `9ff63fcfee76c4291f387ad040af9d56bd6360eff17bb892c6747b7a9ea28bf5` |
| Corpus manifest | `c2053a7e2847b10c509de54468743615d2f38bb2d21bc4118da51be8a1a70338` |

The manifest pins the WP1 evidence-pack identity/version, every active evidence
version/hash, all approved passage IDs, the exclusion-register version/hash,
the selected query-template versions and all controlling policy/contract
versions. A stable ID without its exact version and hash is insufficient.

## Completeness And Atomic Activation

The schema supports three completeness states and two activation states:

| Completeness | Permitted activation | Meaning |
|---|---|---|
| `incomplete` | `inactive` only | Candidate is still being assembled or has missing items/checks |
| `complete_validated` | `inactive` or `active` | Every declared active item and reference exists and has passed the current contract checks |
| `quarantined` | `inactive` only | Candidate failed validation and cannot replace the prior complete manifest |

An active record must have non-null validation and activation times plus the
logical pointer ID `ai-p2-active-corpus`. The checked-in v1 file is the one
local logical pointer/version decision; it does not represent an AWS, database,
cache or deployed-service mutation. A future runtime pointer must switch to a
new complete manifest atomically and must never expose partial writes.

The v1 manifest has no prior complete manifest. It records `status = none`
rather than inventing a fallback. Future versions may point to one prior exact
ID/version/hash, but fallback must re-evaluate access, lifecycle and freshness,
cannot bypass revocation and cannot assemble one answer across manifests.

## Inventory And Query-Template Boundary

The manifest reconciles all 16 WP1 selected identities:

| Class | WP1 selected | Active | Contract-blocked | Rule |
|---|---:|---:|---:|---|
| Structured facts | 8 | 1 | 7 | Only `SF-08` currently satisfies WP4 |
| Documents | 8 | 8 | 0 | All eight bounded records satisfy the WP5 identity/hash boundary |
| Passages | 8 | 8 | 0 | Exactly `DP-01` through `DP-08` |

Both selected query-template identities remain recorded:

- `query-contract-8` version 1 remains selected as a bounded compatibility
  shape, but has no active evidence entries while `SF-01` through `SF-07` are
  blocked. No Athena query was executed.
- `bmrs-dataset-latest-metadata-v1` version 1 has the one active `SF-08`
  public-API precomputed fact. No Athena query was executed.

Query contract 12 remains reference-only and query contract 9 remains rejected
in the exclusion register. Neither can be inferred as selected from repository
existence.

## Exclusions And Adversarial Isolation

The register contains seven explicit decisions:

| ID | Target | Status | Reason code |
|---|---|---|---|
| `EX-001` | `SF-01` through `SF-07` | `contract_blocked` | `structured_operands_missing` |
| `EX-002` | Query contract 12 | `reference_only` | `query_shape_not_required` |
| `EX-003` | Query contract 9 and inspected ENTSO-E price candidates | `rejected` | `reuse_basis_not_established` |
| `EX-004` | Local structured sample and dashboard snapshot | `reference_only` | `non_authoritative_presentation_or_internal_locator` |
| `EX-005` | Copied local RSS descriptions | `rejected` | `reuse_basis_not_established` |
| `EX-006` | Sanitised regression artifacts | `adversarial_only` | `adversarial_only_not_answer_evidence` |
| `EX-007` | Broad/raw/failed/private/secret/operational/full-article scope | `rejected` | `deny_by_default_repository_boundary` |

Every exclusion has `answer_evidence_eligible = false`, a bounded authority and
classification, a retention mode, a human-reviewable reason, a reconsideration
gate and a null or explicit replacement pointer. Excluded source text is not
copied into the register.

WP7 owns adversarial fixture instantiation. The current register records that
such fixtures do not yet exist, must be marked `adversarial_only`, are
ineligible for ordinary retrieval and answers, and are inert data that cannot
alter policy or authorize tools.

## Lifecycle Tombstones

The manifest schema includes a closed tombstone shape containing only stable
ID, evidence kind, version/hash, prior authority class, lifecycle decision,
decision time, reason code, optional replacement pointer and
`content_retained = false`.

The v1 tombstone array is empty because no selected active version has actually
been superseded, revoked, source-deleted or quarantined. This is an explicit
zero, not an omitted state. `SF-01` through `SF-07` are contract-blocked before
initial manifest admission and therefore remain exclusions, not fabricated
revocation tombstones.

## Field Dictionary

### Manifest identity and state

| Field | Type and constraint | Meaning |
|---|---|---|
| `contract_version` | Constant `ai_corpus_manifest_v1` | Selects this manifest contract |
| `manifest_id` | Constant `ai-p2-corpus-manifest-v1` | Immutable logical v1 manifest identity |
| `manifest_version` | Constant integer `1` | Version paired with the ID and hash |
| `manifest_hash` | Lowercase 64-character SHA-256 | Canonical whole-manifest hash excluding this field |
| `canonicalization_rule_id` | Constant manifest rule ID | Names the exact hash serialization |
| `created_at` | Offset-bearing RFC 3339 instant | Local record-creation instant; not source freshness |
| `scope.purpose` | Constant local P2 purpose | Prevents production or open-ended reuse claims |
| `scope.access_scope` | Constant `read_only_evaluation` | Prohibits write-capable use |
| `scope.publication_status` | Constant `local_unpublished` | Does not authorize publication |
| `scope.aws_state` | Constant `no_change` | Records that WP6 made no AWS change |
| `status.candidate_started_at` | Offset-bearing instant | Beginning of the local candidate decision |
| `status.validation_completed_at` | Offset-bearing instant or null | Non-null only after the candidate passes validation |
| `status.completeness_status` | Closed three-state enum | Separates incomplete, accepted and quarantined candidates |
| `status.activation_status` | `inactive` or `active` | Logical local manifest selection |
| `status.activated_at` | Offset-bearing instant or null | Required for active; null for incomplete/quarantined |
| `status.atomic_pointer_id` | Constant `ai-p2-active-corpus` | Names the single logical selection point |

### Pack, policies and counts

| Field | Type and constraint | Meaning |
|---|---|---|
| `evidence_pack.id/version` | Exact WP1 pair | Pins the frozen selected collection |
| `evidence_pack.record_locator` | Exact repository record | Resolves the source pack without broad discovery |
| `policy_versions.selection_policy_id` | Constant WP1 policy ID | Pins selection/exclusion semantics |
| `policy_versions.access_policy_id` | Constant WP2 policy ID | Pins authority, classification and access |
| `policy_versions.freshness_conflict_policy_id` | Constant WP3 policy ID | Pins time, fallback and conflict rules |
| `policy_versions.structured_contract_version` | Constant WP4 contract | Pins structured evidence shape |
| `policy_versions.document_contract_version` | Constant WP5 contract | Pins document evidence shape |
| `policy_versions.exclusions_contract_version` | Constant WP6 exclusion contract | Pins negative-selection shape |
| `inventory_summary.*` | Exact non-negative v1 counts | Reconciles selected, active, blocked, excluded, tombstone and projection counts |

### Active evidence and passages

| Field | Type and constraint | Meaning |
|---|---|---|
| `active_structured_evidence[].evidence_id/version` | Exact `SF-08` v1 | Only structured item admitted now |
| `active_structured_evidence[].content_hash_kind` | `source_content_sha256` | Identifies the pinned WP1/WP4 hash meaning |
| `active_structured_evidence[].content_sha256` | Exact SF-08 source hash | Pins the source representation |
| `active_structured_evidence[].source_record_pointer` | Exact JSON Pointer | Resolves the WP1 record without search |
| `active_structured_evidence[].contract_version` | WP4 constant | Requires strict structured semantics |
| `active_structured_evidence[].authority/classification/scope` | Exact WP2 values | Fails closed against widening |
| `active_structured_evidence[].lifecycle_status` | Constant `active_selected` | Revoked/quarantined values cannot occupy the active array |
| `active_structured_evidence[].route_eligibility` | Exact `ST-04` | Prevents cross-case reuse |
| `active_structured_evidence[].query_template_id` | Exact metadata template | Pins the bounded source-fact shape |
| `active_document_evidence[].document_id/version/hash` | Bounded WP5 identities | Pins one immutable bounded document version |
| `active_document_evidence[].source_record_pointer` | Pointer `/document_passages/0..7` | Resolves the frozen source passage |
| `active_document_evidence[].publisher_id` | `ofgem` or `desnz` | Bounded source identity |
| `active_document_evidence[].authority/classification/scope` | Exact WP2 values | Restricts use to approved public read-only evidence |
| `active_document_evidence[].lifecycle_status` | Constant `active_selected` | Excludes lifecycle-ineligible documents |
| `active_document_evidence[].route_eligibility` | One matching `DO-*` or `CO-*` | Preserves WP1 route mapping |
| `active_document_evidence[].passage.*` | Exact ID/version/chunk/hash vocabulary | Pins the one bounded passage per document |
| `approved_passage_ids` | Ordered `DP-01` through `DP-08` | Complete approved-passage inventory |

### Exclusions, coverage, projections and fallback

| Field | Type and constraint | Meaning |
|---|---|---|
| `exclusion_register.id/version/hash` | Exact record identity | Pins the separately reviewable negative decisions |
| `exclusion_register.record_locator` | Exact local path | Resolves the immutable register |
| `selected_query_templates[]` | Exactly two selected template records | Pins versions, active IDs and no-Athena boundary |
| `tombstones[]` | Closed lifecycle audit objects | Records prior active versions without retaining content |
| `derived_projection_boundary.required_for_manifest` | Constant false | No retrieval/index projection is claimed or needed in WP6 |
| `derived_projection_boundary.expected_count/actual_count` | Both zero | Explicitly reconciles absence of projections |
| `evaluation_coverage.status` | Constant `partial_blocked` | Prevents completeness being mistaken for P1 readiness |
| `evaluation_coverage.ready_case_ids` | `ST-04`, `DO-01..04` | Cases whose current evidence boundary is present |
| `evaluation_coverage.blocked_required_case_ids` | `ST-01..03`, `CO-01..04` | Cases blocked by the seven structured records |
| `evaluation_coverage.blocked_evidence_ids` | `SF-01..07` | Exact cause of partial coverage |
| `evaluation_coverage.remaining_case_families_status` | Constant `WP7_not_instantiated` | Avoids claiming fixture completion |
| `prior_complete_manifest` | Tagged none/available union | Pins an exact safe-fallback predecessor or explicit absence |
| `fallback_policy.*` | Closed fail-safe constants | Requires rechecks, prohibits revocation bypass/version mixing and returns a bounded non-answer |

### Exclusion register

| Field | Type and constraint | Meaning |
|---|---|---|
| `exclusion_register_id/version/hash` | Exact v1 identity and canonical hash | Makes all negative decisions immutable and manifest-pinnable |
| `entries[].exclusion_id` | `EX-001` through `EX-007` | Stable exclusion decision identity |
| `entries[].target_type/target_ids` | Bounded enums | Names exactly what is outside active evidence |
| `entries[].selection_status` | Contract-blocked, reference, rejected or adversarial | Preserves distinct reasons for ineligibility |
| `entries[].authority_class/classification/lifecycle_status` | Bounded WP2 values | Prevents public reachability being mistaken for authority |
| `entries[].answer_evidence_eligible` | Constant false | Core deny-by-default invariant |
| `entries[].retention_mode` | Closed minimal-retention enum | Avoids preserving excluded text unnecessarily |
| `entries[].reason_code/reason` | Bounded machine/human explanation | Makes failure and review deterministic |
| `entries[].reconsideration_gate` | Bounded text | Defines the evidence needed for a new decision |
| `entries[].replacement_pointer` | Null or exact stable ID/version | Prevents silent replacement |
| `entries[].audit_reference` | Bounded planning-document enum | Links to the controlling decision without becoming a public citation |
| `adversarial_fixture_boundary.*` | Closed WP7/ineligibility constants | Keeps future negative fixtures isolated from ordinary evidence |

## Negative Examples

The negative examples are compact mutation descriptors rather than duplicated
full manifests. `validate_contracts.py` loads the pinned valid base record,
applies only a closed `replace` or `remove` JSON-Pointer operation in memory,
then proves the materialized payload fails for its declared reason.

| Fixture | Mutation | Required failure |
|---|---|---|
| `ai_corpus_manifest_v1.incomplete_active.invalid.json` | Marks the active manifest incomplete | Active requires `complete_validated` and an incomplete manifest must be inactive |
| `ai_corpus_manifest_v1.revoked_active_entry.invalid.json` | Replaces an active document lifecycle with `revoked` | Active entry requires `active_selected` |
| `ai_corpus_manifest_v1.missing_exclusion_hash.invalid.json` | Removes the pinned register hash | `register_hash` is required |
| `ai_corpus_exclusions_v1.answer_eligible.invalid.json` | Sets an excluded item answer-eligible | `false` is required |

This approach keeps each invalid case to one deliberate difference, avoids a
large copied manifest drifting from its valid base and still validates the
actual mutated payload against the production schema.

## Schema Checks And WP8 Semantic Checks

The schemas close all objects, bound arrays and enums, enforce active/incomplete
state coupling, reject lifecycle-ineligible active entries and require all
version/hash references. WP8 must still implement the semantic checks that JSON
Schema cannot express conveniently:

1. Recompute the exclusion-register and manifest canonical hashes.
2. Resolve every JSON Pointer and compare ID, version, source/document/passage
   hashes, publisher, route and policy to WP1-WP5.
3. Enforce uniqueness and exact one-to-one mappings for document, passage,
   chunk, exclusion and query-template IDs.
4. Reconcile every count with the actual arrays and the eight-plus-eight WP1
   inventory.
5. Prove `SF-01` through `SF-07` occur only in the blocked exclusion and never
   in active evidence.
6. Re-evaluate lifecycle, access and freshness at each case's pinned `as_of`;
   manifest activation alone does not make evidence current.
7. Confirm candidate, validation and activation timestamp ordering.
8. Confirm any future tombstone or prior-manifest pointer is minimal, exact and
   cannot authorize content or cross-manifest assembly.
9. Confirm adversarial fixture IDs, once WP7 creates them, remain separately
   classified and unreachable by ordinary answer-evidence selection.

## Alternatives And Trade-Offs

### Selected: active subset plus explicit blocked coverage

This produces a usable, immutable local manifest without weakening WP4. It
also exposes that seven nominally selected aggregates are not strict-contract
instances. Treating structural manifest completeness and evaluation coverage
as separate fields prevents an attractive but false readiness claim.

### Rejected: activate all eight structured aggregates

Doing so would require either inventing 336 scalar operands or relaxing the
WP4 contract. Both would destroy reproducibility and violate the frozen
evidence boundary.

### Rejected: mark the whole manifest incomplete

The nine valid active records and seven explicit exclusions are internally
complete and useful for the next contract stage. Keeping the entire manifest
inactive would conflate missing P1 coverage with an invalid manifest write.

### Selected: separate exclusion register pinned by hash

Negative decisions need independent review and stable reason codes. Pinning its
hash in the manifest prevents the active evidence boundary from changing when
an exclusion is edited.

### Rejected: put adversarial fixtures in active evidence with a flag

A downstream selector could ignore the flag. Separate classification,
ineligibility constants and a dedicated WP7 boundary make accidental ordinary
retrieval structurally harder.

### Selected: empty tombstone array for v1

No selected active version has actually been revoked, deleted or superseded.
The schema defines the future minimal audit shape, while an explicit empty
array avoids fabricating a lifecycle event.

## Completion Assessment

| Gate | Result | Evidence |
|---|---|---|
| Manifest identity/version/hash | Pass | Immutable v1 ID plus recomputable canonical SHA-256 |
| Complete versus active state | Pass | Schema rejects incomplete-active mutation |
| Selected inventory reconciliation | Pass | All 16 WP1 IDs are active or explicitly blocked; counts are explicit |
| Active evidence pinning | Pass | One strict structured item plus eight document/passage versions and hashes |
| Exclusion contract | Pass | Seven bounded reason-coded decisions; excluded evidence always ineligible |
| Lifecycle history | Pass | Closed tombstone shape and truthful explicit zero for v1 |
| Last-known-good rule | Pass | Explicit no-prior state plus fail-safe future pointer and fallback policy |
| Adversarial isolation | Pass | Separate, not-instantiated, WP7-owned and never answer-eligible |
| Known coverage blocker | Pass | `SF-01..07` and seven affected required cases remain visibly blocked |
| Scope stayed inside WP6 | Pass | No WP7 cases, WP8 final validator, retrieval, model, network, Athena, publication or AWS action |

WP6 is complete. P2 remains incomplete until WP7 instantiates exactly 28 cases
with the 7/7/14 split and WP8 performs final semantic reconciliation and a
bounded advance/revise/stop decision.

## Tracker Mapping And Next State

- **Tracker/interview milestone:** supplies the immutable corpus-governance
  artifact required before fixtures, retrieval or model selection.
- **Lakehouse case study:** demonstrates curated authority, exact replay input,
  exclusions, lineage, versioning and fail-safe degradation.
- **SAP-C02 Domain 1:** applies classification, least privilege,
  deny-by-default access, lifecycle revocation and audit boundaries.
- **SAP-C02 Domain 2:** selects a proportionate local contract rather than a
  managed vector or orchestration service without evidence.
- **SAP-C02 Domain 3:** applies immutable versions, atomic activation,
  validation, quarantine, last-known-good constraints and deterministic
  failure handling.

The state transition from WP5 evidence-record contracts to WP6 immutable
manifest/exclusion governance has occurred, and WP6 is complete. The next
tracker-ordered package is **P2 WP7 — Instantiate The P1 Evaluation Cases**:
exactly 28 cases across the seven four-case families, split 7 calibration / 7
development / 14 holdout, with separate holdout gold and explicit evidence,
freshness, conflict, safety and scoring assertions. WP7 must preserve the
current blocked structured-evidence state and keep synthetic adversarial
fixtures separate from ordinary evidence. Do not begin WP8 final validators,
P3 retrieval/model comparison, orchestration or AWS deployment first.
