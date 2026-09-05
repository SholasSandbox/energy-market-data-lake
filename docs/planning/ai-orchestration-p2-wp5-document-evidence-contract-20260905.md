# AI Orchestration P2 WP5 Document Evidence Contract

**Date:** 2026-09-05<br>
**Status:** Complete for WP5; P2 remains in progress<br>
**Decision type:** Local contract and evaluation-evidence design only<br>
**Controlling plan:**
[`ai-orchestration-p2-corpus-evidence-contract-plan-20260901.md`](ai-orchestration-p2-corpus-evidence-contract-plan-20260901.md)

## Objective

Define the smallest closed, versioned document-evidence contract that can
represent the eight public passages frozen by P2 WP1 without storing full
external articles or exposing internal provenance as a public citation. The
contract implements the WP2 authority and access boundary and the WP3
freshness, version and conflict rules. It does not create a corpus manifest,
perform retrieval, call a model, publish evidence or change AWS.

## Delivered Artifacts

- [`schemas/ai_document_evidence_v1.schema.json`](../../schemas/ai_document_evidence_v1.schema.json)
  is the closed Draft 2020-12 document-evidence schema.
- [`schemas/examples/ai_document_evidence_v1.example.json`](../../schemas/examples/ai_document_evidence_v1.example.json)
  is one valid bounded-passage example based on WP1 passage `DP-08`.
- Three reason-checked invalid examples prove that the contract rejects an
  oversized passage, internal provenance inserted into a public citation and
  text inserted into metadata-only mode.
- `scripts/validate_contracts.py` validates both the successful example and
  the expected failure reasons.

The timestamps in the valid example's `processing` object are deterministic
contract-fixture values. They demonstrate field shape and ordering; they do
not claim that a production ingestion pipeline ran at those times.

## Contract Boundary

An `ai_document_evidence_v1` record is one selected document version and either
one bounded passage or a metadata-only representation. Every JSON object is
closed with `additionalProperties: false`. Unknown fields, arbitrary source
domains, free-form routing, unbounded text and lifecycle states that are not
answer-eligible fail schema validation.

The schema permits only the current WP1 boundary:

- authority class `approved_document`;
- classification `public`;
- access scope `read_only_evaluation`;
- publishers Ofgem and the Department for Energy Security and Net Zero;
- GB region plus the bounded WP1 topic and entity vocabulary;
- document cases `DO-01` through `DO-04` or combined cases `CO-01` through
  `CO-04`; and
- sources that are available, not revoked and the current selected version.

This schema defines an answer-eligible evidence record, not a general document
archive. WP6 must retain superseded, deleted or revoked identities as manifest
tombstones, but those entries must not validate as active document evidence.

## Stable Identity And Version Rules

WP5 assigns each of the eight independently selected WP1 passages a stable
document identity. Each current document has one selected passage, so the
mapping is deliberately one-to-one for version 1:

| Document | Passage | Publisher | Canonical bounded-content SHA-256 |
| --- | --- | --- | --- |
| `DOC-01` | `DP-01` | Ofgem | `ff34b9be336f7d11fad53be352d587bf1b183789137cede06553c21bd151d68d` |
| `DOC-02` | `DP-02` | DESNZ | `64fb6682dfe535be2a57d77fe030902dbb3e51b1749348f3c7b04c0f6971a4a7` |
| `DOC-03` | `DP-03` | Ofgem | `ca8ea34b5a5911abb8d81226cb1f900be70ff3db85a079b8e19991f6f2829d63` |
| `DOC-04` | `DP-04` | Ofgem | `c6e88f3d2cfca088776f60db7e10bf82d5840629a4d8f475a2539d190015fe81` |
| `DOC-05` | `DP-05` | DESNZ | `88e5d33fc0b98cdabf2b760d6a674891dfa2d7d17a91f0d3d358f7cd68a9cadb` |
| `DOC-06` | `DP-06` | DESNZ | `ed54ca5f0f88ad2c62e6bcf552c8c22029782fa423370d304bccdd84003fcdd1` |
| `DOC-07` | `DP-07` | DESNZ | `669b798edd16fec6080a6914c2251d0709994c1a0ab470c1e1ca29accc805bea` |
| `DOC-08` | `DP-08` | Ofgem | `8a73aeccc2c764b7a116481e6be3326eaafcfc664ea92e1c3735d1a727c21599` |

The canonical document-content hash is the lowercase SHA-256 of UTF-8 JSON
serialized with sorted keys and compact separators for this exact object:

```json
{
  "canonical_url": "<canonical public URL>",
  "passage": {
    "passage_id": "<DP-nn>",
    "source_section": "<source section>",
    "text": "<exact bounded passage>"
  },
  "published_date": "<YYYY-MM-DD>",
  "publisher": "<publisher label>",
  "title": "<source title>"
}
```

This is a bounded evaluation-content fingerprint, not a claim that the entire
external page was copied and hashed. A change to any canonicalized field
creates a new `document_version` and document-content hash. A changed passage
also increments `passage_version` and changes its exact passage hash. WP6 must
pin both identities and hashes in the immutable manifest.

## Content Modes

### Bounded passage

`bounded_passage` carries the exact selected text, capped at 500 characters,
with a stable passage ID, passage version, chunk ID, ordinal, exact SHA-256 and
either section coordinates or a character range. The valid example uses the
minimum Ofgem sentence required to support the price-cap component claim. It
does not copy the full article.

Every bounded passage requires `claim_identity`. This is the deterministic
candidate key used by WP7 conflict fixtures. It records proposition identity,
normalized meaning, claim type, causal scope, precision, geography and the
time to which the claim applies. Model similarity alone is never sufficient to
declare two passages materially conflicting.

### Metadata only

`metadata_only` explicitly states why text is unavailable and lists the
allowlisted metadata fields that remain usable. It contains no passage text,
passage hash, chunk coordinate or claim identity. Metadata-only records may
support source discovery or citation administration, but they cannot support a
substantive answer unless a later contract and manifest explicitly permit it.

None of the eight frozen WP1 passages needs metadata-only mode. Its presence in
the schema records the required fail-closed policy without enlarging the WP1
corpus.

## Source Time, Freshness And Lifecycle

- `publication_time` is source time. Date-only values use `Europe/London` and
  retain `date` precision.
- `update_time` is either a known source time or an explicit `not_declared`
  reason. Ingestion and validation timestamps cannot stand in for it.
- `processing.ingested_at` and `processing.validated_at` are internal processing
  events. Reprocessing does not make old evidence fresh.
- `exact_historical` requests use `valid_for_requested_window` and no relative
  maximum age. `current_latest` requests use WP3's 168-hour maximum.
- When the controlling source time is unavailable, `time_unknown` records the
  reason and fails closed for any decision requiring freshness.
- Only `source_available`, `not_revoked` and `current_selected_version` records
  are answer-eligible. Deletion, revocation, rights uncertainty or a superseded
  version removes active eligibility; WP6 retains only the required tombstone
  and fallback history.

## Provenance And Citation Separation

`internal_provenance` links to the frozen WP1 selection record and carries the
selected passage ID, document hash, passage hash and source section required
for local reconciliation. `public_citation` contains only the publisher, title,
publication date, stable public locator, canonical URL and required OGL
attribution.

The public-citation object is independently closed. Internal file paths,
selection records, hashes, local processing timestamps and implementation
identifiers cannot be inserted into it. A caller must construct a public
citation from this object rather than serialize the internal record.

## Field Dictionary

### Record identity and policy

| Field | Type and constraint | Meaning |
| --- | --- | --- |
| `contract_version` | Constant `ai_document_evidence_v1` | Selects this exact contract version |
| `document_id` | `DOC-nn` | Stable logical identity assigned above |
| `document_version` | Integer, minimum 1 | Monotonic version of the selected bounded document record |
| `document_content_sha256` | 64 lowercase hex characters | Hash of the bounded canonical JSON, not the full remote page |
| `canonicalization_rule_id` | Constant `ai-document-bounded-canonical-json-v1` | Names the deterministic document hash rule |
| `evidence_pack.id` | Fixed-format pack identifier | Identifies the frozen WP1 selection pack |
| `evidence_pack.version` | Integer, minimum 1 | Version of that pack |
| `policy.policy_id` | Constant `ai-evidence-access-policy-v1` | Pins the WP2 policy version |
| `policy.selection_status` | Constant `selected` | Excludes rejected and reference-only sources |
| `policy.authority_class` | Constant `approved_document` | Identifies answer-authoritative documents |
| `policy.classification` | Constant `public` | Confirms public classification |
| `policy.access_scope` | Constant `read_only_evaluation` | Prohibits write-capable use |
| `route_eligibility.route` | `document` or `combined` | Selects one P1 route family |
| `route_eligibility.p1_case_ids` | Unique bounded case IDs | Lists exact eligible cases in that family |

### Lifecycle and source

| Field | Type and constraint | Meaning |
| --- | --- | --- |
| `lifecycle.document_status` | `selected_bounded_passage` or `selected_metadata_only` | Couples lifecycle state to its content branch |
| `lifecycle.deletion_status` | Constant `source_available` | Active evidence must not be source-deleted |
| `lifecycle.revocation_status` | Constant `not_revoked` | Active evidence must not be revoked |
| `lifecycle.version_status` | Constant `current_selected_version` | Prevents a superseded version from being active evidence |
| `source.publisher_id` | `ofgem` or `desnz` | Stable internal publisher key |
| `source.publisher_label` | Exact public publisher name | Public publisher label consistent with its key |
| `source.title` | 1-250 characters | Exact selected source title |
| `source.canonical_url` | HTTPS Ofgem or GOV.UK URL | Canonical public source locator |
| `source.publication_time` | Date plus London zone, or offset timestamp | Source-declared publication time and precision |
| `source.update_time` | Known source time or `not_declared` reason | Separates update time from processing time |
| `source.licence` | Fixed OGL v3 object | Records reuse terms and attribution |

### Metadata and content

| Field | Type and constraint | Meaning |
| --- | --- | --- |
| `metadata.regions` | Exactly `GB` | Geographic evidence boundary |
| `metadata.topics` | 1-5 unique allowlisted terms | Bounded retrieval and fixture topics |
| `metadata.entities` | 1-10 unique allowlisted labels | Bounded claim entities |
| `content.mode` | `bounded_passage` or `metadata_only` | Selects mutually exclusive content shapes |
| `content.passage_id` | `DP-nn` | Stable WP1 passage identity; bounded mode only |
| `content.passage_version` | Integer, minimum 1 | Version of exact passage content |
| `content.chunk_id` | `DP-nn-Cnnn` | Stable chunk within the passage version |
| `content.ordinal` | Integer 1-40 | Deterministic order within the selected document |
| `content.coordinates` | Closed section or character-range object | Re-locates the exact bounded passage in the public source |
| `content.text` | 1-500 characters | Minimum exact evaluation passage; never the full article |
| `content.passage_sha256` | 64 lowercase hex characters | SHA-256 of the exact UTF-8 passage text |
| `content.reason` | 1-300 characters | Explains why metadata-only mode is required |
| `content.allowed_metadata_fields` | Unique allowlisted field names | States the only metadata usable in metadata-only mode |

### Claim identity

| Field | Type and constraint | Meaning |
| --- | --- | --- |
| `claim_identity.claim_id` | `CLAIM-DP-nn-nn` | Stable identity for one normalized passage claim |
| `claim_identity.proposition_key` | WP1-bounded enum | Deterministic proposition candidate for WP7 |
| `claim_identity.normalized_proposition` | 1-300 characters | Concise normalized claim, not new source text |
| `claim_identity.claim_type` | Descriptive, quantitative, policy or definition | Claim semantics used in conflict comparison |
| `claim_identity.causal_scope` | Causal, association, descriptive or component | Prevents causal and non-causal claims being conflated |
| `claim_identity.precision` | Exact, approximate or qualitative | Preserves meaningful precision differences |
| `claim_identity.geographic_scope` | Constant `GB` | Geographic conflict dimension |
| `claim_identity.applicable_time` | Closed publication-context or relative-period object | Time window to compare for material conflict |

### Processing, freshness and output

| Field | Type and constraint | Meaning |
| --- | --- | --- |
| `processing.ingested_at` | Offset date-time | Internal ingestion event; not source freshness |
| `processing.validated_at` | Offset date-time | Latest successful local validation event |
| `freshness_assessment.rule_version` | Constant WP3 rule version | Pins freshness semantics |
| `freshness_assessment.request_mode` | Historical or current-latest | Chooses the matching freshness branch |
| `freshness_assessment.status` | Mode-specific closed value | Records usable, current, stale, future or unknown state |
| `freshness_assessment.as_of` | Offset date-time | Deterministic assessment time |
| `freshness_assessment.controlling_source_time` | Source time or null | Source update time when known, otherwise publication time |
| `freshness_assessment.maximum_age_hours` | `168` or null | Current threshold; not applicable to exact historical use |
| `freshness_assessment.age_seconds` | Number or null | Derived age for current evidence only |
| `freshness_assessment.reason` | Required for `time_unknown` | Explains why freshness cannot be established |
| `internal_provenance.selection_record` | Fixed repository path | Points to the frozen WP1 selection evidence |
| `internal_provenance.selected_passage_id` | `DP-nn` | Reconciles the record to the selected passage |
| `internal_provenance.document_content_sha256` | SHA-256 | Duplicates the top-level hash for provenance checking |
| `internal_provenance.passage_sha256` | SHA-256 | Duplicates the content hash for provenance checking |
| `internal_provenance.source_section` | 1-250 characters | Retains the WP1 source section |
| `public_citation.citation_label` | 1-300 characters | Public-safe human-readable citation label |
| `public_citation.publisher` | Exact publisher label | Public source authority |
| `public_citation.title` | 1-250 characters | Exact public title |
| `public_citation.publication_date` | Date | Public citation date |
| `public_citation.section_locator` | 1-250 characters | Public passage locator |
| `public_citation.canonical_url` | HTTPS source URL | Public navigation target |
| `public_citation.attribution` | Fixed OGL statement | Required public-sector attribution |

## Schema Checks And WP8 Semantic Checks

JSON Schema closes shapes, bounds strings, validates formats, restricts enums,
couples publisher domains and content modes, and keeps internal provenance out
of public citations. It cannot prove equality between distant fields or
recompute hashes. WP8 must therefore implement deterministic semantic checks
for every record:

1. Recompute the exact passage SHA-256 and bounded canonical document SHA-256.
2. Confirm top-level, content and provenance hashes agree.
3. Confirm document, passage and chunk IDs use the frozen mapping and versions.
4. Confirm publisher, title, URL, dates and source section equal the selected
   WP1 record and the public citation fields.
5. Resolve the section or character coordinates to the exact passage text.
6. Confirm ingestion precedes validation and reproduce the WP3 freshness
   calculation from the controlling source time.
7. Confirm route eligibility, claim identity and topic/entity terms match the
   WP1 and WP2 decisions.
8. Confirm public citations contain no internal fields and support only the
   bounded claim represented by the passage.
9. Reject unbounded/full-article fields and lifecycle states not selected by
   the active complete WP6 manifest.

## Invalid Example Expectations

| Invalid example | Required rejection |
| --- | --- |
| `ai_document_evidence_v1.oversized_passage.invalid.json` | `content.text` exceeds 500 characters (`is too long`) |
| `ai_document_evidence_v1.public_citation_internal_field.invalid.json` | Closed `public_citation` rejects `internal_provenance` |
| `ai_document_evidence_v1.metadata_only_with_text.invalid.json` | Closed metadata-only content rejects `text` |

Checking a required error fragment prevents an invalid fixture from passing for
an accidental, unrelated reason.

## Design Trade-Offs

### Accepted: one bounded passage per evidence record

This preserves exact claim-level hashes, coordinates and provenance while
keeping copyrighted/public-source content to the minimum evaluation text. It
also gives WP6 an unambiguous manifest unit.

### Rejected: extend `news_summary_v1`

That schema serves a different application surface and permits fields and
provenance shapes not authorized by WP2. Reusing it would weaken the closed
evaluation boundary and conflate public citations with feed processing.

### Rejected: store or hash a full downloaded article snapshot

WP5 needs reproducible evidence, not a private mirror of public pages. Full
snapshots increase licensing, drift and accidental-publication risk. The
bounded canonical hash is explicit about what this repository actually holds.

### Rejected: combine internal provenance and public citation

A single object would make it easy to publish repository paths, local hashes or
processing metadata. Separate closed objects make the output boundary
inspectable and testable.

### Accepted: schema branch for metadata-only evidence

This records the fail-closed access state required by the plan. It contains no
text or claim identity and does not make any current WP1 source answer-eligible.

### Deferred to WP6: tombstones and active-manifest state

Deletion, revocation and supersession must be auditable, but encoding them as
valid answer evidence would weaken the lifecycle gate. WP6 owns immutable
tombstones, exclusions and prior-complete-manifest fallback.

## Completion Assessment

| Gate | Result | Evidence |
| --- | --- | --- |
| Strict versioned schema | Pass | Closed Draft 2020-12 schema with document, passage and policy versions |
| Source and time metadata | Pass | Publisher, canonical URL, source and processing times remain distinct |
| Bounded public content | Pass | 500-character maximum, exact hash and stable coordinates; no full article |
| Authority and access | Pass | Only selected public `approved_document` evidence in read-only evaluation scope |
| Freshness and lifecycle | Pass | WP3 historical/current branches and answer-eligible lifecycle constants |
| Provenance separation | Pass | Closed internal and public objects; leakage fixture rejected |
| Valid and invalid examples | Pass | One valid example and three reason-checked invalid examples |
| Scope stayed inside WP5 | Pass | No manifest, fixtures, retrieval, model, publication, network, Athena or AWS action |

WP5 is complete. P2 remains incomplete until WP6-WP8 define and validate the
active corpus manifest, evaluation fixtures and final semantic validation.

## Tracker Mapping And Next State

This work supports the Energy Data Lakehouse case study, near-term cloud
architect positioning and SAP-C02 evidence-led architecture practice. It
creates the P2 document-contract evidence artifact required by the controlling
tracker, preserves least-privilege/read-only boundaries from Domain 1, and
records resilience against stale, revoked or conflicting evidence.

The transition from WP4 structured-evidence design to WP5 document-evidence
design has occurred, and WP5 remains complete. WP6 is complete in
`docs/planning/ai-orchestration-p2-wp6-corpus-manifest-exclusion-contract-20260905.md`.
The next tracker-ordered package is **P2 WP7 — Instantiate The P1 Evaluation
Cases**: exactly 28 cases, the 7/7/14 split, separate holdout gold and explicit
evidence, freshness, conflict, safety and scoring assertions. Do not begin WP8
semantic validators, retrieval, model comparison, orchestration or AWS
deployment until WP7 is reviewable and passes its gate.
