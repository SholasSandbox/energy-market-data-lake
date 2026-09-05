# AI Orchestration P2 WP3 Freshness, Version And Conflict Rules

<!-- markdownlint-disable MD013 MD060 -->

**Decision date:** 2026-09-05<br>
**Status:** Complete for WP3; P2 remains in progress<br>
**Rule ID:** `ai-evidence-freshness-conflict-policy-v1`<br>
**Access-policy prerequisite:**
`docs/planning/ai-orchestration-p2-wp2-authority-classification-access-rules-20260905.md`<br>
**Selected evidence:**
`docs/evidence/ai-orchestration-p2-wp1-selected-evidence-20260905.json`<br>
**AWS changes:** None; this is a local documentation contract

## Objective

Define deterministic time, freshness, version, manifest-fallback and conflict
rules for every WP2 evidence class and P1 case family before the structured and
document schemas are designed.

The thresholds below are repository engineering assumptions for a bounded
local evaluation. They are not external customer requirements, service-level
agreements or evidence that a production query path exists. WP3 creates no
schema, active manifest, fixture, retrieval path, model call, publication or
AWS resource.

## Decision Summary

Every evaluation case must pin an explicit `as_of` instant, evidence-pack
version and freshness/conflict rule version. Temporal intent is never inferred
from the wall clock after a case is frozen.

Three request modes are permitted:

1. **Exact historical:** the question names a date or closed time window.
   Evidence must match that window and active version; it has no relative-age
   expiry merely because the event is old.
2. **Current or latest:** the question explicitly asks for current, latest or
   recent evidence. Structured market evidence may be no more than 36 hours
   old; document context may be no more than 168 hours old.
3. **Temporally ambiguous:** the question needs time scope but supplies none.
   Return `invalid_request`; do not guess historical versus current intent.

Freshness is a property of evidence relative to a question and `as_of`, not a
permanent property of an object. Object existence, retrieval success, a recent
ingestion time or a new index timestamp cannot make old source evidence fresh.

## Time Semantics

| Time field | Meaning | Authority and use | Must not be used as |
|---|---|---|---|
| Event/effective time | When the measured fact or claim applies | Primary applicability time for structured evidence and exact historical matching | Ingestion, publication or validation time |
| Source publication time | When a document was first published by its authoritative source | Fallback controlling time for document freshness and exact publication-date matching | Proof that the page has not changed |
| Source update time | A source-declared substantive document update time | Controlling document time when present and tied to the selected version/hash | Local file-modification or retrieval time |
| Source-created time | When the source emitted or last changed a structured source row/response | Version and lag evidence; required provenance for current structured data when available | A replacement for the fact's effective window when it would make old events appear current |
| Acquired/ingested time | When the exact source representation entered the evaluation preparation path | Ordering, latency and replay evidence | Source freshness |
| Validated time | When identity, hash, authority, classification, reuse and contract checks passed | Proof that the selected version passed admission checks | Event/effective or source publication time |
| Indexed time | When a derived projection finished writing the exact selected version | Projection lag and manifest completeness | Source authority or freshness by itself |
| Manifest creation/activation time | When a complete, validated set became addressable/active | Atomic-version and last-known-good control | Proof that every source inside the manifest is current |
| Query `as_of` | Frozen evaluation instant against which all time rules are evaluated | Reproducibility and age calculation | The runtime wall clock substituted after the case is frozen |

The required ordering is source time no later than acquisition, acquisition no
later than validation, validation no later than indexing, indexing no later
than manifest activation, and manifest activation no later than the query
`as_of`. A missing required time, impossible ordering or source time later than
`as_of` makes the evidence temporally invalid; it cannot be labelled current.

WP4 and WP5 must represent these meanings separately. Reusing one timestamp for
multiple meanings is prohibited unless the source truly exposes one event and
the contract records that explicit equivalence.

## Timezone And Precision Rules

- Full instants use RFC 3339 with an explicit offset and are normalized to UTC
  for comparison and storage. A timestamp without an offset is invalid.
- GB settlement and source civil dates use the IANA timezone
  `Europe/London`; code must use timezone-aware conversion rather than a fixed
  GMT or BST offset.
- A date-only value retains `date` precision. Exact historical matching compares
  the civil date and must not silently convert it to midnight UTC.
- When a date-only document time is used for a current-age calculation, use the
  start of that `Europe/London` civil day converted to UTC. This conservative
  boundary prevents unknown publication time from understating age.
- A closed structured daily window ends at the next local midnight. Current-age
  calculation uses that effective-window end, not the later ingestion or index
  time.
- Leap seconds, daylight-saving transitions and month boundaries are handled by
  timezone-aware libraries. Manual fixed-offset arithmetic is not acceptable.
- Null, absent, zero, `not_applicable`, unknown precision and unparseable time
  remain distinct states. None may be converted to another for convenience.

## Freshness States

| State | Deterministic meaning | Answer eligibility |
|---|---|---|
| `current` | The item is WP2-eligible, its required times and version are valid, and its controlling source time is within the current/latest threshold at the pinned `as_of` | Eligible for its WP2 route |
| `valid_for_requested_window` | The item is WP2-eligible, its active version/hash matches and its event/publication/update time matches the explicit historical window | Eligible only for that historical window |
| `stale` | The item otherwise qualifies but its controlling source time exceeds the applicable current/latest threshold | Ineligible for an unqualified current answer; use the P1 stale outcome rules |
| `future` | The controlling source/effective time is later than `as_of` without an explicit forecast contract | Ineligible and invalid for this P2 corpus |
| `time_unknown` | A required time, precision or timezone is missing, ambiguous, unparseable or ordered impossibly | Ineligible; do not treat as current |
| `not_applicable` | The evidence class has no answer-route freshness, such as `reference_only`, or the field genuinely does not apply | Never a substitute for missing answer evidence |
| `manifest_incomplete` | The candidate projection set did not complete or validate atomically | Candidate manifest is inactive; evaluate the prior complete manifest under its own times and versions |

`revoked`, `source_deleted`, `quarantined`, `superseded` and `excluded` are WP2
lifecycle/access states rather than freshness states. They are removed before
freshness and conflict evaluation and cannot be rescued by a recent timestamp.

## Per-Class Freshness Rules

| Evidence class | Exact historical rule | Current/latest rule | Controlling source time | Missing-time behaviour |
|---|---|---|---|---|
| `authoritative_structured` daily fact | Effective civil date/window must exactly match the request; frozen source-response hash and derivation must remain selected; no relative expiry | Age from effective-window end must be at most 36 hours; source-created time and validation must also be present and consistently ordered | Effective-window end; a later source-created, ingestion or index time cannot refresh an old event | `time_unknown` and `insufficient_evidence`; never infer the date from ingestion |
| `authoritative_structured` source-metadata fact | Recorded metadata instant must be at or before the historical `as_of`; the exact version/hash must match | Age from the source-declared `lastUpdated`/effective instant must be at most 36 hours | Source-declared effective/last-updated instant | `time_unknown`; a missing market metric remains absent rather than zero |
| `approved_document` | Publication/update date must match the requested date/window and passage hash; no relative expiry for a historical claim | Age from source update time, falling back to source publication time, must be at most 168 hours | Source update time when source-declared and versioned; otherwise publication time | `time_unknown`; do not substitute local fetch, file or index time |
| `adversarial_fixture` | Fixture pins its own synthetic `as_of`, source times, expected rule and synthetic marker | Not answer-eligible; may intentionally cross a threshold to test a non-answer | The timestamp named by the fixture's tested rule | Invalid fixture until WP7 supplies every required time and expected state |
| `reference_only` | Not applicable | Not applicable | None | Remains route-ineligible rather than stale/current |

The current/latest thresholds are deliberately small and simple for the
portfolio evaluation: 36 hours for operational structured evidence and seven
days, expressed as 168 hours, for document context. Changing either threshold
requires a new rule version and a stated evidence/business rationale.

## P1 Family Decision Table

| P1 family | Time and version decision | Maximum acceptable age | Conflict treatment | Permitted outcome |
|---|---|---|---|---|
| `ST-01` exact value | Require the exact metric, unit, region, effective date/window and selected fact version | No relative expiry for exact historical; 36 hours for explicit current/latest | A material disagreement for the same logical fact blocks a settled value | `answered`, `stale_evidence`, `conflicting_evidence` or `insufficient_evidence` as applicable |
| `ST-02` derived comparison | Every operand must be selected, version-pinned and valid for the same requested comparison windows; record the calculation rule | Each operand independently applies the historical or 36-hour rule | Operand or calculation-rule conflict blocks the derived result | `answered`, otherwise the applicable non-answer; no partial numeric result |
| `ST-03` time/unit discrimination | Requested and evidence time windows, precision, unit and currency basis must match or use an approved deterministic conversion | Historical exact or 36 hours for current/latest | Non-equivalent units/windows that purport to answer the same claim are material | `answered`, `invalid_request`, `conflicting_evidence` or `insufficient_evidence` |
| `ST-04` present record with missing metric | The presence/version of the selected record or metadata fact must be valid at the case `as_of`; the requested metric must remain explicitly absent/null | Historical case pins the assessment instant; current metadata uses 36 hours | Conflicting presence/absence claims are material | `insufficient_evidence`; never coerce missing to zero |
| `DO-01` to `DO-04` | Require selected passage version/hash, publisher, publication/update time and exact historical window or current intent | No relative expiry for exact historical; 168 hours for current/latest | Mutually incompatible approved claims for the same entity/topic/time/scope are material | `answered`, qualified `partial_evidence`, `stale_evidence`, `conflicting_evidence` or `insufficient_evidence` |
| `CO-01` to `CO-04` | Evaluate structured and document components independently, then require the WP1 case pairing and requested temporal alignment | Exact-date pairs have no relative expiry; current pairs require both 36-hour structured and 168-hour document gates | Preserve structured/document tension; neither class silently overrides the other | `answered` only when both support the synthesis; otherwise qualified `partial_evidence`, `stale_evidence`, `conflicting_evidence` or `insufficient_evidence` |
| `SA-01` to `SA-04` | Pin the threshold, evidence versions, case `as_of` and manifest status being tested | Intentionally exceeds one applicable rule or uses an incomplete manifest | Conflict is separate unless the case explicitly combines stale and conflicting evidence | `stale_evidence`, qualified `partial_evidence` or permitted prior-complete-manifest fallback |
| `CF-01` to `CF-04` | Both references must be WP2-eligible, version-pinned and applicable to the same logical claim at `as_of` | Each reference must first pass its historical/current freshness rule unless staleness is the explicit test dimension | Preserve every material conflict and its provenance; no recency or model tie-break | `conflicting_evidence` or qualified `partial_evidence` for unaffected claims |
| `UN-01` to `UN-04` | Apply WP2 safety/authorization before temporal evaluation | Not applicable as an override | Freshness never converts unsafe or unauthorized evidence into permitted evidence | `unsafe_request` or `not_authorized` |
| `NA-01` to `NA-04` | Unsupported time, region, metric or ambiguous temporal scope yields no qualifying evidence | Not applicable when no matching evidence exists | Do not manufacture a conflict from non-comparable evidence | `insufficient_evidence` or `invalid_request` |

For a combined question, the weakest required component controls the combined
claim. A fresh fact with stale context may support the fact alone as
`partial_evidence` only when the answer clearly omits the stale explanation and
the remaining claim independently passes citation and grounding. It may not
present stale context as current.

## Version Identity And Change Rules

Evidence identity and evidence version are separate:

- the stable evidence or passage ID identifies the logical selected item;
- the evidence version and immutable content/source hash identify one exact
  representation;
- the evidence-pack version identifies one frozen selected collection;
- a future manifest version identifies one complete set of derived projections;
- the WP2 policy ID and this WP3 rule ID identify the access and time/conflict
  decisions applied to the answer; and
- every evaluation trace must record all applicable versions and the pinned
  `as_of`.

The deterministic lookup key is the stable ID plus exact version/hash. An ID
without its version/hash is insufficient for answer evidence.

| Change | Required version action | Prior-version state |
|---|---|---|
| Structured value, unit, metric definition, region/dimensions, effective window, derivation, operands, source identity or source-response content changes | Create a new evidence version/hash and repeat selection/validation | `superseded`; retained only for audit or a pinned historical case when policy still permits it |
| Document passage, meaning, title, source section, canonical locator, publication/update time or passage content changes | Create a new document/passage version/hash and repeat reuse/selection checks | `superseded` or `quarantined` until the new version passes |
| Classification, access scope, reuse terms, citation projection or lifecycle decision changes | Create a new policy/metadata version; never widen in place | Prior policy remains traceable but cannot authorize new answers after revocation |
| Only storage/index location changes while source identity, content and policy remain identical | Create a new projection/manifest version, not a new source-evidence version | Prior projection may be retired after the replacement is complete |
| Hash changes without an approved new version | Do not infer a legitimate update | `quarantined`; no answer or citation use |
| Correction is semantically identical after canonical serialization | Record the canonicalization rule and resulting hash in a new auditable version unless byte identity is preserved | No silent hash substitution |

A newer version does not automatically win. It becomes usable only after the
same authority, classification, access, reuse, time and validation checks pass.
Two accepted versions for the same logical fact and overlapping applicability
must not both remain active without an explicit supersession or conflict record.

## Manifest Completeness And Last-Known-Good Rules

WP6 will define the manifest schema. WP3 fixes these semantics in advance:

1. A candidate manifest starts inactive and incomplete.
2. It names the exact expected evidence IDs, versions/hashes, projections,
   access-policy version and freshness/conflict-rule version.
3. It becomes `complete_validated` only when every expected item and projection
   exists, hashes match, all validation passes, and no included item is revoked,
   source-deleted, quarantined, superseded or excluded.
4. Activation is one atomic pointer/version decision after validation. Partial
   writes or a recent manifest timestamp never become active.
5. A failed candidate is quarantined; the previously active
   `complete_validated` manifest remains the last-known-good candidate.
6. Last-known-good use is allowed only when every required item still passes
   WP2 access/lifecycle checks and this WP3 rule for the question's pinned
   `as_of`. Previous activation alone is not enough.
7. A revoked, deleted or newly excluded item invalidates that manifest for any
   route that requires it; last-known-good cannot bypass revocation.
8. If no complete and still-eligible manifest exists, return `stale_evidence`
   for the P1 stale-manifest case or the appropriate bounded non-answer for the
   requesting route. Never assemble across incomplete manifests.
9. Every answer and evaluation trace records the one manifest version used.
   Mixing versions inside one answer is prohibited unless a conflict fixture
   explicitly requires and labels the comparison.

The current WP1 JSON evidence pack is not an active corpus manifest and no
last-known-good manifest exists yet. These rules authorize only later WP6
design and WP8 validation, not manifest creation now.

## Conflict Identity

A conflict exists only after WP2 eligibility, temporal applicability, version
and manifest checks. Evidence that is excluded, revoked, stale for the current
question or about a different scope is not silently promoted into a conflict
set.

### Structured Conflict Key

Structured facts are comparable only when all parts of this logical identity
match:

- metric definition and definition version;
- region and normalized dimensions;
- effective start/end and time precision;
- unit, currency and currency basis, after an explicitly approved lossless
  conversion; and
- derivation rule when the fact is derived.

For the same logical identity, values conflict when they differ after applying
the recorded source precision, rounding and approved conversion. No undocumented
numeric tolerance is allowed. Different dates, windows, regions or genuinely
different metrics are non-comparable rather than conflicting.

An accepted correction from the same authority is a new version that explicitly
supersedes the old one. It is not a conflict once only the corrected version is
active. If two overlapping versions remain active or the correction status is
unclear, preserve both and mark a conflict.

### Document Conflict Key

Document passages require a stable claim identity comprising source/publisher,
entity/topic, asserted proposition, geographic scope, applicable time window
and claim precision. Two approved passages conflict only when they make
mutually incompatible material assertions for that same identity. Different
emphasis, added context, non-overlapping periods or compatible qualifications
are not conflicts.

WP5 must preserve enough versioned claim/source metadata for WP7 to construct
deterministic document-conflict fixtures. A model similarity score or generated
summary is not a conflict decision.

### Combined Conflict Key

Structured/document tension is material when an approved document explicitly
contradicts the structured metric, definition, unit, effective window or a
claim necessary to the requested synthesis. Mere temporal alignment or topical
association is not causal support and is not itself a conflict.

## Conflict Materiality And Outcomes

| Situation | Material? | Required behaviour | Outcome |
|---|---|---|---|
| Same structured identity, unequal normalized values beyond recorded precision | Yes | Preserve both references and calculation/precision evidence; do not choose a winner | `conflicting_evidence` |
| Same requested claim, non-equivalent unit or time-window definitions with no approved conversion | Yes | State the incompatibility; do not normalize or average silently | `conflicting_evidence` or `invalid_request` when the request itself is ambiguous |
| One version explicitly and validly supersedes another | No active conflict | Use only the accepted active version and retain the old version in audit history | Normal freshness/answer decision |
| Different regions, dates, entities or non-overlapping windows | No | Treat as non-comparable and apply route/query matching | `insufficient_evidence` if nothing matches |
| Approved documents make mutually exclusive assertions for the same claim identity | Yes | Preserve both citations and avoid unsupported adjudication | `conflicting_evidence` |
| Document provides context but no causal claim about a structured fact | No | Keep measured fact and reported context distinct | `answered` or qualified `partial_evidence` |
| Document explicitly contradicts a required structured claim | Yes | Preserve both classes and identify the tension | `conflicting_evidence` or qualified `partial_evidence` for unaffected claims |
| Conflict affects only an unrelated optional claim | No for the requested core claim | Omit the optional claim; retain conflict in trace | `answered` only if every emitted claim is independently supported |
| One candidate is stale, revoked, excluded or source-deleted | Not an active conflict | Remove it under WP2/WP3 before comparison; do not use recency as an implicit tie-break between otherwise eligible sources | Decide from remaining eligible evidence or return the applicable non-answer |

`partial_evidence` is permitted only when the answer explicitly identifies the
missing or disputed portion and every remaining claim is independently grounded.
It cannot present a disputed value, causal link or time window as settled.

## Decision Order And Outcome Precedence

Apply rules in this order:

1. Reject unsafe or unauthorized requests under WP2 as `unsafe_request` or
   `not_authorized` before considering freshness.
2. Validate request time mode and `as_of`; ambiguous or malformed temporal
   scope returns `invalid_request`.
3. Resolve exact WP2-selected IDs, versions/hashes, class, classification,
   access scope and route eligibility.
4. Validate required timestamps, timezone/precision and ordering.
5. Resolve the one complete manifest version when manifests exist; otherwise
   remain in contract/fixture design only.
6. Apply historical or current/latest freshness rules independently to every
   required evidence item.
7. Build a conflict set only from eligible and applicable items.
8. Return `answered` only if every emitted claim is supported; otherwise use
   the specific P1 non-answer or qualified-partial rule.

No newer timestamp, preferred publisher, source order, retrieval score or model
confidence silently resolves a material conflict.

## Application To The Frozen WP1 Pack

At the pack's `assessment_completed_at` value of
`2026-09-05T08:13:20Z`:

- `SF-08` records the Elexon INDO dataset `lastUpdated` value
  `2026-09-05T08:00:00Z`, an age of 13 minutes and 20 seconds. It passes the
  36-hour current-source rule at that frozen assessment instant.
- `SF-01` through `SF-07` are selected historical daily facts. They are valid
  only for their exact requested civil date/window and active hash; they are
  too old to answer an unqualified current/latest question at the assessment
  instant.
- `DP-01` through `DP-08` are selected historical passages. They are valid for
  their exact publication window and active passage hash; all are too old for
  an unqualified current/latest document request at the assessment instant.
- The `CO-01` through `CO-04` fact/passage pairs share the exact selected civil
  date and may support only their matching historical combined case. No current
  combined pair is selected.
- The pack-level assessment time is acceptable evidence of the WP1 selection
  review, but it does not replace the per-item acquisition, validation, update,
  index or manifest fields required by WP4-WP6.
- No conflicting pair, active manifest or last-known-good manifest is present.
  CF fixtures and manifest-state fixtures remain future WP7 work.

The stored `SF-08` value can remain valid for a future exact historical fixture
whose `as_of` is pinned to the assessment instant. It becomes stale for a later
current/latest question once its age exceeds 36 hours; freezing the pack does
not freeze real-world freshness.

## WP3 Completion Assessment

| Requirement | Result | Evidence |
|---|---|---|
| Event/effective, source, ingestion, validation, index/manifest and `as_of` meanings defined | Pass | Time-semantics and ordering tables keep source applicability separate from processing time |
| Controlling timestamp defined per evidence class | Pass | Per-class rules use structured effective/last-updated time and document update/publication time |
| Maximum age defined per P1 family | Pass | Exact historical, 36-hour structured and 168-hour document rules are explicit and combined cases evaluate both |
| Missing-time and timezone behaviour defined | Pass | RFC 3339 UTC, `Europe/London` civil-date precision and fail-closed states are explicit |
| Version identity and replacement behaviour defined | Pass | Stable ID, immutable hash, pack/policy/manifest versions and no in-place replacement are separated |
| Manifest completeness and last-known-good behaviour defined | Pass | Atomic activation, still-eligible fallback and no revoked/incomplete bypass are explicit |
| Conflict identity and materiality defined | Pass | Structured, document and combined keys distinguish disagreement from non-comparability or supersession |
| Terminal outcomes defined | Pass | P1 family and precedence tables preserve stale, conflict, partial and bounded non-answer outcomes |
| Scope stayed inside WP3 | Pass | No schema, manifest, fixture, retrieval, model, publication or AWS action was created |

WP3 is complete. WP4 subsequently defined and validated the strict structured-
evidence contract. P2 remains incomplete until the document schema, manifest,
28-case fixtures and final local validators pass in plan order.

## Alternatives And Trade-Offs

### Selected: question-relative freshness with fixed evaluation `as_of`

This preserves reproducible historical cases while preventing a frozen object
from answering a later current question merely because it still exists. It
requires explicit temporal intent and more metadata, which is necessary for the
P1 stale-evidence red line.

### Rejected: expire every item by age alone

This would incorrectly invalidate exact historical evidence even when its date,
version and hash precisely match the question.

### Rejected: use ingestion, index or manifest time as source freshness

This is operationally convenient but lets old evidence appear current after a
new copy or rebuild. Processing times measure lag and reproducibility only.

### Rejected: newest source always wins a conflict

Recency may represent a correction, a different time window or a different
claim. Only explicit accepted supersession removes a prior version; otherwise
material disagreements remain visible.

### Rejected: average or ask the model to resolve conflicting values

This produces unsupported certainty and violates the P1 conflict red line.
Deterministic normalization may establish equivalence, but material conflicts
require a conflict outcome or a narrowly qualified partial answer.

## Reconsideration Triggers

Create a new WP3 rule version before:

- changing the 36-hour or 168-hour threshold;
- adding intraday, forecast, event-stream, non-GB civil-time or customer-
  specified freshness requirements;
- defining an approved unit/currency conversion or numeric materiality
  tolerance;
- admitting a source with source-declared revision, correction or deletion
  semantics that differ from this contract;
- permitting mixed-manifest answers, multi-tenant caches or a managed retrieval
  service with different consistency guarantees; or
- allowing an automated tie-break, source-precedence hierarchy or external
  adjudication workflow.

## Tracker And Interview Mapping

- **Tracker:** completes only P2 WP3 in the active interview-linked AI
  workstream and preserves the evidence-led sequence.
- **Lakehouse:** distinguishes source time from processing time, keeps immutable
  version lineage and prevents derived projections from becoming authority.
- **SAP-C02 Domain 1:** preserves policy precedence, revocation and safe access
  when data becomes stale or disputed.
- **SAP-C02 Domain 2:** defines proportionate consistency and temporal contracts
  before selecting storage, retrieval or model services.
- **SAP-C02 Domain 3:** provides deterministic freshness, atomic activation,
  last-known-good fallback, quarantine and conflict handling.
- **Interview:** supplies a truthful System Architecture and GenAI Fluency
  explanation of staleness, reproducibility, safe degradation and disagreement
  without claiming runtime implementation or customer outcomes.

## Decision Consequence And Next Priority

WP3 remains complete. WP4 is complete in
`docs/planning/ai-orchestration-p2-wp4-structured-evidence-contract-20260905.md`.
WP5 is complete in
`docs/planning/ai-orchestration-p2-wp5-document-evidence-contract-20260905.md`.
WP6 is complete in
`docs/planning/ai-orchestration-p2-wp6-corpus-manifest-exclusion-contract-20260905.md`.
Execute P2 WP7 next: instantiate the exact 28-case set and separate holdout
gold. Do not begin final validators, retrieval, model selection, local
orchestration or AWS deployment before WP7 is reviewable and passes its gate.

State transition status: **WP6 is complete and the transition from manifest/
exclusion governance to evaluation-case design has occurred; WP7 is the next
bounded work package.**
