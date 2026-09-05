# AI Orchestration P2 Corpus And Evidence Contract Plan

<!-- markdownlint-disable MD013 MD060 -->

**Prepared:** 2026-09-01<br>
**Status:** Complete; WP8 decision is advance to P3 after explicit continuation<br>
**Prerequisite:**
`docs/planning/ai-orchestration-p1-evaluation-contract-20260901.md`<br>
**Decision register:**
`docs/planning/ai-orchestration-architecture-decision-register-20260830.md`<br>
**AWS changes:** None authorized or required

## Objective

Define and validate the smallest public-safe corpus and structured/document
evidence contracts needed to instantiate the 28-case P1 evaluation set. Stop
before retrieval benchmarking, model selection, orchestration code, managed
service selection, or AWS deployment.

P2 must make four boundaries explicit:

1. which structured records and document passages are approved;
2. which sources, paths and fields are excluded;
3. how identity, version, time, freshness, classification, access and citation
   are represented; and
4. how every P1 case resolves to expected evidence or an intentional
   non-answer.

## State And Authority

- P0 architecture and P1 evaluation-contract work are complete.
- The truthful stakeholder remains the repository owner as one internal
  decision-support user.
- The prospective outcome remains at least 30% lower median time to a trusted
  answer than the matched manual baseline, with no P1 red-line failure.
- Curated S3 contracts remain authoritative in the target architecture; local
  P2 fixtures are versioned evaluation evidence, not a new data authority.
- The scheduled managed-AI workflow remains the verified baseline and must not
  be changed or used as evidence that the proposed query path exists.
- This plan authorizes documentation and local contract/fixture validation
  only. It does not authorize AWS APIs, data creation in AWS, deployment,
  external publication, or model invocation.

## P2 Decision Questions

P2 is complete only when the resulting contract answers:

1. What is the smallest structured evidence subset needed for the `ST-*` and
   `CO-*` cases?
2. Which allowlisted query-contract shapes or precomputed facts are sufficient
   without exposing unrestricted SQL?
3. What is the smallest document subset needed for the `DO-*` and `CO-*`
   cases?
4. Which negative fixtures are synthetic evaluation controls rather than
   approved answer evidence?
5. Which repository, raw, failed, private, identity-bearing, secret-bearing,
   copyrighted, or operational files are excluded?
6. What stable IDs, versions/hashes, source fields, timestamps, classifications
   and access scopes are mandatory?
7. What freshness rule applies to each question family, and which timestamp
   controls the decision?
8. What citation form is safe for internal evaluation and for a future public
   answer?
9. How are conflicts, missing values, stale evidence, incomplete manifests and
   source deletion represented?
10. Can all 28 P1 cases resolve deterministically to accepted evidence or the
    intended no-evidence/policy outcome?

## Candidate Inventory, Not Approval

The first P2 action is a read-only inventory. The following are candidates
because they already express relevant contracts or public-safe evidence; this
table does not approve ingestion.

| Candidate | Possible role | Required review before selection |
|---|---|---|
| `schemas/energy_input_v1.schema.json` and its example | Structured field and type baseline | Confirm metric/unit semantics, null handling, internal locator treatment and stable evidence ID additions |
| `docs/evidence/energy_input_v1.sample.json` | Small structured fixture candidate | Confirm public-safety, age, source-reference handling and whether one record can support only calibration cases |
| `athena/query-contracts.json` | Allowlisted structured-query shape inventory | Select only the minimum required contract IDs; inspect SQL separately; prohibit arbitrary query text and parameters |
| `schemas/news_summary_v1.schema.json` and its example | Document metadata baseline | Confirm document identity, version/hash, passage coordinates, classification and access-scope additions |
| `docs/evidence/curated/news_summary_v1.sample.json` | Small approved-document candidate | Review each article for public-safe fields, stable source URL, licence/quotation boundary, freshness and duplicate content |
| `docs/evidence/phase17ar-scheduled-observation-news-summary-artifact-sanitized-20260610.json` | Sanitised negative or regression candidate | Inspect its actual shape before use; sanitised does not automatically mean suitable answer evidence |
| `schemas/dashboard_snapshot_v1.schema.json` and examples | Freshness/status field reference | Avoid treating a presentation snapshot as authoritative when structured source evidence is available |
| `dashboard-ui/public/dashboard-data.json` | Current public-surface comparison only | Do not make it the structured authority or ingest it by default; review tracked freshness and duplication first |

Do not inspect live S3, Athena, Bedrock, CloudFront or external sites for P2
unless a later user instruction explicitly expands the read-only evidence
boundary. Repository-local evidence is sufficient to create the first
contract.

## WP1 Admission-Gate Assessment - 2026-09-05

The repository owner approved the recommendations for the ten P2 decision
questions as **proposed WP1 decisions**, conditional on the actual eight
structured facts and eight document passages each passing four checks:

1. public safety;
2. question, topic and time alignment;
3. licensing or an affirmative reuse basis; and
4. independence from calibration, development and holdout leakage.

This assessment does not ratify those recommendations. The current local
candidate set does not pass the four-part gate, so no item is `selected` as
approved answer evidence and no WP1 selection/exclusion decision has been
recorded. Candidate paths below are exact audit locators, not public
citations. Their current Git blob IDs are:

| Candidate artifact | Git blob ID |
|---|---|
| `docs/evidence/energy_input_v1.sample.json` | `2d567b5ca359244b717c9fdcadcc8fa2b822e4e1` |
| `dashboard-ui/public/dashboard-data.json` | `e89abbcc78bfa396f830bfa1b3f5a1c5e25a43d7` |
| `docs/evidence/curated/news_summary_v1.sample.json` | `57141c2d66ab27b921f1bb6906b5dc9b88740333` |
| `athena/query-contracts.json` | `549453b2536dc9a0f0ab7302489813ff4dc0bb03` |

### Structured Fact Trial

The trial follows the proposed mix of four GB electricity facts, three
ENTSO-E regional price facts and one completeness/freshness fact.

| Trial ID | Exact local candidate | Finding | Admission result |
|---|---|---|---|
| `SF-C01` | `energy_input_v1.sample.json#/records/0/demand_mw`: `25118.0 MW`, GB, 2026-05-07 | The value and unit are explicit, but the record has an internal S3 locator, lacks a public source URL or recorded source-use basis, and shares its source row with `SF-C02`. | Fail |
| `SF-C02` | `energy_input_v1.sample.json#/records/0/system_buy_price_gbp_mwh`: `105.17 GBP/MWh`, GB, 2026-05-07 | The value and unit are explicit, but the same locator, licensing/provenance and independence gaps apply. | Fail |
| `SF-C03` | `dashboard-data.json#/overview/summaryCards/3/value`: `132.06 GBP/MWh`, GB, 2026-08-21 | This is a presentation value, not an authoritative structured result; its file metadata contains an identity-bearing bucket name. | Fail |
| `SF-C04` | `dashboard-data.json#/overview/summaryCards/4/value`: `31687 MW`, GB, 2026-08-21 | This is a presentation value from the same snapshot and cannot establish an independent authoritative fact row. | Fail |
| `SF-C05` | `dashboard-data.json#/overview/marketPanels/4/series/0/values/13`: `111.51215909090928 EUR/MWh`, FR | The series has no per-point date axis and conflicts with the separate ENTSO-E quality metadata's latest-date boundary, so effective time cannot be resolved deterministically. | Fail |
| `SF-C06` | `dashboard-data.json#/overview/marketPanels/5/series/0/values/13`: `120.00198863636363 EUR/MWh`, DE | The same presentation-authority, effective-time and common-snapshot independence gaps apply. | Fail |
| `SF-C07` | `dashboard-data.json#/overview/marketPanels/6/series/0/values/13`: `128.96954545454534 EUR/MWh`, NL | The same presentation-authority, effective-time and common-snapshot independence gaps apply. | Fail |
| `SF-C08` | `dashboard-data.json#/dataQuality/checks/0`: `28/48` GB settlement intervals, 2026-08-21 | The completeness observation is useful reference material, but it is not a result from query contract 12 and shares the same presentation snapshot. | Fail |

Only two dates are represented rather than the proposed four or more. Six
facts come from one presentation artifact, and the two remaining facts come
from one shared row. This cannot support the proposed independent P1 split.
The P1 example values for 2026-04-05 are already disclosed in the contract and
may be calibration controls only; they cannot repair holdout independence.

Query contracts 8 (`gb-electricity-daily-operating-view`), 9
(`entsoe-day-ahead-price-curve`) and 12
(`curated-table-freshness-coverage`) remain the proposed minimum shapes, but
the inventory labels business criticality `not-approved` and the repository
contains no retained results for those three contracts. They therefore remain
unratified templates, not selected evidence.

### Document Passage Trial

Eight distinct, plain-text RSS summary candidates were reviewed to avoid the
entries that contain truncated HTML and image markup.

| Trial ID | JSON article index and publisher | Published | Topic/region evidence | Admission result |
|---|---|---|---|---|
| `DP-C01` | 0, Energy Voice | 2026-05-07 | Oil/gas and renewables; EU and GB | Fail |
| `DP-C02` | 1, Energy Voice | 2026-05-07 | Power supply; no explicit region tag | Fail |
| `DP-C03` | 2, Energy Voice | 2026-05-07 | Industrial decarbonisation; GB | Fail |
| `DP-C04` | 8, Factor This | 2026-05-06 | Policy; no explicit region tag | Fail |
| `DP-C05` | 9, Power Technology | 2026-05-07 | Gas supply and renewables; no explicit region tag | Fail |
| `DP-C06` | 12, Power Technology | 2026-05-07 | Renewables; no explicit region tag | Fail |
| `DP-C07` | 14, Power Technology | 2026-05-07 | Renewables; no explicit region tag | Fail |
| `DP-C08` | 16, Power Technology | 2026-05-06 | Renewables; no explicit region tag | Fail |

These are distinct documents from three publishers and two dates, and their
selected fields contain no repository-private locator. They still fail
admission for two decisive reasons:

- `energy_market/news_ai.py` copies each publisher-provided RSS `summary` or
  `description` into the local artifact. Neither the repository nor the
  candidate metadata records a licence or other affirmative reuse basis for
  retaining those passages as evaluation answer evidence. RSS availability
  alone is not permission to republish or build a durable evaluation corpus.
- The candidates do not provide four document-only plus four structured-
  aligned combined passages. Most have no explicit region tag, and none
  supplies a defensible causal explanation for the trial structured values.
  No case allocation or isolated holdout-gold boundary exists yet.

Titles, publishers, timestamps and canonical URLs may be reconsidered as
metadata-only references. The copied RSS descriptions remain excluded from
approved answer evidence until an affirmative reuse basis is recorded or they
are replaced by independently authored, source-cited factual abstracts whose
creation and use comply with the source terms.

### Gate Result And Safe Next Move

| Check | Structured trial | Document trial | Result |
|---|---|---|---|
| Public safety | Fails because current source records expose an internal S3 locator or identity-bearing snapshot metadata | Passes for the eight minimized plain-text candidate fields | Overall fail |
| Alignment | Fails date diversity, authoritative-result provenance and deterministic ENTSO-E effective time | Fails the required four combined-case region/time/topic mappings | Overall fail |
| Licensing/reuse | No source-use basis is recorded with the fact artifacts | RSS descriptions have no recorded affirmative reuse basis | Overall fail |
| Holdout independence | Two shared source groups cannot support eight independent case facts; P1 examples are already disclosed | Documents are distinct, but case allocation and isolated gold labels do not exist | Overall fail |

The safe next move is to obtain eight bounded precomputed results from query
contracts 8, 9 and 12 with public-safe provenance and a recorded source-use
basis, plus eight independently usable document passages with explicit reuse
status. At least four structured/document pairs must align by region, topic
and effective/publication time. That acquisition is outside the current
repository-local boundary if it requires AWS or external-source access, so it
requires a separate explicit instruction. Until then, WP1 remains stopped and
WP2 must not advance to schemas, manifests, fixtures or retrieval.

## WP1 Selection Decision - 2026-09-05

The repository owner subsequently instructed continuation to the next tracker
priority, expanding the boundary to read-only AWS evidence discovery and
official external-source licensing checks. No AWS resource was created,
updated or deleted; no Athena query or model call was made.

The expanded search produced eight Elexon BMRS structured facts and eight
bounded passages from distinct official Ofgem or GOV.UK pages. All 16 passed
the public-safety, alignment, reuse-basis and selection-stage holdout-
independence checks. The accepted evidence pack is:

`docs/evidence/ai-orchestration-p2-wp1-selected-evidence-20260905.json`

The selection and exclusion decision, including the revised query-shape choice
and the ten WP1 answers, is:

`docs/planning/ai-orchestration-p2-wp1-selection-decision-20260905.md`

The successful set deliberately differs from the proposed trial mix:

- query-contract shape 8 plus a bounded BMRS dataset-metadata fact are
  selected for contract design;
- query-contract shape 12 remains `reference_only` until freshness-contract
  work establishes that it is required; and
- query-contract shape 9 and the inspected ENTSO-E day-ahead price candidates
  are rejected for the public evaluation corpus because an affirmative reuse
  basis for those price data was not established.

This decision supersedes the stopped WP1 state above but preserves that failed
local-only assessment as audit history. WP1 is complete. WP2 may now define
authority, classification and access rules; schemas, an active manifest,
evaluation gold, retrieval and model work remain unstarted.

## Corpus Size Boundary

The first P2 contract must stay small enough for manual audit and deterministic
gold labels:

- no more than 16 approved structured records or precomputed fact rows;
- no more than 12 approved documents;
- no more than 40 approved document passages/chunks;
- only the allowlisted structured query shapes required by the four structured
  and four combined P1 cases;
- a separate adversarial fixture set for stale, conflicting, unsafe and
  unanswerable cases; and
- no bulk repository ingestion, raw-zone ingestion or recursive document
  collection.

These are maximums, not targets. Select fewer items when they cover the P1
cases without weakening holdout independence.

## Work Packages

### WP1 - Read-Only Inventory And Selection Decision

1. Inspect candidate schemas, examples, samples and allowlisted query
   contracts locally.
2. Record each candidate as `selected`, `rejected`, `adversarial_only` or
   `reference_only`.
3. For each selected item, state which P1 case IDs require it.
4. For each rejected item, state the exclusion reason and reconsideration
   trigger.
5. Stop if the required P1 cases cannot be supported without broad or unsafe
   ingestion; revise P1 rather than widening silently.

**Output:** selection table and exclusion register in the P2 contract.

### WP2 - Authority, Classification And Access Rules

Define:

- `authoritative_structured`, `approved_document`, `adversarial_fixture` and
  `reference_only` evidence classes;
- `public`, `portfolio_safe_internal` and `excluded` classifications;
- read-only access scope and route eligibility;
- the separation between internal provenance and public-safe citation;
- deletion/revocation behaviour; and
- the rule that derived indexes and chunks are rebuildable projections.

No `raw/`, `failed/`, secret, identity-bearing, private local path, internal S3
URI, registration/payment data or unrestricted repository content may become
a public citation.

**Output:** authority/classification matrix and deny-by-default access rules.

**Status:** Complete in
`docs/planning/ai-orchestration-p2-wp2-authority-classification-access-rules-20260905.md`.
The four authority classes, three classifications, logical access scopes,
item-level route eligibility, citation boundary, revocation behaviour and
derived-projection rule are accepted for the WP1 evidence set.

### WP3 - Freshness, Version And Conflict Rules

Define per evidence class:

- event/effective time;
- source publication/update time;
- ingestion time;
- validation time;
- index/manifest time;
- the timestamp used for freshness decisions;
- maximum acceptable age for each P1 question family;
- missing-time and timezone behaviour;
- manifest completeness and last-known-good behaviour; and
- conflict identity, materiality and permitted terminal outcomes.

Freshness thresholds must be explicit engineering assumptions tied to the P1
case, not presented as external customer requirements.

**Output:** deterministic freshness and conflict decision table.

**Status:** Complete in
`docs/planning/ai-orchestration-p2-wp3-freshness-version-conflict-rules-20260905.md`.
Question-relative historical/current rules, 36-hour structured and 168-hour
document thresholds, timezone/precision handling, immutable version semantics,
atomic manifest fallback and material-conflict outcomes are accepted.

### WP4 - Structured Evidence Contract

Define a strict versioned schema containing at least:

- evidence ID and contract version;
- metric name and definition;
- value, type and unit;
- region, dimensions and time window;
- source and dataset identifiers;
- query-contract/template ID and bounded parameter values;
- source record or result-set hash;
- effective, generated and validated timestamps;
- classification and access scope;
- freshness status and rule version; and
- internal provenance plus public-safe citation label.

The contract must distinguish null, absent, zero and not-applicable. Derived
facts must record every operand and calculation rule. It must not contain
arbitrary SQL or permit an unbounded table/column/parameter choice.

**Proposed outputs:**

- `schemas/ai_structured_evidence_v1.schema.json`;
- one valid example and at least two invalid examples; and
- a structured-evidence field dictionary in the P2 contract.

**Status:** Complete in
`docs/planning/ai-orchestration-p2-wp4-structured-evidence-contract-20260905.md`.
The closed Draft 2020-12 schema admits only the three selected GB BMRS metric
identities and two bounded template shapes. It separates value states,
internal provenance and public citation; requires complete derivation operands;
and passes one valid plus three reason-checked invalid examples. The seven WP1
daily aggregates are not materialized under the schema because their scalar
operands were not retained in the frozen pack; future manifest admission
remains blocked until approved exact operands pass semantic validation.

### WP5 - Document Evidence Contract

Define a strict versioned schema containing at least:

- document ID, version and content hash;
- source/publisher and canonical public URL when permitted;
- title, publication/update/ingestion/validation timestamps;
- region, topic and entity metadata;
- classification and access scope;
- document status, freshness status and deletion/revocation status;
- passage/chunk ID, ordinal and stable character or section coordinates;
- exact passage hash and bounded evaluation text;
- internal provenance reference; and
- public-safe citation label and locator.

Do not copy full external articles into the repository. Use the minimum
existing public-safe summary or bounded passage required for evaluation, retain
the canonical source URL, and record when only metadata may be used.

**Proposed outputs:**

- `schemas/ai_document_evidence_v1.schema.json`;
- one valid example and at least two invalid examples; and
- a document-evidence field dictionary in the P2 contract.

**Status:** Complete in
`docs/planning/ai-orchestration-p2-wp5-document-evidence-contract-20260905.md`.
The closed Draft 2020-12 schema represents one selected document version and
either one exact bounded passage or a text-free metadata-only record. It pins
authority, access, route, source, version, freshness, lifecycle, claim identity,
internal provenance and public citation fields; limits passage text to 500
characters; and passes one valid plus three reason-checked invalid examples.
Full articles, revoked/deleted/superseded active evidence and internal citation
leakage are rejected. WP6 must now pin these records in an immutable active
manifest and represent lifecycle history as tombstones.

### WP6 - Corpus Manifest And Exclusion Contract

Create one immutable manifest version that records:

- manifest ID, schema version, created time and completeness state;
- every selected structured/document evidence ID and version/hash;
- every approved passage ID;
- source/classification/access/freshness rule versions;
- the selected query-contract IDs;
- explicit exclusions and exclusion reasons;
- superseded/revoked entries;
- deterministic manifest hash; and
- prior-complete-manifest pointer for safe fallback.

An incomplete manifest must never become active. Adversarial fixtures must be
listed separately and must never be eligible as ordinary answer evidence.

**Proposed outputs:**

- `schemas/ai_corpus_manifest_v1.schema.json`;
- `evaluation/ai-orchestration/p2/corpus-manifest-v1.json`;
- `evaluation/ai-orchestration/p2/exclusions-v1.json`; and
- valid and invalid manifest examples.

**Status:** Complete in
`docs/planning/ai-orchestration-p2-wp6-corpus-manifest-exclusion-contract-20260905.md`.
The closed manifest and exclusion schemas pin one immutable local active
manifest, all policy/contract versions, exact evidence/passages and hashes,
seven explicit negative decisions, an empty-but-defined tombstone boundary and
fail-safe no-prior-manifest state. Manifest completeness is explicitly separate
from evaluation coverage: `SF-08` plus all eight document passages are active,
while `SF-01` through `SF-07` and their seven required cases remain blocked by
the WP4 operand requirement. Three reason-checked manifest mutations and one
exclusion mutation are rejected.

### WP7 - Instantiate The P1 Evaluation Cases

Create metadata and gold labels for exactly the bounded first set:

- 4 structured cases;
- 4 document cases;
- 4 combined cases;
- 4 stale cases;
- 4 conflicting cases;
- 4 unsafe/unauthorized cases; and
- 4 unanswerable/invalid cases.

Maintain the P1 split of 7 calibration, 7 development and 14 holdout cases.
Each family contributes one calibration, one development and two holdout
cases. Every case must record expected route, required/forbidden evidence IDs,
allowed outcome codes, mandatory facts/qualifications, prohibited claims,
freshness/conflict/safety assertions and scoring-rule version.

Holdout gold labels must remain separate from candidate prompts and tuning
inputs. Synthetic stale, conflict and injection fixtures must be visibly
labelled and must not be confused with observed production evidence.

**Proposed outputs:**

- `schemas/ai_evaluation_case_v1.schema.json`;
- `evaluation/ai-orchestration/p2/evaluation-set-v1.json`;
- a separate holdout-gold file or equivalent access boundary; and
- a coverage report mapping 28 cases to evidence and policy fixtures.

**Status:** Complete in
`docs/planning/ai-orchestration-p2-wp7-evaluation-case-contract-20260905.md`.
The closed case, policy-fixture and holdout-gold schemas instantiate exactly 28
cases with the required 7/7/14 split and one/one/two split per family. Fourteen
holdout labels are physically separate and candidate-ineligible; sixteen
synthetic policy fixtures are `adversarial_only` and cannot become answer,
ordinary-retrieval or tuning evidence. Active and contract-blocked WP6 evidence
dependencies reconcile without making `SF-01` through `SF-07` answer-ready.

### WP8 - Local Validators And Decision Record

Add the minimum local validation needed to prove:

- every schema accepts its valid examples and rejects known-bad examples;
- every selected manifest entry resolves to one exact local fixture;
- hashes, IDs, coordinates and citation labels are stable;
- all 28 cases have a unique ID and correct split/family counts;
- expected evidence exists for answerable cases;
- intended no-evidence cases do not accidentally resolve accepted evidence;
- adversarial fixtures are ineligible for ordinary answers;
- no excluded path, private locator, secret or identity data appears;
- local Markdown links and JSON syntax are valid; and
- P1 remains unchanged unless a discovered contradiction requires an explicit
  revision.

**Output:** validation script/tests, validation summary and P2 advance/revise/
stop decision.

**Status:** Complete in
`docs/planning/ai-orchestration-p2-wp8-validation-decision-20260905.md`.
The durable validator recomputes canonical hashes, resolves manifest and case
references, reconciles identities/counts/timestamps/freshness, protects the
blocked, holdout and adversarial boundaries, validates local links/redaction
and rejects nine known-bad semantic mutations for their intended reason. All
P2 exit criteria pass. The decision is to advance to a bounded P3 local
retrieval benchmark only after an explicit continuation instruction.

## Ordered Execution

| Order | Action | Gate before continuing |
|---:|---|---|
| 1 | Inventory candidates and write selection/exclusion rationale | Every selected item maps to a P1 case; no selection is based only on convenience |
| 2 | Define authority, classification, access, citation and freshness rules | Public/internal boundaries and deterministic stale/conflict outcomes are reviewable |
| 3 | Define structured and document evidence schemas | Required fields, null semantics, bounded parameters and known-bad examples are explicit |
| 4 | Define the corpus manifest and adversarial separation | One complete immutable manifest resolves; incomplete manifests cannot activate |
| 5 | Instantiate and label the 28 evaluation cases | Counts, splits, evidence mappings and forbidden claims reconcile exactly |
| 6 | Run local validators and redaction checks | All valid/invalid, resolution, hash, count, link and redaction checks pass |
| 7 | Record P2 decision and handoff | P2 is marked complete only if every exit criterion passes; otherwise revise or stop |

Do not begin P3 retrieval comparisons during P2 validation. P3 starts only
after the P2 decision is recorded and the user requests continuation.

## Validation Commands And Evidence

Use repository-native checks where available and add only a narrow P2
validator when required. The final P2 validation should include:

- JSON parsing for every new manifest, fixture and example;
- JSON Schema validation for good and known-bad examples;
- exact family/split/count reconciliation for the 28 cases;
- stable hash and reference-resolution checks;
- exclusion and adversarial-eligibility assertions;
- `git diff --check` scoped to P2 files;
- `scripts/check_public_evidence_redaction.sh`;
- literal-path `markdownlint-cli2` for changed Markdown files; and
- `git status --short --branch` with unrelated dirty files preserved.

No live data refresh, model call, network fetch, Terraform command or AWS API is
part of P2 validation.

## Exit Criteria

P2 is complete only when:

- the selected corpus is at or below the size boundary and every item is
  justified by P1 case coverage;
- an explicit exclusion register prevents broad or unsafe ingestion;
- structured/document evidence and corpus-manifest schemas are versioned,
  strict and locally validated;
- public citation and internal provenance are distinct;
- freshness, conflict, missing/null and incomplete-manifest behaviour are
  deterministic;
- the 28-case evaluation set and 7/7/14 split reconcile exactly;
- answerable cases resolve expected evidence and negative cases preserve their
  intended outcome;
- known-bad examples fail for the intended reason;
- redaction, Markdown, JSON, reference and diff checks pass;
- no AWS, model, retrieval-engine or deployment choice is made; and
- the final record states whether to advance to P3, revise P2 or stop.

## Stop And Escalation Rules

Stop and request direction when:

- a required fixture contains private, secret, identity-bearing, licensed or
  otherwise unsuitable content that cannot be safely minimized;
- the 28 P1 cases require a materially larger corpus than this plan allows;
- a public citation cannot preserve both verifiability and the repository's
  private-location boundary;
- freshness or conflict rules cannot be made deterministic from available
  metadata;
- the only route forward requires live AWS data, an external fetch, model
  invocation or retrieval technology selection;
- P2 evidence contradicts the stakeholder, outcome or promotion gates in P1;
  or
- continuing would displace the interview assignment, STAR preparation or
  rehearsal.

## Tracker And Interview Mapping

- **Tracker:** advances only the active interview-linked AI workstream and
  produces a planning artifact without reopening SAP-C02 or parked work.
- **Lakehouse:** preserves curated authority, public/private boundaries,
  provenance, replay and deterministic failure handling.
- **SAP-C02 Domain 1:** classification, least privilege, deny-by-default access,
  auditability and safe public citation.
- **SAP-C02 Domain 2:** proportionate contracts, bounded query paths and
  evidence-led architecture sequencing.
- **SAP-C02 Domain 3:** freshness, validation, failure isolation,
  reproducibility and regression controls.
- **Interview:** supports System Architecture, GenAI Fluency, Customer
  Obsession, Invent and Simplify and Business Acumen without overstating
  implementation or customer impact.

## Plan Decision

- The P2 execution plan is complete.
- The first 2026-09-05 local-only admission-gate trial failed and remains
  recorded as audit history.
- After an explicit read-only evidence-boundary expansion, the replacement
  eight structured facts and eight document passages passed all four WP1
  gates. The WP1 selection/exclusion decision is accepted.
- WP2 is complete in
  `docs/planning/ai-orchestration-p2-wp2-authority-classification-access-rules-20260905.md`.
  Authority, classification, read-only scope, item-level route eligibility,
  citation projection, revocation and deny-by-default rules are defined.
- WP3 is complete in
  `docs/planning/ai-orchestration-p2-wp3-freshness-version-conflict-rules-20260905.md`.
  Historical/current freshness, version identity, timezone, manifest fallback,
  conflict materiality and bounded outcome rules are defined.
- The WP4 structured-evidence schema, field dictionary, valid example and
  three reason-checked invalid examples are complete in
  `docs/planning/ai-orchestration-p2-wp4-structured-evidence-contract-20260905.md`.
- The WP5 document-evidence schema, field dictionary, valid example and three
  reason-checked invalid examples are complete in
  `docs/planning/ai-orchestration-p2-wp5-document-evidence-contract-20260905.md`.
- The WP6 immutable active corpus manifest, exclusion register, strict schemas,
  field dictionary and reason-checked negative examples are complete in
  `docs/planning/ai-orchestration-p2-wp6-corpus-manifest-exclusion-contract-20260905.md`.
- The WP7 exact 28-case evaluation set, 7/7/14 split, sixteen isolated policy
  fixtures, separate fourteen-record holdout gold boundary and candidate-safe
  coverage report are complete in
  `docs/planning/ai-orchestration-p2-wp7-evaluation-case-contract-20260905.md`.
- WP8 is complete in
  `docs/planning/ai-orchestration-p2-wp8-validation-decision-20260905.md`.
  The schema and semantic suites pass, nine known-bad semantic mutations fail
  for their intended codes, and the decision is to advance to P3 only after
  explicit continuation.
- No corpus item is approved merely because it appears in the candidate table.
- No AWS resource, model, retrieval service or deployment has been changed.
- P2 is complete. The next coherent action, after explicit continuation, is P3:
  run the bounded local retrieval benchmark without selecting a generation
  model, managed service, deployment topology or AWS change first.
