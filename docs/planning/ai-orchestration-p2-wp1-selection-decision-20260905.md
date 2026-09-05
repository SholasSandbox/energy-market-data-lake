# AI Orchestration P2 WP1 Evidence Selection Decision

<!-- markdownlint-disable MD013 MD060 -->

**Decision date:** 2026-09-05<br>
**Status:** Accepted for WP1; P2 remains in progress<br>
**Evidence pack:**
`docs/evidence/ai-orchestration-p2-wp1-selected-evidence-20260905.json`<br>
**AWS changes:** None; read-only source discovery only

## Decision

Accept the eight structured facts and eight bounded document passages in the
evidence pack as the smallest evidence set for P2 contract design. The set
passed the selection-stage public-safety, alignment, reuse-basis and holdout-
independence gates.

This decision revises the earlier proposed source mix. It selects Elexon BMRS
facts compatible with query-contract shape 8 plus one bounded BMRS source-
metadata fact. It does not select ENTSO-E day-ahead prices or query-contract
shape 9 because an affirmative public-reuse basis for those price data was not
established. The official ENTSO-E terms require users to protect primary-owner
rights and check the free-reuse list; day-ahead prices are absent from the
reviewed list. Treating that absence as a failed reuse gate is a conservative
inference, not a claim that all access to or use of the data is prohibited.

WP1 acceptance does not activate a corpus, approve `athena/query-contracts.json`
for production, execute an Athena query, define evaluation gold, or prove that
all 28 P1 cases already resolve. Those are later P2 work packages.

## Selected Structured Facts

| ID | P1 coverage | Fact | Effective date | Stable source evidence |
|---|---|---|---|---|
| `SF-01` | `CO-01` | GB daily average system buy price, `112.605666 GBP/MWh` | 2026-03-31 | 48-row BMRS response hash |
| `SF-02` | `CO-02` | GB daily average system buy price, `95.492496 GBP/MWh` | 2026-04-02 | 48-row BMRS response hash |
| `SF-03` | `CO-03` | GB daily average system buy price, `78.360184 GBP/MWh` | 2026-04-21 | 48-row BMRS response hash |
| `SF-04` | `CO-04` | GB daily average system buy price, `100.806875 GBP/MWh` | 2026-05-27 | 48-row BMRS response hash |
| `SF-05` | `ST-01` | GB daily average net imbalance volume, `-30.357954 MWh` | 2026-02-25 | 48-row BMRS response hash |
| `SF-06` | `ST-02` | GB daily average net imbalance volume, `-247.200669 MWh` | 2026-05-28 | 48-row BMRS response hash |
| `SF-07` | `ST-03` | GB daily average net imbalance volume, `-90.814152 MWh` | 2026-08-21 | 48-row BMRS response hash |
| `SF-08` | `ST-04` | BMRS `INDO` source last-updated time, `2026-09-05T08:00:00Z` | 2026-09-05 | 100-row BMRS dataset-metadata response hash |

The full response hashes, source URLs, calculation rules, units and timestamps
are in the evidence pack. The seven daily facts came from seven different
date-bounded responses. `SF-08` came from a separate metadata response. No
Athena query was run. The values were calculated locally from public, read-only
responses and rounded deterministically to six decimal places.

`ST-04` must be instantiated as a present-record/missing-requested-metric case:
the selected record may prove the `INDO` update timestamp but must return
`insufficient_evidence` for a different metric absent from that record. Absence
must not be converted to zero.

## Selected Document Passages

| ID | P1 coverage | Publisher | Publication | Date | Use boundary |
|---|---|---|---|---|---|
| `DP-01` | `CO-01` | Ofgem | Managing business energy costs in an uncertain market | 2026-03-31 | Same-day market context; no causation claim |
| `DP-02` | `CO-02` | DESNZ | Energy Trends and Prices statistical release: 2 April 2026 | 2026-04-02 | Same-day historical generation context |
| `DP-03` | `CO-03` | Ofgem | Strategic energy planning and connections reform in 2026 | 2026-04-21 | Same-day long-term demand context |
| `DP-04` | `CO-04` | Ofgem | Energy price cap will rise by 13% from July | 2026-05-27 | Explains the cap change, not the daily price fact |
| `DP-05` | `DO-01` | DESNZ | Decisive action to break influence of gas on electricity prices | 2026-04-21 | Known-passage lookup |
| `DP-06` | `DO-02` | DESNZ | Your energy bill from April: what's changing | 2026-02-25 | Metadata and provenance lookup |
| `DP-07` | `DO-03` | DESNZ | Energy Trends and Prices statistical release: 28 May 2026 | 2026-05-28 | Paraphrased lookup |
| `DP-08` | `DO-04` | Ofgem | Changes to energy price cap between 1 April and 30 June 2026 | 2026-02-25 | Association-versus-causation boundary |

Only the bounded passages, their hashes, titles, dates, publishers, source
sections and canonical HTTPS URLs are retained. No full article, logo, contact
detail, named third-party quotation or separately identified third-party
material is copied. The four combined pairs match on GB topic and exact
structured-effective/document-publication date. Context must remain distinct
from measured fact and cannot silently become a causal explanation.

## Four-Part Admission Gate

| Gate | Result | Evidence |
|---|---|---|
| Public safety | Pass | All selected fields are public source facts or bounded public passages. No private path, internal S3 locator, account identity, secret, contact detail or operational control is present. |
| Alignment | Pass | Four `CO-*` pairs match region, energy topic and exact date. The remaining facts and passages cover the four `ST-*` and four `DO-*` shapes, including a deliberate missing-metric outcome and a causation boundary. |
| Licensing or reuse basis | Pass | BMRS facts carry the Elexon open-data attribution. Ofgem and GOV.UK Crown content is bounded to material covered by OGL v3.0. ENTSO-E price candidates and unlicensed RSS descriptions are excluded. |
| Holdout independence | Pass for WP1 selection | Eight distinct document pages and eight independently addressable structured results are frozen without prompts, split assignments, expected outcomes or gold labels. The disclosed P1 example date is not selected. WP7 must keep holdout gold separate. |

## WP1 Decisions Against The Ten Questions

1. **Smallest structured subset:** eight facts: four date-bounded GB price
   facts for `CO-*`, three date-bounded GB imbalance facts for `ST-*`, and one
   source-freshness fact that also supports a deliberate missing-metric test.
2. **Allowed structured route:** select only the bounded output shape of query
   contract 8 and the `bmrs-dataset-latest-metadata-v1` precomputed fact shape.
   Parameters are allowlisted date and GB region only. No arbitrary SQL, table,
   column, metric or output-location input is permitted. WP3/WP4 found contract
   12 unnecessary for the selected source fact, so it remains `reference_only`.
   Contract 9 is rejected for this public evaluation corpus pending an
   affirmative reuse basis for its price data.
3. **Smallest document subset:** eight bounded passages from eight distinct
   official Ofgem or GOV.UK pages: four `DO-*` passages and four exact-date
   `CO-*` passages.
4. **Negative controls:** stale copies, conflicting variants, prompt injection,
   private-locator requests, absent metrics and unsupported date/region cases
   are `adversarial_only` synthetic evaluation controls. They are never
   approved answer evidence.
5. **Exclusions:** deny repository-wide ingestion and exclude `raw/`, `failed/`,
   private or identity-bearing paths, secrets, credentials, internal S3/file
   locators, logs, state, registration/payment records, operational control
   files, copied RSS descriptions without a reuse basis, full external
   articles, logos, third-party material, and source-deleted or revoked items.
6. **Mandatory identity and provenance:** stable evidence/passage ID, contract
   version, source and dataset, canonical public URL when permitted, source or
   passage SHA-256, effective/publication/update/validation time, region,
   metric/value/unit or passage coordinates, classification, access scope,
   licence/reuse basis, status and public-safe citation label.
7. **Freshness boundary:** exact-date historical questions use the requested
   effective/publication window and the frozen hash rather than a relative-age
   expiry. Current operational structured facts use the source effective or
   last-updated time and a candidate maximum age of 36 hours. Current document
   context uses the source update time, falling back to publication time, and a
   candidate maximum age of seven days. An active derived manifest must be
   complete and validated; detailed rules remain WP3 work.
8. **Citation boundary:** internal evaluation cites the stable evidence ID,
   version/hash and internal provenance reference. A future public answer cites
   publisher/dataset, title or metric, date/window and canonical HTTPS URL plus
   required attribution. It never exposes an internal S3 URI, query-result
   location, private path, account identity or secret.
9. **Exceptional states:** represent null, absent, zero and not-applicable
   separately. Use explicit `conflicting`, `stale`, `revoked`, `source_deleted`
   and `manifest_incomplete` states. Never activate an incomplete manifest;
   use a validated prior-complete manifest only when the question's freshness
   rule still permits it, otherwise return the P1 non-answer outcome.
10. **Twenty-eight-case determinism:** the selected evidence is sufficient to
    proceed, but deterministic resolution of all 28 cases is not yet proven.
    WP7 must map every case to selected evidence or an isolated policy fixture,
    and WP8 must validate the 7/7/14 split and all intended answer/non-answer
    outcomes before P2 can be marked complete.

## Selection And Exclusion Register

| Candidate | Status | Reason | Reconsideration trigger |
|---|---|---|---|
| Evidence-pack `SF-01` through `SF-08` | `selected` | Passed all four WP1 gates | Re-run if source licence, hash, field meaning or public status changes |
| Evidence-pack `DP-01` through `DP-08` | `selected` | Passed all four WP1 gates | Re-run if source licence, page version, hash or third-party status changes |
| Query contract 8 output shape | `selected` | Smallest allowlisted shape covering the chosen GB metrics; WP4 narrowed it to exact date and GB parameters with no SQL/table/column surface | Reconsider only through a new structured-contract version and evidence decision |
| BMRS dataset-latest metadata shape | `selected` | Supplies one bounded freshness fact without SQL | Replace only if a stricter authoritative freshness source is adopted |
| Query contract 12 | `reference_only` | Useful future curated-table coverage shape but WP3/WP4 found it unnecessary for the selected source fact | A later version proves it is required and safe |
| Query contract 9 and inspected FR/DE/NL price candidates | `rejected` | No affirmative free-reuse basis established for the day-ahead price data | Written permission, a controlling open licence, or an alternative open source |
| Local structured sample and dashboard snapshot | `reference_only` | Useful schema/presentation examples; internal locators and presentation authority prevent answer-evidence use | Public-safe authoritative source result with exact provenance |
| Local RSS descriptions | `rejected` | Feed availability did not establish permission to retain copied descriptions | Affirmative reuse basis or compliant independently authored abstract |
| Sanitised regression artifacts | `adversarial_only` after separate review | May test failures but cannot become ordinary answer evidence | Explicit negative-fixture classification and validator coverage |
| Raw, failed, private, secret, identity-bearing, copyrighted or operational repository/AWS material | `rejected` | Violates the deny-by-default corpus boundary | Explicit new decision plus public-safety, authority and reuse review |

## Alternatives And Trade-Offs

### Selected: one open GB structured source plus official Crown documents

This is the smallest auditable set with explicit reuse bases, deterministic
dates and public citations. It is narrow: it does not demonstrate cross-market
European retrieval or prove the curated Athena path. That limitation is
preferable to retaining broader evidence with uncertain reuse rights.

### Rejected: retain ENTSO-E day-ahead price facts

This would improve regional diversity and match the original proposed mix.
It was rejected because the public-corpus reuse gate is not satisfied for the
selected price item. Revisit only with affirmative rights or a suitable open
alternative.

### Rejected: reuse local RSS summaries

This is convenient and topically broad, but a feed description is not by
itself an affirmative licence for a durable evaluation corpus. Official Crown
pages provide a cleaner, reviewable basis.

### Rejected: manufacture all evidence synthetically

Synthetic data would simplify licensing and holdout control but would weaken
the case study's evidence-grounded claim. Synthetic content is therefore
limited to negative controls, never accepted answer evidence.

## Decision Consequence And Next Priority

WP1 remains complete. WP2 through WP6 subsequently defined authority/access,
freshness/version/conflict and the strict structured- and document-evidence
contracts plus immutable manifest/exclusion governance in
`docs/planning/ai-orchestration-p2-wp2-authority-classification-access-rules-20260905.md`
`docs/planning/ai-orchestration-p2-wp3-freshness-version-conflict-rules-20260905.md`,
then
`docs/planning/ai-orchestration-p2-wp4-structured-evidence-contract-20260905.md`
and
`docs/planning/ai-orchestration-p2-wp5-document-evidence-contract-20260905.md`,
then
`docs/planning/ai-orchestration-p2-wp6-corpus-manifest-exclusion-contract-20260905.md`.
The next tracker-ordered action is P2 WP7: instantiate the exact 28-case set and
separate holdout gold. P2 remains incomplete, and no P3 retrieval or model work
may begin.
