# AI Orchestration P2 WP2 Authority, Classification And Access Rules

<!-- markdownlint-disable MD013 MD060 -->

**Decision date:** 2026-09-05<br>
**Status:** Complete for WP2; P2 remains in progress<br>
**Policy ID:** `ai-evidence-access-policy-v1`<br>
**Selected evidence:**
`docs/evidence/ai-orchestration-p2-wp1-selected-evidence-20260905.json`<br>
**WP1 decision:**
`docs/planning/ai-orchestration-p2-wp1-selection-decision-20260905.md`<br>
**AWS changes:** None; this is a local documentation contract

## Objective

Define the authority, information-classification, read-only access, route,
citation and revocation rules for the WP1-selected evidence before freshness,
schema, manifest, fixture, retrieval or model work begins.

The contract applies to one internal decision-support user and the frozen
WP1 evidence pack. It does not create a production corpus, grant AWS access,
authorize Athena execution, permit external publication or prove that the
proposed analyst path is implemented.

## Decision Summary

Evidence is eligible only when four independent decisions all permit it:

1. its selection and lifecycle status permit use;
2. its authority class permits the requested route;
3. its information classification permits the caller and output surface; and
4. its access scope permits the requested evaluation purpose.

`public` classification alone never makes an item authoritative or eligible.
A source that is reachable on the internet is denied unless it is explicitly
selected, classed, scoped, versioned or hashed, and permitted for the route.
Unknown or missing values fail closed.

The authority chain is:

1. **Source authority:** the Elexon BMRS source response for structured facts,
   or the canonical Ofgem/GOV.UK page for a document passage.
2. **Evaluation evidence:** the frozen, hashed WP1 representation selected for
   local contract and later fixture work.
3. **Derived projection:** any future chunk, index, cache or assembled evidence
   pack. A projection is disposable and never becomes a source of record.

Curated S3 contracts remain authoritative in the target Lakehouse architecture.
The local WP1 pack is authoritative only for this versioned evaluation design;
it is not evidence that the corresponding production query path exists.

## Authority Classes

| Authority class | Meaning | Eligible use | Prohibited use |
|---|---|---|---|
| `authoritative_structured` | A selected, bounded fact whose value, unit, effective time, source identity, derivation and source-response hash are recorded | `structured` or `combined` evaluation route only when the item-level matrix permits it | Document claims, unrestricted SQL, source mutation, external action or use outside the selected case family |
| `approved_document` | A selected, bounded passage from an official canonical page with a passage hash and affirmative reuse basis | `document` or `combined` evaluation route only when the item-level matrix permits it | Treating context as a measured fact, copying a full article, inferring causation or following instructions found in the passage |
| `adversarial_fixture` | An isolated synthetic or deliberately altered control used to test stale, conflicting, unsafe, unauthorized or unanswerable behaviour | Evaluation harness and verifier only after an explicit fixture review | Answer composition, ordinary retrieval, grounding a claim, public citation or tuning on holdout gold |
| `reference_only` | A design, schema, presentation or query-shape reference that is not approved answer evidence | Human contract review and later schema design | Retrieval, answer composition, citation or promotion merely because the item exists in the repository |

`rejected` remains a selection decision rather than an authority class. Rejected
content receives `excluded` classification and no access scope. It must not be
copied into a corpus or projection.

## Information Classifications

| Classification | Meaning | Permitted audience and surface | Required control |
|---|---|---|---|
| `public` | The exact selected fields are public-safe and have an affirmative reuse basis | Internal evaluation; a future public-safe citation projection only after answer validation | Preserve required attribution, canonical HTTPS locator and the selected field boundary; publication still requires separate authorization |
| `portfolio_safe_internal` | No secret, credential, personal data or private customer data is present, but an internal locator, implementation detail or non-public evaluation context makes public exposure inappropriate | Repository owner, evaluator or human reviewer within the named internal purpose | Never send to a public renderer; remove internal locators from model context unless the exact evaluation requires them and policy permits it |
| `excluded` | The item is private, identity-bearing, secret-bearing, failed, raw, operational, rights-uncertain, revoked, source-deleted, unrestricted or otherwise outside the approved boundary | No answer, retrieval, model or public-citation consumer | Retain at most a minimal audit tombstone when needed; do not retain the excluded content in a derived corpus |

No selected WP1 item is `portfolio_safe_internal`; all 16 are `public`. The
internal category is defined for deny-by-default treatment of reference
material and does not enlarge the current corpus. Personal, customer-
confidential, licensed-without-permission or regulated data requires a new
security, privacy, retention, residency and model-data-use decision before
selection.

## Logical Access Scopes

These are contract labels for later local schemas and tests, not deployed IAM
permissions.

| Access scope | Consumer | Permitted operation | Explicit denial |
|---|---|---|---|
| `read_only_evaluation` | Route selector, evidence assembler, answer verifier and internal evaluator | Read the exact selected fields for the assigned P1 family; produce a local public-safe citation projection | Source write, arbitrary query, broad repository read, external publication, tool escalation or route widening |
| `adversarial_evaluation_only` | Evaluation harness and safety verifier | Inject an isolated reviewed control and score the expected non-answer or policy outcome | Ordinary retrieval, answer evidence, citation, prompt tuning or holdout-gold exposure |
| `reference_review` | Repository owner or human contract reviewer | Inspect a named design/query/schema/presentation reference | Automated retrieval, model context, answer composition or citation |
| `none` | No consumer | Minimal identifier/reason tombstone only | All evidence-content access and route eligibility |

The current WP1 pack uses `read_only_evaluation`. Later schemas may encode the
other values, but WP2 does not authorize creating those schemas or fixtures.

## Consumer And Trust-Boundary Matrix

| Consumer boundary | May read | May produce | Must not access or do |
|---|---|---|---|
| Internal analyst caller | Validated answer fields and public-safe citations for an allowed local evaluation case | Read-only question and feedback | Internal provenance, credentials, raw/failed data, unrestricted SQL, publication or AWS mutation |
| Route selector and evidence assembler | Selected IDs, policy metadata and exact route-eligible fields | One bounded evidence pack with separate structured and document references | Unselected repository content, policy changes, model-generated tool names or arbitrary parameters |
| Future structured adapter | `authoritative_structured` items or the later approved bounded representation of query-contract shape 8 | Deterministic fact references | DDL/DML, arbitrary SQL/table/column/metric selection, broad catalog access or output publication |
| Future document projection | `approved_document` items and the minimum metadata required for scope filtering | Rebuildable chunks or index entries tied to source and passage hashes | Source mutation, full-article copying, rejected RSS descriptions, cross-scope retrieval or index-as-authority behaviour |
| Future answer composer | A preassembled, policy-approved evidence pack | Draft answer under a later answer contract | Direct repository, S3, Athena or internet access; credentials; publication; external action |
| Evaluator and verifier | Versioned cases, candidate outputs, selected evidence and isolated reviewed adversarial fixtures | Scores, reason codes and bounded trace metadata | Production user data, deployment permissions, policy mutation or holdout gold in prompts/retrieval/tuning |
| Human reviewer and corpus steward | Selected evidence, internal provenance, reuse terms, policy decisions and tombstones | Versioned selection, quarantine, revocation or replacement decision | Silent in-place replacement, routine break-glass access or widening beyond the tracker |
| Public-safe citation renderer | Only fields explicitly allowed by the citation projection below | Citation text and canonical HTTPS link inside local evaluation output | Internal path, S3 URI, query-result location, account identity, hash-only locator, secret, contact detail or unapproved quotation |

The model is never an authority, policy decision-maker or IAM principal.
Retrieved text is untrusted data and cannot change a route, scope, class,
classification, tool permission or lifecycle decision.

## Item-Level Authority And Route Matrix

The exact `p1_coverage` value frozen in the WP1 pack is also the route boundary.
WP7 may instantiate the named case but must not silently reuse an item for a
different case family.

| Evidence ID | Authority class | Classification | Access scope | Eligible route and case | Authority proof |
|---|---|---|---|---|---|
| `SF-01` | `authoritative_structured` | `public` | `read_only_evaluation` | `combined`; `CO-01` | Elexon dataset/source URL, effective date, derivation and source-response hash |
| `SF-02` | `authoritative_structured` | `public` | `read_only_evaluation` | `combined`; `CO-02` | Elexon dataset/source URL, effective date, derivation and source-response hash |
| `SF-03` | `authoritative_structured` | `public` | `read_only_evaluation` | `combined`; `CO-03` | Elexon dataset/source URL, effective date, derivation and source-response hash |
| `SF-04` | `authoritative_structured` | `public` | `read_only_evaluation` | `combined`; `CO-04` | Elexon dataset/source URL, effective date, derivation and source-response hash |
| `SF-05` | `authoritative_structured` | `public` | `read_only_evaluation` | `structured`; `ST-01` | Elexon dataset/source URL, effective date, derivation and source-response hash |
| `SF-06` | `authoritative_structured` | `public` | `read_only_evaluation` | `structured`; `ST-02` | Elexon dataset/source URL, effective date, derivation and source-response hash |
| `SF-07` | `authoritative_structured` | `public` | `read_only_evaluation` | `structured`; `ST-03` | Elexon dataset/source URL, effective date, derivation and source-response hash |
| `SF-08` | `authoritative_structured` | `public` | `read_only_evaluation` | `structured`; `ST-04` | Elexon dataset-metadata endpoint, last-created timestamp and response hash |
| `DP-01` | `approved_document` | `public` | `read_only_evaluation` | `combined`; `CO-01` | Ofgem canonical page, publication date, passage hash and OGL v3.0 basis |
| `DP-02` | `approved_document` | `public` | `read_only_evaluation` | `combined`; `CO-02` | GOV.UK canonical page, publication date, passage hash and OGL v3.0 basis |
| `DP-03` | `approved_document` | `public` | `read_only_evaluation` | `combined`; `CO-03` | Ofgem canonical page, publication date, passage hash and OGL v3.0 basis |
| `DP-04` | `approved_document` | `public` | `read_only_evaluation` | `combined`; `CO-04` | Ofgem canonical page, publication date, passage hash and OGL v3.0 basis |
| `DP-05` | `approved_document` | `public` | `read_only_evaluation` | `document`; `DO-01` | GOV.UK canonical page, publication date, passage hash and OGL v3.0 basis |
| `DP-06` | `approved_document` | `public` | `read_only_evaluation` | `document`; `DO-02` | GOV.UK canonical page, publication date, passage hash and OGL v3.0 basis |
| `DP-07` | `approved_document` | `public` | `read_only_evaluation` | `document`; `DO-03` | GOV.UK canonical page, publication date, passage hash and OGL v3.0 basis |
| `DP-08` | `approved_document` | `public` | `read_only_evaluation` | `document`; `DO-04` | Ofgem canonical page, publication date, passage hash and OGL v3.0 basis |

`SF-08` is authority for the selected dataset-metadata fact, not for a missing
requested market metric. Its `ST-04` use must preserve the difference between
present metadata, an absent metric, null, zero and not-applicable.

The four `CO-*` pairs may be assembled together only for the matching case ID.
Document context does not cause or validate the structured measurement, and a
structured value does not prove a document's explanatory claim.

## Candidate And Exclusion Mapping

| Candidate or category | WP2 class | Classification | Access scope | Route eligibility |
|---|---|---|---|---|
| WP1 `SF-01` through `SF-08` | `authoritative_structured` | `public` | `read_only_evaluation` | Item-level `structured` or `combined` route only |
| WP1 `DP-01` through `DP-08` | `approved_document` | `public` | `read_only_evaluation` | Item-level `document` or `combined` route only |
| Query-contract shape 8 definition | `reference_only` | `public` | `reference_review` | WP4 now permits only its bounded shape through selected structured facts; the definition itself remains non-answer evidence and does not authorize Athena execution |
| BMRS dataset-metadata shape | `reference_only`; selected `SF-08` is the authority | `public` | `reference_review` | None as a shape; `SF-08` follows its item-level route |
| Query-contract shape 12 | `reference_only` | `public` | `reference_review` | None; WP3/WP4 found it unnecessary for the selected source fact |
| Local structured sample and dashboard snapshot | `reference_only` | `portfolio_safe_internal` | `reference_review` | None; presentation values and internal locators are not answer evidence |
| Reviewed synthetic negative control | `adversarial_fixture` | `portfolio_safe_internal` unless every field is explicitly public | `adversarial_evaluation_only` | None for answer routes; evaluator/verifier only |
| Unreviewed sanitised regression artifact | No admitted class | `excluded` | `none` | None until separate adversarial-fixture review |
| Query-contract shape 9 and inspected ENTSO-E price candidates | Rejected | `excluded` | `none` | None pending affirmative reuse rights or a newly selected open source |
| Copied RSS descriptions | Rejected | `excluded` | `none` | None pending affirmative reuse rights or a compliant independently authored replacement |
| Raw, failed, private, secret, identity-bearing, copyrighted or operational material | Rejected | `excluded` | `none` | None; a new explicit selection and security/reuse decision is mandatory |

## Provenance And Public-Citation Separation

Internal provenance supports audit and replay; a public-safe citation supports
the reader. One must never be substituted for the other.

| Evidence class | Internal provenance reference | Public-safe citation projection |
|---|---|---|
| `authoritative_structured` | Evidence-pack ID/version, evidence ID, source dataset ID, source-response hash, derivation, effective date, source row count, source-created time and compatible query-shape ID when present | Metric label, value/unit when the answer permits it, effective date/window, Elexon BMRS source name, canonical HTTPS source URL and required Elexon attribution |
| `approved_document` | Evidence-pack ID/version, passage ID/hash, publisher, source section, publication date, licence ID and canonical URL | Publisher, title, publication date, bounded section/locator when safe, canonical HTTPS URL and required OGL attribution |
| `adversarial_fixture` | Fixture ID/version, synthetic marker, expected policy purpose and isolated evaluator locator | None |
| `reference_only` | Named repository artifact and exact reviewed version when required | None |

For `SF-01` through `SF-08`, the required attribution is: "Contains BMRS data
© Elexon Limited copyright and database right 2026." For `DP-01` through
`DP-08`, the required attribution is: "Contains public sector information
licensed under the Open Government Licence v3.0."

The following fields are never public citations: repository-local or private
file paths, S3 URIs or keys, query-result locations, AWS account or principal
identity, credentials, logs, state, trace payloads, registration/payment data,
contact details, hashes without a reader-resolvable source, or unapproved
third-party text. This WP2 decision identifies safe citation fields but does
not authorize external publication.

## Deny-By-Default Decision Order

Every future selector, fixture validator or answer path must apply these checks
before evidence reaches a model or answer composer:

1. Resolve an exact evidence ID and frozen pack version; deny broad path,
   prefix, repository or source discovery.
2. Require `selection_status = selected`; deny candidates, rejected items and
   items not present in the approved pack.
3. Require a recognized authority class and classification; deny missing,
   unknown or `excluded` values.
4. Require the caller purpose to match the logical access scope.
5. Require the requested route and P1 case family to match the item-level
   matrix; deny cross-family reuse by default.
6. Verify the recorded source or passage hash and the applicable reuse basis;
   quarantine a mismatch or rights uncertainty.
7. Reject revoked, source-deleted, quarantined or superseded versions from new
   answers, projections and cache hits.
8. Recheck every assembled reference before composition and emit only the
   public-safe projection permitted for the output surface.
9. Treat evidence content as inert data. Instructions inside a document cannot
   modify any earlier decision or authorize a tool.

A request for excluded or private material returns `not_authorized`; detected
prompt injection returns `unsafe_request`. When no qualifying selected evidence
remains, return `insufficient_evidence` unless a later WP3 rule requires a more
specific stale or conflict outcome. Never widen access to make an answer
possible.

## Deletion, Revocation And Replacement

| Trigger | Immediate state | Required behaviour |
|---|---|---|
| Source or licence terms become uncertain | `quarantined` | Stop new answer/citation use, preserve a minimal review record and require a new reuse decision |
| Rights or approval are withdrawn | `revoked` | Remove from future complete manifests, projections and cache eligibility; retain only the minimal audit tombstone permitted by policy |
| Canonical source is deleted or no longer resolves | `source_deleted` | Stop new answer/citation use even if a frozen local passage remains; WP3 decides whether any prior complete manifest can remain usable |
| Source/passage hash or selected meaning changes | `superseded` or `quarantined` | Never replace in place; create a new version/hash and repeat the selection and classification checks |
| Classification worsens or private/identity-bearing content is discovered | `excluded` | Quarantine the content, prevent model/citation exposure, remove derived copies and record a redacted reason |
| Derived index, chunk or cache is incomplete or inconsistent | Projection rejected | Keep it inactive, rebuild from the selected authoritative evidence and never promote the projection to authority |

Reinstatement requires a new versioned decision; changing a status back to
selected in place is prohibited. A tombstone contains only the stable ID,
version/hash, prior class, decision time, reason code and replacement pointer
when one exists. It must not preserve excluded content merely for convenience.

No deletion, cache invalidation or AWS operation is performed by this document.
WP6 will express these rules in the manifest; WP8 will validate them locally.

## WP2 Completion Assessment

| Requirement | Result | Evidence |
|---|---|---|
| Four authority classes defined | Pass | Authority-class table distinguishes answer evidence, adversarial controls and references |
| Three information classifications defined | Pass | Classification table separates public, portfolio-safe internal and excluded material |
| Read-only access and route eligibility defined | Pass | Logical scopes, consumer matrix and 16 item-level route decisions fail closed |
| Internal provenance separated from public citation | Pass | Per-class projections and prohibited public fields are explicit |
| Deletion and revocation behaviour defined | Pass | Quarantine, revocation, source deletion, replacement and tombstone rules are versioned and non-destructive by default |
| Derived projections remain non-authoritative | Pass | Authority chain and projection rejection/rebuild rules preserve source and curated authority |
| Excluded material denied | Pass | Candidate mapping and decision order prohibit broad/raw/failed/private/secret/operational ingestion |
| Scope stayed inside WP2 | Pass | No freshness threshold, schema, manifest, fixture, retrieval, model, AWS or publication action was created |

WP2 is complete. The contract is reviewable and the state transition to WP3
is permitted. P2 remains incomplete until freshness, structured/document
schemas, manifest, 28-case fixtures and local validation pass in plan order.

## Alternatives And Trade-Offs

### Selected: independent authority, classification and scope dimensions

This prevents a public or locally available item from becoming answer evidence
without an explicit authority and route decision. It adds policy fields, but
the extra structure is proportionate to the P1 leakage and grounding red lines.

### Rejected: treat all public-source content as approved

This is simpler but conflates reachability, reuse rights, authority and answer
fitness. It would re-admit the rejected RSS and ENTSO-E candidates and weaken
the audit boundary.

### Rejected: one shared internal/public provenance object

This reduces duplication but creates a direct leakage path for internal S3,
file, query-result or account locators. Separate projections are required.

### Rejected: let the model decide authority or access

This is flexible but untestable as a security boundary. Code-enforced policy
and deterministic validation must decide before the model sees evidence.

### Deferred: AWS IAM implementation

ADR 0006 records the logical future roles, but no deployed query, retrieval or
answer path exists. IAM policies, resources and live proof require a later
topology decision and separate explicit AWS authorization.

## Reconsideration Triggers

Revisit WP2 before admitting:

- a new publisher, dataset, licence or evidence class;
- any `portfolio_safe_internal`, personal, customer-confidential, regulated or
  rights-restricted evidence into answer routes;
- a second user, tenant or materially different caller scope;
- public publication rather than local evaluation;
- write-capable tools, external actions or arbitrary query generation;
- a managed knowledge base or index with access semantics that cannot enforce
  the item-level matrix; or
- an AWS topology whose IAM, KMS, logging, network, retention or deletion model
  conflicts with this logical contract.

## Tracker And Interview Mapping

- **Tracker:** completes only P2 WP2 in the active interview-linked AI
  workstream and preserves the required evidence-led order.
- **Lakehouse:** preserves curated/source authority, replayable provenance,
  bounded evidence and disposable projections.
- **SAP-C02 Domain 1:** demonstrates classification, least privilege,
  deny-by-default access, separation of duties and safe public evidence.
- **SAP-C02 Domain 2:** selects a proportionate read-only contract before
  implementation or service selection.
- **SAP-C02 Domain 3:** defines quarantine, revocation, audit and rebuild
  behaviour without turning current intent into verified runtime claims.
- **Interview:** provides a truthful System Architecture, Security and GenAI
  Fluency decision without claiming a customer result or deployed query path.

## Decision Consequence And Next Priority

WP2 remains complete. WP3 through WP6 subsequently defined deterministic
freshness/version/conflict rules and the structured- and document-evidence
contracts plus immutable manifest/exclusion governance in
`docs/planning/ai-orchestration-p2-wp3-freshness-version-conflict-rules-20260905.md`.

The contracts are
`docs/planning/ai-orchestration-p2-wp4-structured-evidence-contract-20260905.md`
and
`docs/planning/ai-orchestration-p2-wp5-document-evidence-contract-20260905.md`.
The WP6 decision is
`docs/planning/ai-orchestration-p2-wp6-corpus-manifest-exclusion-contract-20260905.md`.
Execute P2 WP7 next: instantiate the exact 28-case set and separate holdout
gold. Do not begin final validators, retrieval, model selection, local
orchestration or AWS deployment before WP7 is reviewable and passes its gate.

State transition status: **WP6 is complete and the transition from manifest/
exclusion governance to evaluation-case design has occurred; WP7 is the next
bounded work package.**
