# AI Orchestration P2 WP7 Evaluation Case Contract

<!-- markdownlint-disable MD013 MD060 -->

**Decision date:** 2026-09-05<br>
**Status:** Complete for WP7; P2 remains in progress<br>
**Evaluation contract:**
`docs/planning/ai-orchestration-p1-evaluation-contract-20260901.md`<br>
**Corpus manifest:**
`evaluation/ai-orchestration/p2/corpus-manifest-v1.json`<br>
**AWS changes:** None; all artifacts and checks are local

## Outcome

WP7 instantiates exactly the 28 cases required by P1: four cases in each of
seven families, split into seven calibration, seven development and fourteen
holdout cases. Case `01` is calibration, `02` development and `03` and `04`
holdout in every family.

The package does not repair or obscure the WP4/WP6 evidence gap. `SF-01`
through `SF-07` remain unavailable because the selected aggregates lack their
required exact scalar operands. `ST-01` through `ST-03` and `CO-01` through
`CO-04` explicitly record those unavailable dependencies and cannot present the
blocked values as supported. `ST-04` uses active `SF-08` but asks for a metric
that the metadata record does not contain, preserving the P1 missing-is-not-zero
test.

## Artifacts

| Purpose | Schema or record |
|---|---|
| Candidate-visible cases and non-holdout gold | `schemas/ai_evaluation_case_v1.schema.json`; `evaluation/ai-orchestration/p2/evaluation-set-v1.json` |
| Synthetic policy inputs | `schemas/ai_policy_fixture_v1.schema.json`; `evaluation/ai-orchestration/p2/policy-fixtures-v1.json` |
| Physically separate holdout labels | `schemas/ai_holdout_gold_v1.schema.json`; `evaluation/ai-orchestration/p2/holdout/holdout-gold-v1.json` |
| Candidate-safe 28-case mapping | `evaluation/ai-orchestration/p2/coverage-report-v1.md` |

All three schemas use JSON Schema Draft 2020-12 and close every declared object
with `additionalProperties: false`.

## Case Metadata Dictionary

| Field | Contract meaning |
|---|---|
| `case_id` | Stable P1 identity from `ST-01` through `NA-04`. Prefix identifies the family; suffix identifies the split. |
| `family` | One of structured, document, combined, stale, conflicting, unsafe/unauthorized or unanswerable/invalid. |
| `split` | `calibration`, `development` or `holdout`; semantic reconciliation enforces one, one and two per family. |
| `case_shape` | One of the exact 28 P1-required shapes, such as exact value, timestamp alignment or indirect retrieved instruction. |
| `prompt` | Candidate-visible question or request. It contains no holdout result. |
| `as_of` | Frozen RFC 3339 instant used by WP3 rather than the runtime wall clock. |
| `request_mode` | Exact historical, current/latest, temporally ambiguous or policy precheck. |
| `candidate_visible` | Always `true` for case metadata. |
| `evidence_resolution_status` | `ready`, `contract_blocked` or `policy_fixture`; this is input state, not an expected outcome. |
| `candidate_input_scope` | Exact manifest identity/hash plus active evidence IDs, blocked dependency IDs and inert fixture IDs available to the case. |
| `gold` | Expected route, outcomes and deterministic assertions for calibration/development only. |
| `gold_boundary` | Opaque pointer for holdout cases; it exposes no expected route, outcome, fact or policy assertion. |

## Gold Dictionary

Both inline and separate gold use the same closed assertion shape:

| Field | Contract meaning |
|---|---|
| `gold_id`, `case_id` | Stable gold identity and its one case. `IG-*` is inline; `HG-*` is holdout. |
| `scoring_rule_version` | Pinned to `p1-deterministic-task-success-v1`. |
| `expected_route` | Structured, document, combined, policy guard or no retrieval route. |
| `allowed_outcome_codes` | Complete bounded set of acceptable P1 terminal outcomes. |
| `primary_outcome_code` | Deterministic expected result and a member of the allowed outcomes. |
| `required_available_evidence_ids` | Active selected evidence that must be retrieved and cited when applicable. |
| `required_unavailable_evidence_ids` | Explicit WP6 contract-blocked dependencies; never candidate answer content. |
| `required_fixture_ids` | Synthetic conditions needed to exercise a policy case; never answer evidence. |
| `forbidden_evidence_ids` | Excluded source, locator, operation or inference identifiers that must not be used. |
| `mandatory_facts` | Facts that must appear when the outcome permits a supported statement. |
| `mandatory_qualifications` | Required limitations, scope or abstention rationale. |
| `prohibited_claims` | Claims, conversions, causal links, disclosures or actions whose presence fails the case. |
| `assertions` | Pinned freshness, conflict, safety, citation, grounding, version-trace and cost-trace expectations. |

## Holdout Separation

The fourteen holdout cases contain only `HG-*` identifiers and a
`candidate_visible: false` pointer. Their expected routes, outcome codes,
evidence assertions, facts, qualifications and prohibited claims appear only in
`holdout/holdout-gold-v1.json`.

That file is contractually classified `holdout_gold` and sets candidate,
prompt, retrieval and tuning eligibility to `false`. Its only release condition
is a frozen candidate configuration for the holdout run. This is a repository
artifact separation and machine-readable input boundary, not an operating-
system access-control claim; a future harness must enforce it by building
candidate inputs from the evaluation set and fixture register only.

## Synthetic Fixture Boundary

Sixteen fixtures cover the four P1 policy-heavy families:

- four freshness/manifest conditions (`FIX-SA-*`);
- four material-conflict conditions (`FIX-CF-*`);
- four injection or authorization conditions (`FIX-UN-*`); and
- four unsupported or invalid-request conditions (`FIX-NA-*`).

Every fixture is `synthetic: true`, `adversarial_only`,
`portfolio_safe_internal`, not observed production evidence and ineligible for
answers, ordinary retrieval or tuning. Fixture text is inert and cannot change
policy or authorize tools. The fixture register contains input conditions and
tested rule IDs only; it contains no expected route, outcome or gold fact.

## Design Decisions And Trade-Offs

### Fixed per-family split

**Accepted:** suffix `01` is calibration, `02` development and `03`/`04`
holdout for every family.

**Rejected alternatives:** random allocation would make review and replay less
deterministic; family-block allocation would violate P1's one/one/two split.
Revisit only through a new evaluation-set version if P1 changes the split.

### Separate holdout record

**Accepted:** retain candidate-visible prompts and input identities in the main
set while placing all holdout expected results in a separately hashed file.

**Rejected alternatives:** embedding hidden-looking fields in the same case
object would make accidental prompt/tuning leakage easy; omitting gold entirely
would make the future frozen holdout irreproducible. Revisit only if a stronger
secret-store boundary is explicitly authorized for an actual evaluation run.

### Explicit blocked dependencies

**Accepted:** preserve the seven `SF-*` IDs as unavailable references and score
correct abstention or qualified partial evidence.

**Rejected alternatives:** copying aggregate values from WP1 would violate the
strict operand contract; deleting the affected cases would violate P1's exact
28-case contract. Revisit when approved scalar operands allow new evidence and
manifest versions to pass WP8.

### Separate inert fixtures

**Accepted:** keep policy conditions outside the answer corpus with deny-by-
default eligibility flags.

**Rejected alternatives:** treating synthetic conflict or injection text as
ordinary retrieved evidence would weaken route authority and contaminate answer
metrics. Revisit only through a new policy-fixture contract version.

## Validation Completed In WP7

Local schema and semantic reconciliation established:

- all three schemas are valid Draft 2020-12 schemas and accept their records;
- 28 unique ordered case IDs exist, with four cases per family;
- split counts are exactly 7 calibration, 7 development and 14 holdout;
- every family contributes exactly one calibration, one development and two
  holdouts;
- fourteen non-holdout cases contain inline gold and no holdout pointer;
- fourteen holdout cases contain only a separate gold pointer, and the separate
  file has exactly one matching gold record per holdout;
- sixteen unique fixture IDs resolve one-to-one to `SA-*`, `CF-*`, `UN-*` and
  `NA-*` cases and carry no gold fields;
- required available evidence resolves to the WP6 active manifest, while every
  required unavailable evidence ID resolves to the WP6 contract-blocked set;
- manifest, fixture, holdout and case hash references reconcile; and
- each primary outcome is included in its allowed outcome set.

The canonical hashes are:

- evaluation set: `25a7c2e01806205b1dc30b7d6f9f2580d897685ea5a89df52b5c9bfee7796c18`;
- policy fixture set: `9a106587ebca205db5a54c2c3556bf61765d8e226254725899d58789d7cc52c6`;
  and
- holdout gold set: `c70cc969a32ea5989e8cf78c8c5ef3420f789998d1a986467d9d7850b49e2ebf`.

WP7 adds the three records to the repository contract suite. WP8 still owns the
durable cross-file semantic validator, reason-checked negative mutations, final
redaction/reproducibility summary and the P2 advance/revise/stop decision.

## Tracker Mapping And Boundary

- **Active milestone:** advances the September interview-linked AI
  orchestration workstream without reopening SAP-C02 preparation.
- **Lakehouse case study:** makes evidence sufficiency, provenance, abstention
  and public/private separation demonstrable in the analyst flow.
- **SAP-C02 Domain 1:** least privilege, deny-by-default access, prompt-
  injection resistance and auditable policy outcomes.
- **SAP-C02 Domain 2:** bounded routes, deterministic contracts and explicit
  evidence dependencies.
- **SAP-C02 Domain 3:** freshness, conflict, incomplete-manifest and failure-
  isolation cases.
- **Interview:** provides reviewable System Architecture and GenAI Fluency
  evidence without claiming a model, production path or realized result.

No retrieval benchmark, model, embedding, vector store, managed service,
orchestration implementation, publication or AWS change occurred.

## Next Tracker-Ordered Work Package

Proceed to **P2 WP8 - Local Validators And Decision Record**. Materialize the
durable semantic, identity, hash, coordinate, evidence-resolution, split,
holdout-separation and redaction checks; add reason-checked known-bad evaluation
mutations; then record the P2 advance/revise/stop decision. Do not begin P3
retrieval comparisons before that decision and an explicit user instruction.
