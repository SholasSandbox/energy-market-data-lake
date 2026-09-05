# AI Orchestration P2 WP8 Validation And Decision

<!-- markdownlint-disable MD013 MD060 -->

**Completed:** 2026-09-05T21:44:57Z<br>
**Status:** Complete<br>
**Decision:** Advance to P3 only after an explicit continuation instruction<br>
**AWS changes:** None authorized or performed

## Decision

**Advance.** P2's corpus, evidence, exclusion, manifest and evaluation
boundaries pass the final local schema and semantic gates. The repository may
proceed to a bounded P3 local retrieval benchmark when explicitly requested.
This decision does not start P3, select a retrieval engine, embedding model,
generation model, managed service or AWS topology, or claim that the proposed
analyst path has been implemented.

The decision is constrained rather than unconditional:

- `SF-08`, `DOC-01` through `DOC-08` and `DP-01` through `DP-08` are the exact
  locally active evidence identities;
- `SF-01` through `SF-07` remain contract-blocked because the frozen WP1 pack
  does not retain the 48 exact scalar operands for each aggregate;
- `ST-01` through `ST-03` and the structured components of `CO-01` through
  `CO-04` therefore remain abstention or qualified-partial evaluation cases;
- the sixteen policy fixtures remain synthetic, adversarial-only and
  ineligible for ordinary answers, retrieval or tuning; and
- the fourteen holdout gold records remain excluded from candidate prompts,
  retrieval and tuning inputs until the candidate configuration is frozen.

These constraints are expected P2 outcomes, not validation exceptions. The
28-case contract explicitly tests supported answers, missing evidence,
staleness, conflict, unsafe requests and abstention. P2 would require revision
if blocked aggregates were silently admitted, fixtures became answer evidence,
or holdout labels entered candidate inputs; the validator rejects each of
those failure modes.

## Delivered Validation

`scripts/validate_ai_orchestration_p2.py` is the durable WP8 semantic
validator. It complements, rather than replaces, the repository's JSON Schema
suite in `scripts/validate_contracts.py`.

| Validation area | Result | Durable check |
|---|---|---|
| Schema acceptance | Pass | All seven P2 schemas accept their versioned records/examples |
| Known-bad schema rejection | Pass | WP4-WP6 invalid examples fail for the required diagnostic fragment |
| Canonical hashes | Pass | Manifest, exclusion register, evaluation set, policy fixture set and holdout set hashes recompute exactly |
| Manifest resolution | Pass | Every active manifest pointer resolves once to the frozen WP1 pack and matches ID, hash, publisher, route, classification and access scope |
| Evidence hashing | Pass | `SF-08` source hash, eight bounded-document hashes and eight exact passage hashes reconcile |
| Identity and counts | Pass | Structured, document, passage, chunk, exclusion, query-template, case, fixture and gold identities are unique and counts match their arrays |
| Blocked boundary | Pass | `SF-01` through `SF-07` occur in the contract-blocked exclusion and never in active or approved answer evidence |
| Time and freshness | Pass | Manifest and example timestamp ordering, `SF-08` age, historical publication ordering and 36/168-hour stale fixtures recompute from pinned times |
| Citation and coordinates | Pass | The strict examples reconcile internal provenance, public citation fields, deterministic labels, hashes and the selected section coordinate |
| Evaluation reconciliation | Pass | Exactly 28 ordered cases, four per family, use the exact 7 calibration / 7 development / 14 holdout split |
| Evidence outcomes | Pass | Candidate scopes equal their gold evidence dependencies; active, blocked and fixture references resolve to the intended class |
| Holdout isolation | Pass | Fourteen holdout cases have no inline gold and resolve one-to-one to the separate candidate-ineligible gold set |
| Adversarial isolation | Pass | Sixteen fixtures resolve one-to-one to policy cases and remain synthetic and ineligible for answers, retrieval and tuning |
| Local references and redaction | Pass | Repository locators and Markdown links resolve; P2 files contain no detected local home path, email, AWS key or private-key material |
| P1 stability | Pass | The completed P1 evaluation contract remains pinned at SHA-256 `85972e3edc6a33deb0d13c79248264ef5f28e3ea2561e8737cd56dedf5bae62c` |

## Reason-Checked Semantic Mutations

WP8 adds compact mutations under `evaluation/ai-orchestration/p2/invalid/`.
Each starts from one valid checked-in record, changes one bounded condition and
must produce the named semantic error code.

| Mutation | Required rejection |
|---|---|
| Duplicate evaluation case ID | `EVAL_CASE_ID_UNIQUE` |
| Drifted global split | `EVAL_SPLIT_COUNTS` |
| Contract-blocked fact admitted as approved evidence | `CASE_APPROVED_EVIDENCE_RESOLUTION` |
| Evaluation manifest hash reference drift | `EVALUATION_REFERENCE_HASH` |
| Duplicate policy-fixture case mapping | `FIXTURE_CASE_MAPPING` |
| Answer-eligible adversarial fixture | `FIXTURE_INELIGIBLE` |
| Holdout gold mapped to the wrong case | `HOLDOUT_CASE_MAPPING` |
| Manifest content changed without a new hash | `MANIFEST_HASH` |
| Document section coordinate drift | `DOCUMENT_COORDINATES` |

All nine mutations are rejected for their required reason. A mutation rejected
only because some unrelated invariant also fails does not pass the WP8 test.

## P2 Exit-Criteria Assessment

| Exit criterion | Result | Decision evidence |
|---|---|---|
| Corpus stays within the 8-plus-8 boundary and maps to P1 | Pass | Eight selected structured facts and eight selected passages; every item names a P1 case |
| Exclusions prevent broad or unsafe ingestion | Pass | Seven reason-coded exclusion entries; all remain answer-ineligible |
| Evidence and manifest schemas are strict and validated | Pass | Draft 2020-12 schemas and reason-checked valid/invalid records pass |
| Public citation is separate from internal provenance | Pass | Closed schema projections plus semantic equality and leakage checks pass |
| Freshness, conflict, missing and incomplete behaviour is deterministic | Pass | Pinned `as_of`, threshold, conflict and manifest-fallback cases reconcile |
| The 28-case 7/7/14 evaluation set reconciles | Pass | Exact ID, family, split, gold and fixture counts pass |
| Positive and negative evidence outcomes resolve correctly | Pass | Active, blocked and adversarial references match candidate scope and gold |
| Redaction, Markdown, JSON, reference and diff checks pass | Pass | Final local command suite passes |
| No technology or deployment choice is made | Pass | No retrieval, model, managed-service, Terraform, network or AWS action occurred |
| Final disposition is explicit | Pass | Advance to a bounded P3 local retrieval benchmark only after explicit continuation |

No P1 contradiction was found, so P1 was not revised. No unsuitable required
fixture, corpus-size breach, citation blocker, nondeterministic freshness rule
or dependency on live AWS/model access was found, so the stop rules were not
triggered.

## Validation Evidence

The final local validation suite passed:

```text
.venv/bin/python scripts/validate_contracts.py --check-failures
.venv/bin/python scripts/validate_ai_orchestration_p2.py
.venv/bin/python -m compileall -q scripts/validate_contracts.py scripts/validate_ai_orchestration_p2.py
JSON parsing for the P2 schemas, examples, manifests, fixtures and mutations
scripts/check_public_evidence_redaction.sh
npx --yes markdownlint-cli2 with literal P1/P2 Markdown paths
git diff --check scoped to the P2 package
git status --short --branch
```

The semantic validator reported all nine known-bad mutations rejected for their
required codes and then reported the positive P2 boundary coherent. Validation
was local-only. It made no network request, model call, Athena query, Terraform
operation or AWS API call.

## Tracker And Interview Mapping

- **Active milestone:** completes P2 of the interview-linked AI orchestration
  workstream without reopening SAP-C02 preparation.
- **Lakehouse case study:** proves bounded evidence authority, replay identity,
  provenance, public/private separation and deterministic failure handling.
- **SAP-C02 Domain 1:** demonstrates least privilege, deny-by-default access,
  safe citation, holdout isolation and adversarial ineligibility.
- **SAP-C02 Domain 2:** demonstrates evidence-led sequencing and prevents
  premature retrieval, model, managed-service or deployment choices.
- **SAP-C02 Domain 3:** demonstrates hash integrity, freshness arithmetic,
  versioning, validation, fallback and regression controls.
- **Interview:** provides reviewable System Architecture and GenAI Fluency
  evidence without overstating implementation, production authority or a
  realized business result.

## Next Tracker-Ordered State

P2 is complete and the P2-to-P3 decision gate has advanced. P3 has **not**
started. The next bounded item, after explicit continuation, is a local
retrieval benchmark comparing deterministic structured lookup, lexical
document retrieval and only the minimum additional candidate justified by the
28-case contract. The benchmark must preserve the blocked structured-evidence,
holdout and adversarial boundaries established here.
