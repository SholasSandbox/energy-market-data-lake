# P2 Evaluation Case Coverage Report v1

<!-- markdownlint-disable MD013 MD060 -->

**Prepared:** 2026-09-05<br>
**Evaluation set:** `ai-p2-evaluation-set-v1`<br>
**Candidate-visible:** Yes; this report contains no holdout gold assertions<br>
**AWS changes:** None

## Reconciled Counts

| Dimension | Required | Instantiated |
|---|---:|---:|
| Total cases | 28 | 28 |
| Case families | 7 | 7 |
| Cases per family | 4 | 4 |
| Calibration | 7 | 7 |
| Development | 7 | 7 |
| Holdout | 14 | 14 |
| Synthetic policy fixtures | 16 | 16 |
| Separate holdout gold records | 14 | 14 |

Every family uses case `01` for calibration, `02` for development and `03` and
`04` for holdout. Holdout rows below identify only the opaque gold pointer; the
expected route, outcomes, mandatory facts and policy assertions remain in the
separate, candidate-ineligible holdout file.

## Case-To-Input Coverage

| Case | Split | Required shape | Candidate input | Resolution | Gold location |
|---|---|---|---|---|---|
| `ST-01` | Calibration | Exact value | Blocked reference `SF-05` | Contract-blocked | Inline `IG-ST-01` |
| `ST-02` | Development | Derived comparison | Blocked references `SF-05`, `SF-06` | Contract-blocked | Inline `IG-ST-02` |
| `ST-03` | Holdout | Time/unit discrimination | Blocked reference `SF-07` | Contract-blocked | Separate `HG-ST-03` |
| `ST-04` | Holdout | Present record, missing metric | Active `SF-08` | Ready record; requested metric absent | Separate `HG-ST-04` |
| `DO-01` | Calibration | Known passage | Active `DOC-05`, `DP-05` | Ready | Inline `IG-DO-01` |
| `DO-02` | Development | Metadata/provenance | Active `DOC-06`, `DP-06` | Ready | Inline `IG-DO-02` |
| `DO-03` | Holdout | Paraphrased lookup | Active `DOC-07`, `DP-07` | Ready | Separate `HG-DO-03` |
| `DO-04` | Holdout | Association versus causation | Active `DOC-08`, `DP-08` | Ready | Separate `HG-DO-04` |
| `CO-01` | Calibration | Exact fact plus context | Active `DOC-01`, `DP-01`; blocked `SF-01` | Partial coverage | Inline `IG-CO-01` |
| `CO-02` | Development | Calculated fact plus context | Active `DOC-02`, `DP-02`; blocked `SF-01`, `SF-02` | Partial coverage | Inline `IG-CO-02` |
| `CO-03` | Holdout | Timestamp alignment | Active `DOC-03`, `DP-03`; blocked `SF-03` | Partial coverage | Separate `HG-CO-03` |
| `CO-04` | Holdout | Qualified synthesis | Active `DOC-04`, `DP-04`; blocked `SF-04` | Partial coverage | Separate `HG-CO-04` |
| `SA-01` | Calibration | Stale structured | `FIX-SA-01` | Synthetic policy fixture only | Inline `IG-SA-01` |
| `SA-02` | Development | Stale document | `FIX-SA-02` | Synthetic policy fixture only | Inline `IG-SA-02` |
| `SA-03` | Holdout | Fresh fact, stale context | `FIX-SA-03` | Synthetic policy fixture only | Separate `HG-SA-03` |
| `SA-04` | Holdout | Incomplete manifest | `FIX-SA-04` | Synthetic policy fixture only | Separate `HG-SA-04` |
| `CF-01` | Calibration | Conflicting values | `FIX-CF-01` | Synthetic policy fixture only | Inline `IG-CF-01` |
| `CF-02` | Development | Conflicting units/windows | `FIX-CF-02` | Synthetic policy fixture only | Inline `IG-CF-02` |
| `CF-03` | Holdout | Document disagreement | `FIX-CF-03` | Synthetic policy fixture only | Separate `HG-CF-03` |
| `CF-04` | Holdout | Structured/document tension | `FIX-CF-04` | Synthetic policy fixture only | Separate `HG-CF-04` |
| `UN-01` | Calibration | Direct prompt injection | `FIX-UN-01` | Synthetic policy fixture only | Inline `IG-UN-01` |
| `UN-02` | Development | Indirect retrieved instruction | `FIX-UN-02` | Synthetic policy fixture only | Inline `IG-UN-02` |
| `UN-03` | Holdout | Private/failed locator request | `FIX-UN-03` | Synthetic policy fixture only | Separate `HG-UN-03` |
| `UN-04` | Holdout | Unrestricted query/publication/action | `FIX-UN-04` | Synthetic policy fixture only | Separate `HG-UN-04` |
| `NA-01` | Calibration | Unsupported date/region | `FIX-NA-01` | Synthetic policy fixture only | Inline `IG-NA-01` |
| `NA-02` | Development | Unsupported metric | `FIX-NA-02` | Synthetic policy fixture only | Inline `IG-NA-02` |
| `NA-03` | Holdout | Ambiguous scope | `FIX-NA-03` | Synthetic policy fixture only | Separate `HG-NA-03` |
| `NA-04` | Holdout | Plausible, no approved evidence | `FIX-NA-04` | Synthetic policy fixture only | Separate `HG-NA-04` |

## Evidence Boundary

The active WP6 manifest supplies `SF-08`, `DOC-01` through `DOC-08` and
`DP-01` through `DP-08`. `SF-01` through `SF-07` remain contract-blocked; they
are identifiers explaining an expected absence, not candidate answer content.
WP7 therefore instantiates all required cases without making the seven blocked
facts or their affected cases answer-ready.

`FIX-SA-*`, `FIX-CF-*`, `FIX-UN-*` and `FIX-NA-*` are visibly synthetic,
`adversarial_only`, non-production and ineligible for answers, ordinary
retrieval or candidate tuning. They carry input conditions only and disclose no
expected outcome or holdout assertion.

The WP6 manifest and exclusion register are immutable activation snapshots.
Their `WP7_not_instantiated` and `not_instantiated` fields describe the state at
WP6 activation; WP7 records the subsequent state in a new hashed evaluation set
and fixture register instead of rewriting those snapshots in place.

## Hashes And Locations

| Artifact | Canonical SHA-256 | Location |
|---|---|---|
| Evaluation set | `25a7c2e01806205b1dc30b7d6f9f2580d897685ea5a89df52b5c9bfee7796c18` | `evaluation/ai-orchestration/p2/evaluation-set-v1.json` |
| Policy fixtures | `9a106587ebca205db5a54c2c3556bf61765d8e226254725899d58789d7cc52c6` | `evaluation/ai-orchestration/p2/policy-fixtures-v1.json` |
| Holdout gold | `c70cc969a32ea5989e8cf78c8c5ef3420f789998d1a986467d9d7850b49e2ebf` | `evaluation/ai-orchestration/p2/holdout/holdout-gold-v1.json` |

Each hash is computed after removing only its own top-level hash field and
serializing the remaining JSON as UTF-8 with recursively sorted keys, compact
separators and no trailing newline.

## Boundary For WP8

This report proves the instantiated count and mapping. WP8 still owns the
durable local semantic validator, known-bad evaluation mutations, final
cross-file resolution checks, validation summary and the P2 advance/revise/stop
decision. No retrieval benchmark, embedding, model, managed service or runtime
selection may start before that decision.
