# AWS Startups Solutions Architect Assignment Deck Blueprint

<!-- markdownlint-disable MD013 MD036 MD060 -->

**Prepared:** 2026-08-30<br>
**Assignment:** Explain a business problem solved with a technical solution<br>
**Format:** Five slides; 20-minute presentation; approximately 10 minutes of
interspersed questions and answers<br>
**Audience:** Mixed technical and non-technical leaders<br>
**Preparation constraint:** One to two hours<br>
**Primary case study:** Energy Market Data Lakehouse<br>
**AWS changes:** None authorized or required

## Communication Job

The presentation must make one decision easy to understand:

> A resource-constrained energy team needed trustworthy, decision-ready market
> intelligence from fragmented public data. I built the smallest credible
> serverless data and AI platform that made the evidence usable, protected the
> public boundary, and preserved a trigger-based route to a richer GenAI
> product.

This is a technical project with a business framing. Do not describe a real
customer, employer, adoption result, or financial result unless the learner can
truthfully supply that evidence. Where a business outcome was not measured,
say what the solution enabled and identify the metric that should be measured
next.

## Evidence And Language Boundary

| Evidence class | Permitted language | Examples in this deck |
|---|---|---|
| Verified current | I built, implemented, tested, or observed | Scheduled ingestion; private S3 zones; Glue and Athena; Step Functions; managed Bedrock/Mistral processing; schema validation; deterministic fallback; public-safe publication; monitoring and budget guardrails |
| Observable technical outcome | The system now provides or prevents | Repeatable refreshes; queryable curated data; invalid AI output blocked from publication; prior valid output retained on failure; bounded managed-AI spend |
| Unmeasured business effect | The solution was designed to enable; the next measure is | Analyst time-to-insight, decision confidence, adoption, avoided manual reconciliation, and cost per successful insight |
| Planned enhancement | I would propose; the next experiment would test; I would revisit when | Retrieval-grounded assistant, hybrid retrieval and reranking, tenant-aware access, online quality measurement, and approved tool use |

Never use **implemented**, **deployed**, **proven**, or a past-tense business
claim for the planned retrieval-grounded experience.

## Five-Slide Storyboard

### Slide 1 - From Fragmented Data To A Trusted Decision Surface

**Visible copy**

**Title:** From fragmented energy data to trusted decisions<br>
**Subtitle:** A low-cost Lakehouse with a safe path to GenAI<br>
**Footer:** Business problem, design decisions, results, and next experiment

**Visual**

Use a simple left-to-right transformation with three labels:

`Fragmented public data -> Governed evidence -> Decision-ready insight`

Do not show AWS service icons on this slide. The opening must make sense to a
founder or commercial leader before implementation details appear.

**Speaker notes - 2 minutes**

- Open with the user decision, not the repository: energy-market evidence
  arrived from sources with different formats, schedules, and failure modes.
- Explain why this is a business problem: slow reconciliation, uncertain
  freshness, and inconsistent evidence reduce decision confidence.
- State the constraint: one builder, limited budget, public data, and no case
  for an always-on enterprise platform.
- Preview the recommendation: establish a trusted data path first, then add AI
  only behind validation and cost boundaries.

**Likely question**

Why was this problem worth solving before there was a paying customer?

**Answer shape**

It de-risked the hardest technical assumptions cheaply: source integration,
data contracts, safe publication, and managed-model operation. Commercial
validation and adoption remain separate tests.

### Slide 2 - Work Backwards From The Decision And Constraints

**Visible copy**

**Business need**

- Reduce time spent reconciling fragmented market evidence.
- Make freshness and quality visible before someone acts.
- Preserve runway: pay for use and avoid premature platform operations.

**Technical needs**

- Repeatable ingestion and traceable raw evidence.
- Curated, queryable data with explicit contracts and failure paths.
- A public surface that cannot leak unsafe source content or invalid AI output.
- A design that can grow when usage, sensitivity, or tenancy justifies it.

**Success measures**

- Proven now: scheduled refresh, queryable curation, validation, fallback, and
  bounded managed-AI cost.
- Measure next: time to trusted answer, freshness-SLO attainment, adoption,
  decision usefulness, and cost per successful insight.

**Visual**

Use a cropped view of
`docs/evidence/screenshots/dashboard-phase10-overview-desktop-20260514.png` or
`docs/evidence/screenshots/dashboard-phase11-filters-desktop-20260516.png`.
Highlight freshness, data-quality, and insight areas; do not use the entire tall
screenshot at unreadable scale.

**Speaker notes - 4 minutes**

- Separate needs from features. The dashboard is evidence of a decision
  surface, not the business result itself.
- Name the working assumptions: public data, one account and owner, modest
  request volume, and no regulated or customer-confidential dataset.
- State what would invalidate them: sensitive data, distinct tenants,
  contractual isolation, strict residency, or high sustained throughput.
- Be explicit that analyst time saved and adoption were not measured. This
  protects accuracy and creates a credible measurement plan.

**Interspersed Q&A checkpoint - approximately 2 minutes**

Invite questions about the problem, user, assumptions, and success measures
before showing the architecture.

### Slide 3 - The Implemented Solution: Private Processing, Governed Publication

**Visible copy**

**Flow:** Collect -> Preserve -> Curate -> Analyse -> Validate -> Publish

**Three design principles**

1. Keep source evidence private and traceable.
2. Scale compute with work rather than idle capacity.
3. Treat public publication as a controlled trust boundary.

**Visual**

Use `diagrams/target_aws_service_architecture_icons.png` as the primary
architecture visual. Narrate the flow rather than reading every service name:

1. public market and news sources enter scheduled serverless ingestion;
2. S3 retains raw evidence and curated outputs;
3. Glue and Athena make structured evidence queryable;
4. Step Functions coordinates managed AI processing;
5. Bedrock/Mistral produces constrained output;
6. schema validation, sanitisation, and fallback protect publication; and
7. CloudFront presents the public decision surface.

**Speaker notes - 5 minutes**

- Walk one record end to end. Explain each technology in terms of its job.
- Point out that raw evidence remains available for replay and audit while
  curated outputs provide a stable analytical contract.
- Explain that the managed model does not publish directly. Its output must
  pass a schema and public-safety boundary; failure retains a deterministic or
  previously valid result.
- Close with the observable technical outcomes. Do not imply that service
  availability alone proves user adoption or business value.

**Likely technical probes**

- Where is idempotency enforced and how are duplicate source events handled?
- What happens when a source is late, malformed, or unavailable?
- How do schema failures surface, and what remains visible to the user?
- Which IAM principal can invoke the model or publish public data?
- What are the current recovery and freshness targets?

### Slide 4 - Why This Was The Best Fit, And When I Would Change It

**Visible copy**

| Decision | Choice and reason now | Cost or limitation | Revisit trigger |
|---|---|---|---|
| Compute | Serverless managed services: low operations and pay for work | Cold starts, quotas, and less runtime control | Sustained predictable load or specialised runtime needs |
| Data platform | S3 Lakehouse plus Athena: cheap retention and query on demand | Not optimised for high-concurrency low-latency serving | Product traffic requires consistently fast interactive queries |
| Isolation | One private bucket with raw and curated prefixes: simple policy and lower overhead | Shared bucket-level controls and blast radius | New owner, tenant, sensitivity, compliance, or recovery boundary |
| Encryption | SSE-S3 for public source data: adequate baseline without key-management overhead | No customer-managed key boundary | Contractual, regulatory, cross-account, or sensitive-data requirement |
| AI | Managed model behind contracts and fallback: faster learning with limited operations | Provider dependency, variable output, latency, and token cost | Quality, control, portability, or unit economics justify another model or hosting pattern |

**Speaker notes - 4 minutes**

- Use the table to show judgment, not to defend one permanent architecture.
- Explain the strongest rejected alternative for each important decision.
- The winning design was the one that maximised learning per pound and hour
  while preserving trust boundaries, not the one with the most services.
- Acknowledge limitations: single-account concentration, incomplete business
  measurement, source dependencies, query latency, and generative uncertainty.

**Interspersed Q&A checkpoint - approximately 4 minutes**

Invite challenge on alternatives, risk, cost, security, reliability, and scale.
Use every answer pattern: requirement -> decision -> alternative -> trade-off
-> revisit trigger.

**Repository evidence**

- `docs/adr/0001-shared-s3-data-bucket.md`
- `docs/adr/0002-encryption-and-kms-design.md`
- `docs/phase-17-managed-ai-refresh-preflight.md`

### Slide 5 - Next Experiment: Grounded GenAI, Not Unbounded Automation

**Visible copy**

**Question:** Can an evidence-grounded assistant shorten time to a trusted
answer while preserving accuracy, safety, and unit economics?

**Proposed read-only flow**

`Approved curated facts and documents`
`-> input, access, and intent contract`
`-> controlled route: structured lookup, document retrieval, or both`
`-> evidence assembly with freshness, provenance, and citations`
`-> prompt with citations`
`-> model`
`-> schema, safety, and grounding checks`
`-> cited answer, deterministic fallback, or explicit abstention`

**Promotion gates**

| Gate | Measures before expansion |
|---|---|
| Usefulness | Task success, user rating, and time to trusted answer versus a non-GenAI search baseline |
| Retrieval and grounding | Recall at K, citation correctness, groundedness, freshness, and unsupported-claim rate |
| Safety and governance | Prompt-injection tests, data leakage, unsafe-output rate, least-privilege access, tenant filtering, and human escalation |
| Operations and cost | End-to-end latency, error and throttle rate, token use, cache hit rate, and cost per successful answer |

**Architecture decisions**

- Use parameterized structured lookup or curated facts for exact metrics; use
  retrieval for news and explanatory documents. Document-only RAG is not an
  authority for time-series calculations.
- Keep S3 curated contracts authoritative and make any retrieval index a
  rebuildable projection with version, freshness, classification, and source
  metadata.
- Separate asynchronous corpus preparation from the latency-bounded read-only
  answer path. Preserve the proven scheduled dashboard-insight workflow.
- Start with retrieval rather than fine-tuning because changing evidence needs
  provenance, refresh, and deletion without retraining.
- Fine-tune only if prompt and RAG evaluation expose a stable task, style, or
  behaviour gap that training data can improve.
- Select the model with a representative evaluation set across quality,
  latency, context, data policy, availability, and cost; do not select on a
  leaderboard alone.
- Use token budgets, caching, quotas, model fallback, and graceful degradation
  to protect startup unit economics.
- Keep the first release read-only and explicitly routed. Defer autonomous or
  write-capable agents; allow only bounded structured queries after their
  parameter, IAM, and evaluation contracts are accepted.

**Speaker notes - 5 minutes**

- Label this slide **planned** immediately. It is a testable product evolution,
  not a claim about the implemented platform.
- Explain why retrieval alone does not solve hallucination: source quality,
  retrieval quality, instructions, model behaviour, and output validation all
  need evaluation.
- Describe the threat model: malicious source text, prompt injection, stale or
  cross-tenant retrieval, sensitive-data exposure, unsupported claims, and
  excessive agency.
- Close with a business recommendation: fund the smallest read-only experiment
  and promote it only if it beats the non-GenAI baseline on usefulness, trust,
  latency, and cost.

**Interspersed Q&A checkpoint - approximately 4 minutes**

Invite questions on RAG versus fine-tuning, model selection, evaluation,
security, responsible AI, operations, and cost.

## Timing And Facilitation

| Segment | Presentation | Interspersed Q&A |
|---|---:|---:|
| Slide 1 | 2 minutes | - |
| Slide 2 | 4 minutes | 2 minutes |
| Slide 3 | 5 minutes | - |
| Slide 4 | 4 minutes | 4 minutes |
| Slide 5 | 5 minutes | 4 minutes |
| **Total** | **20 minutes** | **10 minutes** |

If the interviewers ask questions earlier, answer directly and compress later
detail. Protect the recommendation and trade-off slide; those are the clearest
evidence of Solutions Architect judgment.

## Visual System

- Use a restrained monochrome layout with one AWS-blue accent.
- Use one message per slide and no more than one primary visual.
- Prefer plain-language labels above service names.
- Use the dashboard crop only as evidence of the decision surface.
- Use the existing architecture diagram on Slide 3; do not repeat it on Slide
  5. The planned GenAI flow should be a simpler conceptual pipeline.
- Keep body text readable from a meeting-room screen. Move detail into speaker
  notes and use progressive verbal explanation.
- Add a small **Current** or **Planned** label to technical visuals so the
  evidence boundary is visible even if a slide is viewed alone.

## High-Probability Questions To Rehearse

1. Who exactly was the customer or stakeholder, and what decision changed?
2. What measurable business impact did the solution produce?
3. Why use a Lakehouse rather than a database, warehouse, or managed data
   platform?
4. Why AWS and serverless, and what would make containers or an always-on
   service preferable?
5. How does the system recover from missing, duplicate, late, or corrupt data?
6. Why was SSE-S3 sufficient, and when would SSE-KMS become necessary?
7. How do you prevent invalid or unsafe model output reaching the public?
8. Why use RAG before fine-tuning?
9. How would you evaluate model and retrieval quality before launch?
10. How would the architecture change for confidential customer data,
    multiple tenants, or rapid growth?
11. What was the hardest trade-off and what would you do differently?
12. What is the largest current limitation?

## Facts Required Before Final Slides

The learner must supply these truthfully before the narrative is locked:

1. the actual customer, stakeholder, or user context;
2. the decision or workflow that motivated the build;
3. the previous manual or technical process and its observable pain;
4. at least one measured business result, or explicit confirmation that only
   technical outcomes can be claimed;
5. the learner's individual actions and hardest decision;
6. the definitive interview date and submission mechanics; and
7. whether confidential assignment material may be stored in this repository.

## One-To-Two-Hour Preparation Budget

| Activity | Maximum time |
|---|---:|
| Confirm truthful customer context, outcome, and evidence boundary | 15 minutes |
| Assemble five slides from this blueprint and existing visuals | 35 minutes |
| Add speaker notes and simplify language | 20 minutes |
| Timed full run with questions | 20 minutes |
| Correct inaccuracies and trim overflow | 10 minutes |
| **Total** | **100 minutes** |

## Exit Check

The deck is ready only when:

- the business problem appears before the architecture;
- each major technical choice has a benefit, limitation, alternative, and
  revisit trigger;
- all current and planned claims are visibly separated;
- any business impact is measured or explicitly described as unmeasured;
- the GenAI proposal includes a non-GenAI baseline, RAG design, evaluation,
  responsible-AI controls, operations, cost, and human boundaries;
- the talk reaches the recommendation in 20 minutes even with interruption;
  and
- the learner can answer the high-probability questions without reading the
  slide.
