# AI Orchestration Architecture Decision Register

<!-- markdownlint-disable MD013 MD060 -->

**Prepared:** 2026-08-30<br>
**Controlling decisions:**
`docs/adr/0006-read-only-evidence-grounded-ai-orchestration.md` and
`docs/adr/0007-bedrock-runtime-and-orchestration-framework-boundary.md`<br>
**Purpose:** Sequence the smallest evidence-led decisions that strengthen the
Lakehouse assignment and GenAI interview depth without starting an open-ended
platform build<br>
**AWS changes:** None authorized

## Decision Principle

Architecture precedes implementation. Each implementation candidate must trace
to a named user decision, an accepted architecture decision, and a measurable
evidence gap. Stop when the interview-relevant gap is closed.

## Accepted Architecture Decisions

| ID | Decision | Status | Why it is proportionate |
|---|---|---|---|
| AD-01 | Build for a named read-only decision-support use case, not a general AI platform | Accepted | Gives the work a business outcome and limits the operating and evaluation surface. |
| AD-02 | Combine controlled structured evidence with document retrieval | Accepted | Exact market values belong in deterministic queries or curated facts; documents and news benefit from retrieval. |
| AD-03 | Keep curated S3 artifacts and schemas authoritative; treat indexes as rebuildable projections | Accepted | Preserves provenance, replay, deletion, and existing Lakehouse contracts. |
| AD-04 | Separate asynchronous corpus/index preparation from the latency-bounded answer path | Accepted | The two planes have different retry, latency, cost, and scaling requirements. |
| AD-05 | Use explicit routing to read-only paths; do not introduce autonomous agents | Accepted | The current problem does not justify broad tool permissions or write-capable agency. |
| AD-06 | Require citations, grounding checks, schema validation, fallback, and abstention | Accepted | A fluent unsupported answer is not a safe decision-support outcome. |
| AD-07 | Evaluate against a non-GenAI baseline before selecting models or managed retrieval services | Accepted | Prevents service-first design and measures whether GenAI adds enough value to justify cost and risk. |
| AD-08 | Preserve the proven scheduled dashboard-insight workflow | Accepted | Avoids regression and keeps verified current evidence separate from the proposed query experience. |
| AD-09 | Use Bedrock for managed inference and Step Functions/Lambda for the current workflow | Accepted and verified | Reuses the existing adapter, contracts, IAM, failure handling, observability, schedule, publication, and budget controls. |
| AD-10 | Remove OpenClaw/ECS from the target | Rejected option | The use case does not justify a self-hosted general agent runtime and its additional container, security, availability, and operating surface. |
| AD-11 | Keep LangGraph conditional on a proven graph-shaped workflow | Deferred | Bedrock and LangGraph solve different layers; the current acyclic explicit workflow does not need another orchestrator. |
| AD-12 | Never publish raw model text directly | Rejected option | Model output remains untrusted candidate data until validation and public/private gates pass. |

## Pending Decisions In Dependency Order

| Order | Decision | Evidence required before deciding | Candidate outcomes |
|---:|---|---|---|
| 1 | User decision and evaluation contract | Truthful stakeholder context, question categories, expected evidence, answer constraints, unanswerable cases, and baseline timing | Approve a bounded evaluation set or stop because the use case is not specific enough |
| 2 | Corpus and metadata contract | Approved curated prefixes/documents, exclusion list, versioning, freshness, classification, and citation fields | One small corpus contract; no raw or unrestricted repository ingestion |
| 3 | Structured evidence contract | Representative exact questions and safe parameter boundaries | Precomputed curated facts, allowlisted query templates, or both |
| 4 | Chunking and retrieval strategy | Retrieval results across document types, chunk sizes, metadata filters, lexical/vector combinations, and optional reranking | Select the simplest strategy that meets retrieval gates |
| 5 | Model and prompt contract | Representative grounded answers measured across quality, latency, context, data policy, Region, quota, and cost | Retain the current Bedrock model, select another Bedrock candidate, approve an evaluated external candidate through a new governance review, or use deterministic output for a category |
| 6 | Local orchestration shape | Traceable local run, route decision, evidence references, validated answer, abstention, and failure result | Accept a minimal vertical slice or revise the architecture |
| 7 | AWS service topology | Official service capabilities, regional availability, IAM, networking, observability, quota, cost, rollback, and Terraform delta | Managed knowledge base, managed search/vector service, relational vector extension, optional LangGraph or managed-agent runtime if graph needs are proven, or no AWS deployment |
| 8 | Deployment decision | Local gates passed, interview value remains, exact cost and blast radius bounded, and explicit user authorization | Approve one controlled slice or stop at design/local evidence |

Technology names are intentionally absent from Decisions 1-6. They are
implementation candidates, not requirements. ADR 0007 establishes Bedrock as
the default AWS inference boundary; it does not preselect the model, prompt,
embedding, reranking, retrieval, or optional agent-framework choice.

## Priority Order

### P0 - Architecture Package - Complete

- ADR 0006 records the accepted pattern, alternatives, consequences, safety
  boundaries, promotion gates, and revisit triggers.
- ADR 0007 selects the runtime layers: Bedrock for managed inference,
  Step Functions/Lambda for the current workflow, OpenClaw/ECS rejected, and
  LangGraph deferred behind a stateful-workflow evidence gate.
- `diagrams/ai-orchestration-evidence-grounded-target.mmd` records the current,
  proposed, and explicitly deferred boundaries.
- The assignment blueprint uses structured facts plus document retrieval rather
  than treating all Lakehouse data as document RAG.

### P1 - Evaluation Contract - Next

Produce one small evaluation specification before writing orchestration code:

- identify the real user decision and non-GenAI baseline;
- define representative structured, document, combined, stale, conflicting,
  unsafe, and unanswerable questions;
- define expected evidence and acceptable answer/abstention outcomes;
- define measures for retrieval, task success, groundedness, citations, safety,
  latency, and cost; and
- define a stop threshold if GenAI does not improve the baseline.

This is the highest-value next artifact because every technology decision
depends on it.

### P2 - Corpus And Evidence Contracts

Define the smallest approved corpus and two adapter contracts:

1. structured facts with stable metric, time, unit, source, and query
   provenance; and
2. documents with stable ID, version/hash, source, timestamps, classification,
   access scope, and chunk coordinates.

Do not ingest all Lakehouse or repository content.

### P3 - Local Retrieval Benchmark

Compare the minimum viable candidates locally:

- deterministic structured lookup or allowlisted query simulation;
- lexical document retrieval baseline;
- vector retrieval candidate; and
- hybrid retrieval or reranking only if the simpler candidate misses the gate.

Record quality, latency, reproducibility, and cost assumptions. Do not add a
managed service solely to make the architecture look more sophisticated.

### P4 - Local Read-Only Vertical Slice

Only if P1-P3 justify it, implement one end-to-end local path:

```text
question
  -> policy and route contract
  -> approved structured/document evidence
  -> provider-neutral answer adapter
  -> answer schema, citations, grounding, and safety checks
  -> cited answer or explicit abstention
```

The slice must include at least one success, one unanswerable case, one stale or
conflicting case, and one prompt-injection case. It must not publish, deploy,
send messages, or modify data.

### P5 - AWS Service Decision - Conditional

Evaluate current official AWS options only after the local benchmark provides
requirements. Compare the smallest credible service patterns across:

- regional availability and model support;
- retrieval quality and metadata filtering;
- ingestion and freshness control;
- IAM, encryption, private networking, and data-policy boundaries;
- observability and evaluation support;
- idle and per-request cost;
- quotas, latency, operational ownership, and portability; and
- rollback and index rebuild.

This stage may conclude that design and local evidence are sufficient for the
interview and that no AWS deployment is responsible.

### P6 - Deployment Preflight - Not Authorized

Prepare an exact code, IAM, Terraform, cost, rollback, and validation delta only
if the user separately requests AWS implementation. Do not infer deployment
authorization from this register.

## Explicit Deferrals

- polished chat UI;
- general-purpose conversational platform;
- autonomous multi-agent orchestration;
- write-capable tools or external actions;
- unrestricted natural-language-to-SQL;
- production multi-tenancy;
- fine-tuning;
- custom model hosting;
- multi-Region GenAI serving; and
- replacing the current scheduled workflow.

Runtime disposition is explicit: Bedrock `InvokeModel` is verified current,
not deferred; OpenClaw/ECS and raw model publication are rejected, not
deferred. LangGraph, direct Step Functions-to-Bedrock optimization, managed
agents, multi-agent orchestration, and fine-tuning remain deferred behind the
evidence gates in ADR 0007.

## Stop Rules

Stop the workstream when any of these is true:

- the user decision or expected business value cannot be stated truthfully;
- the non-GenAI baseline meets the need with less cost and risk;
- retrieval cannot meet evidence and freshness gates on the small corpus;
- the local slice does not materially strengthen the interview narrative;
- the next step would be platform work rather than closing a named evidence
  gap;
- the time required would displace STAR preparation or presentation rehearsal;
  or
- AWS mutation would be required without a separate explicit authorization.

## Interview Mapping

| Interview dimension | Evidence from this decision-first approach |
|---|---|
| Business Acumen | Starts with the decision and baseline; treats cost per successful answer as a business metric. |
| Invent and Simplify | Uses two bounded evidence paths and delays infrastructure selection until needed. |
| System Architecture | Separates authoritative data, derived indexes, asynchronous preparation, synchronous answers, trust boundaries, and failure modes. |
| Ownership | Makes status, limitations, evidence, and stop conditions explicit. |
| Technical Communication | Provides an executive decision, a technical flow, alternatives, and revisit triggers. |
| Customer Obsession | Prefers a reliable abstention or deterministic answer over an impressive but unsupported response. |
| Deliver Results | Defines small gates and a shippable local slice instead of a broad platform backlog. |
| GenAI Fluency | Covers structured grounding, RAG, routing, model selection, evaluation, responsible AI, operations, and cost. |
| Think Big | Preserves an evolution path without funding future complexity before evidence exists. |

## Next Tracker-Ordered Artifact

Create the P1 evaluation contract. Do not select a vector database, managed
knowledge-base service, embedding model, or deployment topology first.
