# ADR 0007: Bedrock Runtime And Orchestration Framework Boundary

<!-- markdownlint-disable MD013 MD060 -->

- Status: Accepted for the current and target architecture; no AWS change is authorized
- Date: 2026-09-01
- Decision owner: Energy Data Lakehouse repository owner
- Supersedes: OpenClaw runtime options in current target documents
- Complements: `docs/adr/0006-read-only-evidence-grounded-ai-orchestration.md`
- Evidence baseline: `docs/evidence/phase17au-managed-workflow-scheduled-observation-20260612.md`
- Interview use: Supporting GenAI STAR scenario, not the presentation assignment

## Executive Decision

Use **Amazon Bedrock as the managed model-inference boundary** and retain
**AWS Step Functions plus AWS Lambda as the current workflow-orchestration
boundary**.

Do not add LangGraph to the current implementation. Keep it as a conditional
future application-orchestration option only if evaluation proves a need for
cyclic, stateful, long-running, or human-in-the-loop agent behaviour that the
current explicit workflow cannot express economically.

Remove **OpenClaw on Amazon ECS/Fargate** from the target architecture. It is a
rejected choice for this repository, not a deferred implementation. The
reason is not that OpenClaw or containers are inherently non-enterprise. The
reason is that this use case does not require a self-hosted general agent
runtime, so operating one would add security, availability, scaling, patching,
networking, observability, supply-chain, and cost responsibilities without a
measured customer benefit.

Continue to reject direct publication of raw model text. All model output must
pass normalization, schema, provenance, grounding, safety, and public/private
boundary checks before it can become an approved dashboard artifact.

## Status Correction

The older Phase 8 list described five items as deferred. Their current
disposition is:

| Earlier item | Current disposition | Reason |
|---|---|---|
| Bedrock `InvokeModel` | Verified current capability | The Bedrock adapter, conditional IAM permission, managed Lambda action, scheduled Step Functions runs, validated artifacts, public-safe publication, failure controls, and budget guardrail have evidence through Phase 17AU. |
| OpenClaw on ECS/Fargate or another managed runtime | Rejected and removed from target | No requirement justifies a self-hosted general agent gateway or container operating surface. |
| Multi-agent orchestration | Deferred | No validated workflow requires several autonomous roles, dynamic delegation, or agent-to-agent coordination. |
| Fine-tuning | Deferred for stable behaviour gaps; rejected as a knowledge store | Changing market evidence needs retrieval and provenance. Fine-tuning is reconsidered only after prompt and retrieval evaluation isolates a repeatable behaviour gap. |
| Publishing raw model text directly to the dashboard | Rejected | It bypasses the trust boundary and cannot be made the normal publication path merely by waiting for a later phase. |

This correction distinguishes historical sequencing from current
architecture truth. Bedrock was responsibly deferred during Phase 8, then
implemented and verified in later controlled phases. A historical document
may retain its original decision if it also points to this current
disposition.

## Context

The repository already has a bounded managed-AI batch workflow:

```text
EventBridge schedule
  -> Step Functions workflow
  -> Lambda actions build versioned S3 evidence contracts
  -> Lambda Bedrock adapter calls InvokeModel
  -> provider response normalization
  -> ai_insight_v1 validation and sanitization
  -> approved dashboard snapshot publication
  -> audit or failed-path evidence, CloudWatch, SNS, and budget controls
```

Phase 17AU records two successful scheduled runs after the budget guardrail was
applied. Each wrote the expected run artifacts, no new failed artifact was
found, the immutable public path validated, and Terraform reported no change.
That is evidence for the managed batch baseline. It is not evidence that the
repository already implements RAG, LangGraph, multi-agent orchestration,
fine-tuning, a production conversational API, or comprehensive GenAI
evaluation.

ADR 0006 separately accepts a proposed read-only, evidence-grounded
question-answering pattern. It deliberately leaves model, retrieval, and AWS
topology choices behind an evaluation contract. This ADR narrows one part of
that future choice: if the proposed path needs model inference in AWS, Bedrock
is the default model boundary because it reuses the verified control plane.
It does not pre-select a foundation model, embedding model, reranker,
knowledge-base product, vector store, or agent framework.

## Problem Statement

The target documents presented “Bedrock or OpenClaw” as if they were equivalent
runtime alternatives and the new request presented “Bedrock or LangGraph” as
another either-or choice. Those comparisons mix architectural layers:

- Bedrock supplies managed model access and related AI platform capabilities.
- LangGraph is an application framework/runtime for graph-shaped, stateful
  agent workflows.
- OpenClaw would supply a separate general agent gateway/runtime that this
  repository would have to operate somewhere, such as ECS/Fargate.
- Step Functions currently supplies durable AWS workflow orchestration, while
  Lambda owns the domain adapter, normalization, validation, and publication
  controls.

The architecture needs one explicit decision per layer, not one product chosen
to solve several unrelated responsibilities.

## Decision Drivers

1. Preserve the verified scheduled workflow and public trust boundary.
2. Minimize undifferentiated operational responsibility for one small
   portfolio workload.
3. Keep model access replaceable behind a stable domain contract.
4. Retain explicit, observable, retryable workflow transitions.
5. Keep IAM narrow and make model, data, and publication permissions
   independently reviewable.
6. Do not add agent nondeterminism unless a validated business process needs
   it.
7. Evaluate quality, safety, latency, and cost before selecting new managed
   retrieval or orchestration services.
8. Keep the interview claim accurate: verified baseline, proposed evolution,
   and deferred decisions must remain distinct.

## Requirements

### Business Requirements

| ID | Requirement | Architectural response |
|---|---|---|
| BR-01 | Produce useful, traceable energy insight without funding a general AI platform | Reuse the current serverless workflow and add only evidence-led capabilities. |
| BR-02 | Keep operating effort and cost proportionate to uncertain demand | Prefer managed invocation and scale-to-zero Lambda execution; do not add an always-on or container-managed agent runtime. |
| BR-03 | Preserve a path to more complex workflows | Keep provider and orchestration contracts explicit; evaluate LangGraph only when its graph semantics solve a demonstrated problem. |
| BR-04 | Explain the decision to mixed technical and non-technical stakeholders | State the customer need, layer responsibilities, trade-offs, controls, and revisit triggers without treating a product name as the architecture. |

### Functional And Quality Requirements

| ID | Requirement | Architectural response |
|---|---|---|
| FR-01 | Invoke a managed foundation model | Use the existing Lambda Bedrock adapter and least-privilege `bedrock:InvokeModel` boundary. |
| FR-02 | Orchestrate scheduled evidence preparation and publication | Retain EventBridge, Step Functions, and explicit Lambda actions. |
| FR-03 | Normalize provider-specific responses | Keep provider response handling in the adapter instead of exposing it to the state-machine contract. |
| FR-04 | Publish only contracted output | Require schema validation, sanitization, source controls, and public/private boundary checks. |
| NFR-01 | Recover safely | Preserve deterministic fallback, failed-path quarantine, last-known-good public artifacts, retries, and bounded rollback. |
| NFR-02 | Observe and audit decisions | Retain run IDs, Step Functions history, S3 evidence, logs, notification, and budget controls. |
| NFR-03 | Limit blast radius | Separate model invocation, lake writes, validation, and publication permissions. |
| NFR-04 | Remain changeable | Keep inference and orchestration behind contracts so a later framework or model change does not rewrite data authority or publication controls. |

## Layered Architecture Decision

| Layer | Selected now | Status | Boundary |
|---|---|---|---|
| Authoritative evidence | Curated, versioned S3 artifacts and schemas | Retained | Retrieval indexes and model outputs are derived, not authoritative. |
| Scheduled workflow orchestration | EventBridge plus Step Functions | Verified | Explicit state transitions, retries, catches, and run evidence. |
| Domain actions and model adapter | Lambda | Verified | Evidence assembly, Bedrock request/response normalization, validation, fallback, and publication actions. |
| Model inference | Amazon Bedrock `InvokeModel` | Verified | IAM-scoped managed inference behind the adapter. |
| Public output | Validated `dashboard_snapshot_v1` only | Verified | Raw model output cannot cross this boundary. |
| Proposed read-only analyst path | ADR 0006 explicit routing and evidence contracts | Accepted design only | P1 evaluation must precede implementation selection. |
| Stateful agent framework | LangGraph | Deferred | Adopt only after a graph-shaped workflow need and evaluation evidence exist. |
| General self-hosted agent runtime | OpenClaw on ECS/Fargate | Rejected | Adds an unjustified runtime and control plane. |

## Target Flow

```text
CURRENT VERIFIED BATCH PATH

EventBridge
  -> Step Functions
  -> Lambda: build validated evidence bundle
  -> Lambda adapter -> Amazon Bedrock InvokeModel
  -> normalize provider response
  -> validate schema, sources, safety, and public fields
     -> valid: curated S3 + approved dashboard snapshot
     -> invalid: failed S3 + notification; retain last-known-good snapshot

PROPOSED READ-ONLY ANALYST PATH UNDER ADR 0006

question
  -> bounded route: structured facts, documents, combined, or unsupported
  -> versioned evidence pack with freshness and provenance
  -> Bedrock inference only when policy permits
  -> schema, citation, grounding, and safety gates
  -> cited answer, deterministic fallback, or abstention

LangGraph is not in either flow unless a later ADR proves that cyclic,
stateful, or human-in-the-loop execution is required.
```

## Why Removing OpenClaw On ECS/Fargate Is The Right Decision

### Benefit Of Removal

Removing the option simplifies the target in concrete ways:

- no container image, dependency, or agent-gateway supply chain to maintain;
- no ECS service/task lifecycle, scaling policy, capacity, health-check, or
  availability design for an otherwise intermittent workload;
- no new ingress, egress, service discovery, TLS, session, or agent-auth
  surface;
- no duplicated secrets, tool credentials, conversation state, or audit model;
- no separate patching, vulnerability, log, metric, tracing, backup, and
  incident process;
- no baseline Fargate cost or scale-to-zero design question where Lambda and
  Bedrock already match the workload shape; and
- no second orchestration control plane competing with Step Functions.

### Necessary Qualification

“Enterprise grade” is an outcome created by requirements and controls, not a
label automatically granted by a managed service or denied to open-source
software. OpenClaw could be operated to enterprise standards with enough
engineering and governance. This repository has no requirement that makes
that investment proportionate. The defensible decision is therefore:

> I removed OpenClaw on ECS/Fargate because the workload did not require a
> self-hosted general agent runtime, and Bedrock behind the existing controlled
> workflow met the inference need with less operational and security surface.

Do not say that OpenClaw was removed merely because it is open source, uses
containers, or can never support an enterprise workload.

## Alternatives And Dispositions

The following choices are made independently. “Rejected” means the option does
not fit the current requirement. “Deferred” means a named evidence trigger
could make it appropriate later. “Retained” means it remains part of the
working baseline or comparison set.

| ID | Choice | Disposition | Attraction | Why it did not win now | Reconsideration trigger |
|---|---|---|---|---|---|
| ALT-01 | Bedrock through the current Lambda adapter, with Step Functions orchestration | Accepted | Reuses verified contracts, IAM, failure handling, observability, scheduling, and budget controls | Not applicable; this is the winning current choice | Revisit only if it fails model, Region, policy, latency, quota, availability, or unit-cost requirements. |
| ALT-02 | Direct Step Functions optimized integration with Bedrock | Deferred | Removes one Lambda hop and can invoke Bedrock as a service integration | The Lambda currently provides useful provider normalization, schema/safety preparation, fallback, and stable workflow contracts; removing it has no measured benefit | A profile shows the adapter hop is a material cost, latency, or reliability problem and its domain controls can move cleanly to adjacent states. |
| ALT-03 | Add LangGraph inside Lambda or another runtime while retaining Bedrock | Deferred | Durable graph semantics, explicit cycles, persistence, streaming, and human-in-the-loop patterns can suit complex agents | The current path is acyclic and explicit; adding another orchestrator would duplicate state, retries, telemetry, and failure ownership | A validated workflow needs loops, resumable checkpoints, dynamic branching, or human review that becomes materially simpler and safer in LangGraph, with a passing evaluation and operating model. |
| ALT-04 | Replace Step Functions with LangGraph | Rejected for current workflow | One orchestration model for future agent features and application-level control | Would discard a verified AWS workflow boundary and reimplement scheduling integration, retries, auditability, IAM, failure routing, and operations without a customer outcome | The workload becomes predominantly interactive/stateful, Step Functions demonstrably obstructs requirements, and migration lowers measured total risk or cost. |
| ALT-05 | Use Bedrock Agents or a managed agent runtime now | Deferred | Managed tool orchestration, memory, identity, observability, and runtime capabilities may reduce custom operations | The use case has two known read-only evidence paths and does not need a planner or broad tools; product and control selection must follow P1-P4 evidence | Several bounded tools and dynamic plans become necessary, explicit routing fails task-success or maintenance gates, and managed-agent evaluation meets safety and cost limits. |
| ALT-06 | Keep OpenClaw on ECS/Fargate as the primary runtime | Rejected and removed | Flexibility, model/provider choice, agent features, self-hosted control, and potential portability | Adds a container and agent control plane with no matching requirement; duplicates orchestration and expands operational/security ownership | Only a new ADR based on a hard requirement Bedrock and bounded frameworks cannot meet, plus a funded operating model, can reopen it. |
| ALT-07 | Keep OpenClaw as a disaster-recovery inference path | Rejected | Provider diversity and a theoretical escape route from Bedrock disruption | An unexercised second runtime increases drift and recovery uncertainty; deterministic fallback is safer for the present business impact | Availability requirements exceed the current fallback, an independent provider is necessary, and regular recovery tests are funded. |
| ALT-08 | Run a custom open model on ECS, EKS, or EC2 | Rejected for now | Maximum model control, data locality, tuning, and possible high-volume economics | Introduces model serving, accelerators, scaling, patching, capacity, availability, and MLOps before scale or policy requires it | Stable high utilization, a model unavailable through managed APIs, hard data-residency constraints, or measured unit economics justify self-hosting. |
| ALT-09 | Use an external model SaaS directly | Rejected as the default; retain as an evaluation candidate only | Broad model choice and potentially strong quality or price | Adds a new vendor, data-processing, networking, credentials, quota, audit, and exit boundary when Bedrock already fits the verified AWS path | Bedrock candidates fail measured quality/cost/latency requirements and governance approves the external processing boundary. |
| ALT-10 | Multi-agent orchestration | Deferred | Specialized roles could divide research, retrieval, verification, and presentation tasks | Adds model calls, coordination failure, nondeterminism, latency, cost, and a much larger evaluation surface without evidence a single bounded flow fails | One bounded agent or explicit graph cannot meet a proven multi-role workflow, and multi-agent evaluation materially improves task success within strict budgets and permissions. |
| ALT-11 | Fine-tuning for model behaviour | Deferred | May improve stable domain terminology, format adherence, classification, or repeated task behaviour | Prompting, retrieval, model choice, and evaluation have not isolated a stable behaviour gap; training adds data governance and model lifecycle cost | A representative benchmark shows a persistent gap after simpler approaches, and governed training plus untouched holdout data exist. |
| ALT-12 | Fine-tuning as a store for current Lakehouse knowledge | Rejected | Could appear to put domain knowledge “inside” the model | Model weights do not provide current evidence, deletion, provenance, or reliable citations | No expected trigger; changing knowledge remains in governed retrieval/evidence stores. |
| ALT-13 | Publish raw model text and rely on the dashboard to label it | Rejected | Simplest path with the least transformation latency | Bypasses schemas, sanitization, provenance, public/private controls, fallback, and audit; a label does not repair unsafe publication | No expected trigger. Model text must remain a candidate until it passes the publication contract. |
| ALT-14 | Keep the deterministic merge only | Retained as fallback and baseline, rejected as the complete GenAI target | Lowest model risk, repeatable output, known operating behaviour | Does not test grounded model synthesis or the proposed analyst question path | It becomes the winning complete solution if GenAI evaluation shows no material customer benefit. |
| ALT-15 | Replace the scheduled batch workflow with an interactive agent path | Rejected | One apparently unified AI architecture | Mixes batch publication with user-specific latency, identity, retrieval, and failure concerns and risks a verified workflow | The interactive path is independently proven to supply every batch outcome with lower measured risk and cost. |

## Bedrock Versus LangGraph: The Correct Comparison

The architecture can use Bedrock without LangGraph, or Bedrock with LangGraph.
It could also use LangGraph with a different model provider. Therefore the
decision is not “Bedrock or LangGraph.” It is:

1. Which managed inference boundary meets model, governance, Region, latency,
   quota, and cost needs?
2. Which orchestration semantics does the workflow require?

For the current workload, the answers are Bedrock and Step Functions/Lambda.
LangGraph is justified only by a later orchestration requirement, not by the
desire to mention an agent framework in an interview.

## Enterprise Control Model

### Identity And Data Access

- Grant `bedrock:InvokeModel` only to the invoking workload role and approved
  model resources.
- Keep S3 read/write permissions prefix-scoped by action responsibility.
- Keep model invocation separate from public snapshot publication permission.
- Do not give a model or framework unrestricted AWS credentials, arbitrary SQL,
  shell access, infrastructure mutation, or public publishing tools.
- Reassess data classification, provider processing, retention, and tenant
  isolation before using confidential customer data.

### Safety And Quality

- Treat model output as untrusted candidate data.
- Validate JSON structure, bounded enums, lengths, source references, and
  public-safe fields.
- For the ADR 0006 path, verify evidence freshness, citation correctness,
  grounding, authorization, and prompt-injection resistance.
- Return deterministic fallback or explicit abstention when a gate fails.
- Never repair unsupported output into apparent validity without re-evaluation.

### Reliability And Operations

- Retain bounded retries and catches at the workflow boundary.
- Quarantine failed candidates without replacing the last-known-good public
  snapshot.
- Version prompts, model identifiers, contracts, policies, and evidence
  references for reproducibility.
- Measure invocation errors, throttles, invalid outputs, fallback/abstention,
  latency, token use, and cost per accepted outcome.
- Use quotas, token/output caps, concurrency limits, schedule control, and
  budget notifications as expansion gates.

### Change And Rollback

- Keep the deterministic path runnable as a comparison and safe fallback.
- A model change must pass the same contract and evaluation set.
- A LangGraph proof must remain local until its value and operating boundary
  are demonstrated.
- Any AWS deployment or resource mutation remains a separately authorized
  decision with an exact IAM, cost, validation, blast-radius, and rollback
  plan.

## Decisions Still Deferred

The following are genuinely undecided after this ADR:

| ID | Deferred decision | Evidence needed before decision | Earliest decision point |
|---|---|---|---|
| D-01 | Whether the ADR 0006 interactive analyst path should proceed at all | P1 named user decision, non-GenAI baseline, representative cases, expected evidence, and stop threshold | P1 evaluation contract |
| D-02 | Approved document corpus, metadata, freshness, deletion, and citation contract | Source classification and versioned corpus manifest | P2 corpus contract |
| D-03 | Structured evidence templates and safe parameter boundaries | Representative exact questions, query semantics, scan/cost limits, and correctness tests | P2 structured evidence contract |
| D-04 | Chunking, embedding, lexical/vector hybrid retrieval, filtering, and reranking | Local retrieval benchmark over representative documents and questions | P3 retrieval benchmark |
| D-05 | Foundation model, prompt, embedding model, and reranker | Quality, groundedness, citation, safety, latency, Region, data policy, quota, and cost results | P3-P4 evaluation |
| D-06 | LangGraph adoption | A proven need for cycles, persistence, streaming, resumability, or human-in-the-loop orchestration plus comparison with explicit code/Step Functions | P4 or later |
| D-07 | Managed knowledge base, vector store, search service, or relational vector extension | Corpus scale, filters, update rate, retrieval quality, IAM, networking, availability, and total-cost evidence | Conditional P5 topology decision |
| D-08 | Direct Step Functions-to-Bedrock integration refactor | Profiled adapter-hop cost/latency and a safe home for normalization, validation, and fallback logic | Later optimization review |
| D-09 | Bedrock managed-agent or AgentCore-style runtime | Several proven dynamic tools, identity/memory/runtime requirements, and agent safety evaluation | Conditional P5 or later |
| D-10 | Multi-agent orchestration | Demonstrated failure of a single bounded workflow and measured multi-agent improvement | After single-flow evaluation |
| D-11 | Fine-tuning for a stable behaviour gap | Prompt/RAG/model baselines, governed training set, untouched holdout, and lifecycle economics | After P4 model evaluation |
| D-12 | Production API, identity, tenancy, conversation memory, and UI | Real users, sensitivity, traffic, SLO, support, and unit-economics requirements | Separate product decision |
| D-13 | Write-capable tools, autonomous actions, and human approval workflow | A named action use case, authorization model, reversible actions, audit, safety case, and explicit owner approval | Separate ADR; outside current read-only scope |
| D-14 | Private networking, endpoint, egress, multi-Region, and production deployment topology | Data classification, availability, RTO/RPO, traffic, Region, and cost requirements | Conditional P5/P6 |
| D-15 | Any AWS deployment of the proposed interactive path | Local gates passed plus an exact Terraform, IAM, cost, rollback, validation, and blast-radius plan | Separately authorized P6 |

The following are **not** deferred: Bedrock `InvokeModel` is current and
verified; OpenClaw/ECS is rejected; raw model text publication is rejected.

## Consequences

### Positive

- The target matches current implementation evidence rather than an obsolete
  Phase 8 backlog.
- The architecture has one managed inference boundary and one current workflow
  boundary with clear responsibilities.
- The decision reduces operating and security surface without closing a
  justified future LangGraph path.
- Existing schemas, fallback, quarantine, notification, audit, and budget work
  remain valuable.
- The interview narrative demonstrates architectural judgment: requirements
  first, managed service where it removes undifferentiated work, and explicit
  stop/revisit conditions.

### Negative And Accepted Trade-Offs

- Bedrock creates AWS service and model-catalog dependency.
- The Lambda adapter adds one component and hop compared with direct Step
  Functions service integration.
- Step Functions is less natural than a graph framework for cyclic,
  token-streaming, or checkpointed conversational agents.
- Rejecting OpenClaw reduces near-term provider/runtime portability.
- Deferring LangGraph means the project will not demonstrate that framework
  before a business requirement earns it.

### Risks And Mitigations

| Risk | Mitigation |
|---|---|
| Bedrock model availability, quota, or behaviour changes | Keep a model adapter, version configuration, deterministic fallback, evaluation set, quotas, and rollback path. |
| Provider response drift | Normalize provider responses and validate the domain contract before writes or publication. |
| Model output is plausible but unsupported | Require provenance/grounding checks for the interactive path and schema/source controls for batch output; fallback or abstain. |
| Framework pressure causes over-engineering | Require a named workflow gap and comparative evaluation before adopting LangGraph or agents. |
| “Managed” is mistaken for automatically secure | Retain explicit IAM, data classification, safety, observability, cost, and incident controls. |
| Historical documents imply an obsolete target | Add current disposition notes and point authoritative target views to this ADR without rewriting immutable evidence as if it never happened. |

## Implementation And Documentation Scope

This ADR authorizes documentation reconciliation only:

- replace “Bedrock or OpenClaw” in current target views with Bedrock;
- mark OpenClaw/ECS rejected and LangGraph deferred;
- correct stale current-state documents that still call Bedrock future work;
- preserve historical evidence while adding a post-Phase-8 disposition;
- update the interview STAR bank without inserting this scenario into the
  assignment deck; and
- remove the stale source-code validation note that recommends future OpenClaw
  output.

This ADR does not authorize adding LangGraph packages, replacing Step
Functions, changing Terraform, deploying code, invoking Bedrock, changing AWS
resources, publishing a dashboard, or deleting historical evidence.

## Interview STAR Scenario

Use this as a supporting GenAI architecture story, not as the assignment case
study. Keep any personal context and result truthful.

### Situation

The Lakehouse had a verified serverless AI insight workflow, but target
documents still proposed multiple overlapping runtime directions: Bedrock,
OpenClaw on ECS/Fargate, and potentially an agent framework. That ambiguity
would have increased the operating surface and weakened the architecture
story.

### Task

Choose a proportionate enterprise-ready model and orchestration boundary that
preserved the existing controls, supported future evidence-grounded GenAI, and
did not start an open-ended platform build.

### Action

- I separated model inference from workflow and agent orchestration instead of
  comparing unlike products.
- I reconciled the decision against the working implementation and found that
  Bedrock invocation was already verified, while OpenClaw infrastructure did
  not exist.
- I retained Step Functions/Lambda for the explicit scheduled flow and Bedrock
  for managed inference.
- I rejected OpenClaw/ECS because no requirement justified its additional
  container and agent control plane.
- I deferred LangGraph until a cyclic, stateful, or human-in-the-loop workflow
  is proven and made evaluation the gate for models, retrieval, and agents.
- I kept model output behind schema, failure, provenance, public/private,
  fallback, notification, and budget controls.

### Result

Repository-proven results that may be stated:

- the existing scheduled Bedrock workflow and its controls were preserved;
- the target architecture lost one unjustified runtime and gained explicit
  decision/revisit criteria;
- Bedrock, LangGraph, and OpenClaw responsibilities are no longer conflated;
  and
- no AWS mutation, new runtime cost, or unsupported production claim was
  introduced by this decision.

Do not invent customer savings, adoption, latency, accuracy, or incident
results. If the interviewer asks for measurable impact, use the actual Phase
17 evidence that can be defended or supply a genuine personal/customer result
outside the repository.

### Reflection

The learning is that “enterprise grade” comes from requirements, controls, and
operability rather than service names. I would next define the P1 evaluation
contract before choosing retrieval technology or adding an agent framework.

## Validation And Promotion Gates

This documentation decision is complete when:

- current target documents and diagrams show Bedrock rather than OpenClaw;
- historical Phase 8 material records the later disposition;
- the decision register distinguishes accepted, rejected, and deferred
  runtime choices;
- the interview plan identifies this as a supporting GenAI STAR story only;
- Markdown, Mermaid, local diff, and public-evidence checks pass; and
- no AWS command or deployment occurs.

Any later LangGraph adoption requires a new or amended ADR containing:

1. the stateful/cyclic/HITL requirement;
2. the explicit-code or Step Functions baseline;
3. task-success, safety, latency, cost, and operability evidence;
4. checkpoint/state ownership and data classification;
5. runtime, scaling, identity, observability, and recovery design; and
6. a bounded implementation and rollback plan.

## Official References

- [Amazon Bedrock Runtime inference APIs](https://docs.aws.amazon.com/bedrock/latest/userguide/inference-api.html)
- [AWS Step Functions optimized integration with Amazon Bedrock](https://docs.aws.amazon.com/step-functions/latest/dg/connect-bedrock.html)
- [LangGraph overview](https://docs.langchain.com/oss/python/langgraph/overview)
- [Amazon Bedrock Agents documentation](https://docs.aws.amazon.com/bedrock/latest/userguide/agents.html)

## Next Tracker-Ordered Step

P1 is complete in
`docs/planning/ai-orchestration-p1-evaluation-contract-20260901.md`. Create P2,
the smallest approved corpus and structured/document evidence contracts. Do
not add LangGraph, select a vector database or managed knowledge base,
fine-tune a model, or deploy an interactive path before the remaining evidence
gates supply the need.
