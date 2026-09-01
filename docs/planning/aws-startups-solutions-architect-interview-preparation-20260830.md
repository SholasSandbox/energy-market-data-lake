# AWS Startups Solutions Architect Interview Preparation Plan

<!-- markdownlint-disable MD013 MD060 -->

**Prepared:** 2026-08-30<br>
**Role:** Startup Solutions Architect, AWS Startups, Job ID 10461029<br>
**Current tracker date:** 2026-09-14; confirm the definitive interview date and time before date-specific rehearsal<br>
**Preparation window:** 2026-08-30 through 2026-09-13, re-anchored relative to the interview if its date changes<br>
**Primary case study:** Energy Market Data Lakehouse<br>
**AWS changes:** None authorized or required

## Purpose And Evidence Boundary

Prepare for the five disclosed interview loops while using the Energy Market
Data Lakehouse as the technical case study for the assignment and architecture
discussion.

The role description and interview structure were supplied by the learner.
The assignment prompt, audience, duration, evaluation criteria, and slide-count
constraint were supplied on 2026-08-30. The resulting five-slide storyboard,
timing, trade-off narrative, and substantive planned GenAI evolution are
recorded in
`docs/planning/aws-startups-sa-assignment-deck-outline-20260830.md`. Submission
mechanics and any confidentiality restrictions still need confirmation.

Repository evidence can prove technical decisions and delivery. It must not be
presented as customer employment, startup operating experience, public-speaking
experience, or a business result unless the learner has a truthful personal
example that supports that claim.

## Role Signal Hierarchy

The role requires more than AWS service recall. Preparation should prioritize:

1. Working backwards from a startup customer's business model, stage, runway,
   team capability, and growth path.
2. Acting as a trusted advisor: clarifying ambiguity, making assumptions
   visible, and recommending an appropriately simple architecture.
3. Designing secure, reliable, scalable, operable, and cost-aware cloud-native
   systems while explaining trade-offs.
4. Communicating at founder, product, engineering, and specialist depth without
   losing the decision being made.
5. Demonstrating practical GenAI fluency across model selection, prompting,
   RAG, evaluation, responsible AI, governance, operations, and cost.
6. Producing reusable technical guidance and showing curiosity, ownership, and
   learning velocity.

## Five-Loop Preparation Matrix

| Loop | Interview dimensions | What the answer must demonstrate | Required preparation evidence |
|---|---|---|---|
| 1 | Business Acumen; Earn Trust; Think Big | Discover the business outcome before selecting services; challenge assumptions constructively; connect architecture to adoption, revenue, runway, risk, and future scale | Two personal STAR stories, one startup discovery drill, and a 90-second motivation for AWS Startups |
| 2 | Invent and Simplify; System Architecture | Ask clarifying questions, state assumptions, design the smallest credible architecture, compare alternatives, and define evolution triggers | Lakehouse current/proposed diagram plus three architecture drills with security, reliability, operations, and cost trade-offs |
| 3 | Ownership; Technical Communication | Take personal responsibility, explain a difficult decision clearly, adapt depth to the audience, and reflect on what would change next time | Two personal STAR stories plus 3-, 10-, and 20-minute versions of the Lakehouse assignment narrative |
| 4 | Customer Obsession; Deliver Results | Prioritize the customer's real constraint, make progress under ambiguity, use metrics, and close the loop | Two customer or stakeholder STAR stories with measurable outcomes; repository evidence can supplement but not replace these stories |
| 5 | Domain Depth; GenAI Fluency; Think Big | Select an AI pattern deliberately, explain RAG versus prompting versus fine-tuning, address evaluation and responsible AI, and show a scalable product vision | One end-to-end GenAI design drill, one model-selection comparison, one RAG deep dive, and one production-readiness review |

## Lakehouse Assignment Strategy

### One-Sentence Customer Narrative

Use this as a provisional framing until the assignment prompt defines the
customer and audience:

> A resource-constrained energy startup needs trustworthy, decision-ready
> market intelligence; propose the smallest credible AWS architecture now and
> a trigger-based path to a secure, scalable, GenAI-enabled product.

Do not turn the assignment into a repository tour. Start with the customer
decision and business constraint, then introduce only the parts of the
Lakehouse that solve that problem.

### Current, Proposed, And Future Boundary

| Layer | What can be said | Lakehouse examples |
|---|---|---|
| Verified current state | Implemented or evidenced today; describe the exact proof and any limitations | Scheduled ingestion; Lambda and EventBridge; S3 raw/curated zones; Glue and Athena; Step Functions; managed Bedrock/Mistral processing; schema validation and failed-path handling; deterministic fallback; public-safe CloudFront dashboard publication; SNS failure notification; AWS Budget guardrail |
| Assignment proposal | A reasoned design, not completed implementation; identify benefit, trade-off, validation, and cost | Reliability and freshness SLOs; CloudWatch operational alarms; retrieval-grounded analyst experience with citations; GenAI evaluation and safety controls; startup product APIs and tenant-aware authorization |
| Trigger-based future | Add only when customer evidence makes the complexity worthwhile | Stronger account or tenant isolation, broader multi-Region recovery, dedicated model customization, higher-throughput serving, or more complex analytics and data-sharing boundaries |

Planned enhancements must be introduced with language such as **I would
propose**, **the next experiment would test**, or **I would revisit this when**.
Never use **implemented**, **deployed**, or **proven** for a conceptual
enhancement.

### Active AI Orchestration Preparation Scope

The controlling tracker released further AI orchestration from the parking lot
on 2026-08-30. This makes interview-relevant architecture work, evaluation
evidence, and local prototyping active preparation rather than a conceptual-
notes-only exception.

Activation does not change the evidence boundary. A design remains proposed; a
local prototype may be described only as locally tested; and a capability may
be described as live only after separately authorized deployment and current
end-to-end verification. No AWS change is authorized by this plan.

Prioritize these outputs in order:

1. the architecture-first package in
   `docs/adr/0006-read-only-evidence-grounded-ai-orchestration.md`,
   `docs/planning/ai-orchestration-architecture-decision-register-20260830.md`,
   and `diagrams/ai-orchestration-evidence-grounded-target.svg`; this is
   complete;
2. a small representative evaluation contract and scorecard covering
   structured evidence, document retrieval,
   groundedness, citations, safety, latency, and cost;
3. if it materially improves interview depth, one locally tested read-only
   vertical slice from approved evidence retrieval to a cited, validated answer;
   and
4. concise demo and Q&A cues that explain promotion gates, limitations, and
   what would change for confidential or multi-tenant data.

Stop after the smallest evidence set that makes the architectural reasoning
credible. Do not let implementation displace the truthful business framing,
STAR preparation, or timed presentation rehearsal.

### Recommended Evolution Story

Keep the assignment roadmap to three decisions:

1. **Make the evidence trustworthy.** Add explicit freshness, quality,
   availability, and cost measures before broadening the product surface.
2. **Ground the GenAI experience.** Use deterministic structured facts for
   exact metrics and retrieve approved documents for explanatory evidence;
   require citations, evaluate retrieval and answer quality, validate output
   contracts, and define human escalation for high-impact decisions.
3. **Productize only after demand is demonstrated.** Add tenant-aware access,
   usage quotas, APIs, and stronger isolation when real customers, data
   sensitivity, or scale justify them.

This sequence demonstrates Think Big without asking a startup to fund an
enterprise platform before product evidence exists.

## Collaborative Architecture Method

Before drawing, clarify:

- Who is the user and what decision are they trying to make?
- What is the startup's stage, runway, team size, and current technical depth?
- What measurable outcome matters: adoption, analyst time saved, accuracy,
  conversion, revenue, or reduced operational risk?
- What are the data sources, ownership, sensitivity, residency, and retention
  requirements?
- What are the traffic shape, latency, availability, RTO, RPO, and growth
  assumptions?
- What is the acceptable MVP cost and which costs must scale with usage?
- For GenAI, what failure is tolerable, what requires a human, and how will
  quality be evaluated before and after release?

Then answer in this order:

1. restate goals and constraints;
2. state unresolved assumptions;
3. propose the minimum architecture;
4. walk one request or data flow end to end;
5. test security, reliability, operations, performance, and cost;
6. compare the most meaningful alternative;
7. define metrics, experiments, and revisit triggers.

## GenAI Fluency Checklist

Be able to explain, without service-name dumping:

- use-case qualification and a non-GenAI baseline;
- model selection across quality, latency, modality, context, availability,
  throughput, data policy, and cost;
- prompt-only, RAG, tool use, agents, and fine-tuning decision boundaries;
- RAG ingestion, chunking, metadata, embeddings, retrieval, reranking,
  generation, citations, and freshness;
- offline test sets, human review, online quality signals, regression gates,
  and business outcome measures;
- prompt injection, data leakage, unsafe content, hallucination, excessive
  agency, least-privilege tool access, and human approval boundaries;
- token budgets, caching, batching, quotas, fallback, graceful degradation,
  and cost per successful business outcome;
- monitoring for latency, errors, throttling, token use, retrieval quality,
  invalid output, user feedback, and model or data drift.

Use the current managed-AI path as evidence for schema contracts, safe public
publication, deterministic fallback, bounded cost, least-privilege invocation,
and incremental delivery. It is not evidence that the current platform already
implements RAG, fine-tuning, comprehensive evaluation, or all responsible-AI
controls.

## Assignment Communication Shape

The disclosed assignment requires three to five slides and approximately 20
minutes of presentation plus 10 minutes of interspersed questions and answers.
Use the five-slide blueprint at
`docs/planning/aws-startups-sa-assignment-deck-outline-20260830.md`. Its timing
preserves this communication spine:

| Time | Content | Decision served |
|---:|---|---|
| 1 minute | Customer, problem, and business outcome | Why this matters |
| 2 minutes | Requirements, constraints, and assumptions | What is known and unknown |
| 4 minutes | Current Lakehouse and evidence | What is already de-risked |
| 2 minutes | Current gaps | Why change is needed |
| 5 minutes | Proposed architecture and one end-to-end flow | What should be built next |
| 3 minutes | Security, reliability, GenAI safety, operations, and cost | Why the proposal is credible |
| 2 minutes | Phased roadmap, metrics, and revisit triggers | How the startup avoids overbuilding |
| 1 minute | Recommendation and questions | What decision is requested |

Also prepare a 3-minute executive version and a 10-minute technical version.
The same facts should survive all three; only depth changes.

## Truthful STAR Story Bank

Prepare at least eight distinct stories. Complete the personal-context and
measurable-result columns from real experience; do not infer them from Git
history.

| Story slot | Best loop | Repository evidence that may support the technical detail | Personal evidence still required |
|---|---|---|---|
| Simplified a design while preserving future options | Invent and Simplify | `docs/adr/0001-shared-s3-data-bucket.md` | Stakeholders, personal action, measurable result, and reflection |
| Balanced security, cost, and operational risk | Business Acumen; Earn Trust | `docs/adr/0002-encryption-and-kms-design.md` | Who relied on the decision and the outcome |
| Built a long-term operating vision from incremental proofs | Think Big | `docs/target-operating-model.md` | Personal motivation, influence, and measurable effect |
| Owned a difficult managed-AI delivery boundary | Ownership; Deliver Results | `docs/phase-17-managed-ai-refresh-preflight.md` and linked evidence | Exact personal actions, setbacks, outcome, and lesson |
| Removed an unjustified agent runtime while preserving an evolution path | Invent and Simplify; GenAI Fluency; Think Big | `docs/adr/0007-bedrock-runtime-and-orchestration-framework-boundary.md` | Genuine decision context, personal ownership, who reviewed or used it, and any result beyond repository simplification |
| Communicated architecture to different audiences | Technical Communication | `docs/interview-demo-talking-points.md` and architecture diagrams | A real audience, feedback, and communication outcome |
| Protected quality or trust by refusing an unsupported claim | Earn Trust | Tracker evidence rules and validation artifacts | A specific event involving another person or stakeholder |
| Worked backwards from a customer or stakeholder need | Customer Obsession | No repository substitute | A genuine external example with measurable outcome |
| Delivered under time, cost, or information constraints | Deliver Results | Bounded AWS changes, rollback gates, and budget evidence | A specific deadline, individual contribution, and result |

Each STAR card must include the initial conditions, the learner's individual
actions, at least two results where possible, what was learned, and what would
be done differently.

### Supporting GenAI STAR Boundary

ADR 0007 can be mentioned in the GenAI loop as a supporting architecture
decision. It is not the presentation assignment story.

The concise decision is: Bedrock supplies managed inference; Step Functions
and Lambda retain the verified explicit workflow and trust controls; OpenClaw
on ECS/Fargate was removed because no requirement justified another
self-hosted agent/container control plane; LangGraph remains conditional on a
proven cyclic, stateful, resumable, streaming, or human-in-the-loop workflow.

Frame “enterprise grade” as an outcome of IAM, data boundaries, evaluation,
validation, observability, resilience, cost controls, and operational
ownership. Do not claim that open-source or containerized software is
inherently unsuitable for an enterprise.

Repository-proven results are limited to preserving the verified Bedrock
workflow, removing an unjustified target component, clarifying deferred
decisions, and making no AWS mutation. Supply genuine stakeholder and business
impact if the interviewer expects a full behavioural STAR result; do not
invent customer metrics from an ADR.

## Bounded Schedule

Keep total preparation near the tracker's 10-12 focused hours per week. If the
interview date changes, retain the sequence and re-anchor the final rehearsal to
T-2 and the light review to T-1.

| Dates | Maximum focus | Required artifact |
|---|---|---|
| 2026-08-30 to 2026-08-31 | 3 hours: role decomposition and evidence boundaries | This plan, one-sentence customer narrative, and assignment current/proposed/future table |
| 2026-09-01 to 2026-09-04 | 6 hours: architecture and GenAI fundamentals | Three architecture outlines plus model-selection, RAG, evaluation, safety, and cost cards |
| 2026-09-05 to 2026-09-07 | 4 hours: business acumen and behavioural evidence | Eight truthful STAR cards, 90-second motivation, and five interviewer questions |
| 2026-09-08 to 2026-09-10 | 5 hours: assignment build and communication | Assignment-specific outline or slides, one 3-minute run, and one timed full run; adapt to the confidential instructions |
| 2026-09-11 to 2026-09-12 | 4 hours: integrated rehearsal | One five-loop mock with targeted feedback; repeat only weak sections |
| 2026-09-13 or T-1 | 1 hour maximum | Light cue review, questions, technology check, and stop |

## Exit Criteria

Preparation is ready when the learner can:

- open every technical problem with useful clarifying questions;
- connect a recommendation to startup value, cost, risk, and time to learn;
- explain the Lakehouse current state without overstating planned work;
- complete a system-design discussion with alternatives and evolution triggers;
- explain RAG, model selection, evaluation, responsible AI, and GenAI cost at
  architecture depth;
- deliver eight distinct STAR stories with individual actions, results, and
  reflection;
- present the assignment at executive and technical depth within time; and
- ask informed questions about startup customers, team priorities, success
  measures, and technical depth.

## Immediate Next Inputs

Before locking the assignment design:

1. confirm the definitive interview date and time;
2. confirm the submission mechanics and whether the supplied assignment brief
   is safe to retain in the repository;
3. identify two genuine customer/stakeholder stories and two difficult
   ownership/delivery stories; and
4. confirm the truthful customer or stakeholder context and any measurable
   business outcome; the technical storyline uses reliability as the current
   foundation and retrieval-grounded GenAI as the next gated experiment.
