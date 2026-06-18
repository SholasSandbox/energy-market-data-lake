# Repository Governance Instructions

## Controlling planning document

`docs/planning/sap-c02-readiness-tracker.md` is the controlling document for this repository's near-term scope, sequencing, and completion criteria. Codex must read it before starting any work in this repository and must use it to decide whether a requested task should proceed, be narrowed, or be deferred.

## External learning workspace

`/Users/shola/Kiro-Workspace/handlers` is a separate Python/serverless tutorial
workspace. Do not treat its code as Energy Data Lakehouse implementation
evidence or copy it into this repository by default. A tutorial pattern may be
adapted here only when it addresses a named tracker gap or SAP-C02 weak area
and is tested against this repository's contracts and IAM boundaries.

## Required pre-work check

Before making changes, Codex must verify the requested work against the SAP-C02 readiness tracker by checking whether it supports at least one of the following outcomes:

1. SAP-C02 exam readiness.
2. The Energy Data Lakehouse case study.
3. AWS governance, including IAM, Organizations, SCPs, logging, security, networking, resilience, migration, or cost controls.
4. Near-term cloud architect positioning.

If a task does not clearly support one of these outcomes, Codex must reject or defer the work and explain which tracker rule or hard deferral applies. If only part of a task supports the tracker, Codex must narrow the work to the supporting portion and defer the rest.

## Governance conflict resolution

When repository documents, older plans, backlog items, or requested implementation details conflict with the SAP-C02 readiness tracker, Codex must treat the tracker as the controlling source unless the current user instruction explicitly updates the tracker or grants a one-off exception. Codex must not use older documentation, existing application features, or prior PR scope as authorization to do work that the tracker would reject or defer.

When resolving a Git or patch conflict in `AGENTS.md`, preserve the tracker-read requirement, the four approved outcome gates, the reject/defer rule, the scope and sequencing rules, and the AWS safety rule. If another version of this file weakens those requirements, keep the stronger tracker-governance language and only merge non-conflicting clarifications.

## Scope and sequencing rules

- Follow the tracker's current weekly focus, monthly milestones, lakehouse readiness checklist, governance checklist, and evidence requirements when choosing implementation order.
- Prefer artifacts required by the tracker, including code commits, architecture diagrams, ADRs, service comparison tables, IAM/SCP policy examples, wrong-answer log entries, exam-domain notes, and operational runbooks or checklists.
- Every ADR must articulate the design trade-offs behind the accepted decision. It must state the winning choice, the meaningful alternatives considered, why each rejected option was not chosen for the current repository context, and the conditions that would cause the decision to be revisited.
- Treat tracker hard deferrals as out of scope until after the SAP-C02 attempt unless the user explicitly updates the tracker.
- Do not expand polished UI/dashboard work, deep Kubernetes/EKS work, complex microservices, deep REMIT workflow build-out, or AI orchestration beyond light conceptual notes unless the tracker is updated first.
- Keep application changes aligned with the Energy Data Lakehouse, AWS governance, SAP-C02 domain coverage, or immediate portfolio/job-market evidence.

## Completion criteria for future work

For every task, Codex must summarize how the completed work maps back to the tracker, including the relevant SAP-C02 domain, lakehouse/governance checklist item, milestone, or evidence artifact. If the work does not change code, the summary must still identify the tracker rule or deliverable it supports.

## AWS safety

Do not deploy to AWS, modify AWS resources, or run commands that create, update, or delete cloud infrastructure unless the user explicitly requests that action for the current task. Prefer documentation, local validation, and dry-run style checks when possible.
