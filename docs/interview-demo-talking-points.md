# Interview And Demo Talking Points

Use this as the short spoken version of the project when discussing the current
state with a Solution Architect interviewer.

## Thirty-Second Pitch

I have built a serverless energy market analytics platform on AWS patterns. It
ingests electricity, gas, and news evidence, lands raw data in S3, curates it
through Glue and Athena, validates JSON contracts, and presents a public-safe
React dashboard for operator decisions. The project now demonstrates the core
solution-architecture path: data ingestion, lakehouse storage, orchestration,
validation gates, AI insight boundaries, dashboard consumption, evidence, and
operational runbooks.

## What Is Built

- Serverless ingestion pattern using EventBridge, Lambda, S3, Glue, and Athena.
- Electricity and ENTSOG gas evidence from raw landing through curated outputs.
- News evidence and managed Bedrock/Mistral insight generation with strict JSON
  contracts and deterministic fallback.
- Scheduled AWS Step Functions orchestration for the AI insight path, with S3
  artifacts, validation gates, failed-run quarantine, notification, budget,
  and scheduled execution evidence.
- Terraform ownership of the core lakehouse and Phase 8 orchestration
  resources, with known drift documented instead of hidden.
- React + TypeScript operator dashboard with Phase 10 executive overview and
  Phase 11 deterministic filters.
- Target operating model diagrams, AWS service architecture diagrams, and an
  operations control-plane view for interview-level explanation.

## AWS Exposure Demonstrated

- **Compute and integration:** Lambda, EventBridge, and Step Functions.
- **Storage and lakehouse:** S3 raw/curated zones, Glue Catalog, Glue ETL
  pattern, and Athena query layer.
- **Infrastructure ownership:** Terraform-managed resources and import
  hardening.
- **Operational controls:** validation gates, failed evidence, runbooks,
  manual execution proof, and documented state transitions.
- **Public boundary design:** private lake data is transformed into approved
  dashboard JSON before the React dashboard reads it.

## AI Exposure Demonstrated

- The AI work is intentionally controlled rather than free-form.
- Inputs are bundled into strict JSON contracts before insight generation.
- Outputs are validated against schemas and can fail into a quarantine path.
- The dashboard consumes only public-safe AI insight snapshots, not private
  lake paths or uncontrolled model output.
- Amazon Bedrock is the verified managed inference boundary; Step Functions and
  Lambda retain explicit workflow, adapter, validation, and publication
  responsibilities.
- ADR 0007 rejects OpenClaw/ECS for this use case and defers LangGraph until a
  stateful, cyclic, resumable, streaming, or human-in-the-loop requirement is
  proven.

## What Remains

- Additional operational alarms where a signal and response are defined.
- DNS, ACM, and custom-domain work if the public demonstration needs them.
- The ADR 0006 P1 evaluation contract for a proposed read-only,
  evidence-grounded analyst path.
- Retrieval, model, and optional agent-framework choices only after evaluation.
- Further hardening around IAM least privilege, observability, and production
  runbooks.

## Strong Interview Framing

This is not presented as a finished enterprise platform. It is a sequenced
architecture build where each phase proves one operating risk: ingestion,
lakehouse curation, gas expansion, AI orchestration, Terraform ownership,
dashboard decisioning, filter determinism, and target operating model clarity.

The important architect-level point is that the project separates private data
processing from the public decision surface. Operators get a dashboard and
exportable snapshots, while raw data, validation failures, orchestration state,
and model-control concerns stay inside the governed AWS boundary.

## Demo Path

1. Start with the target operating model diagram.
2. Move to the AWS service architecture diagram for service-level depth.
3. Show the operations control-plane diagram for governance and run ownership.
4. Open the React dashboard and show alerts, KPIs, risk coverage, market
   context, AI insight, and data quality.
5. Apply Phase 11 filters, reload the URL, and export the filtered snapshot.
6. Close by explaining what remains before a production analyst experience:
   evaluation, retrieval, identity/tenancy, responsible-AI controls, SLOs, and
   operating hardening.
