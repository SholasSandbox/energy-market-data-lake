# Phase 17O Managed AI Publish/Deployment Preflight Evidence

Date: 2026-05-28

## Boundary

Phase 17O is a preflight decision state after Phase 17N proved the managed
Mistral path can produce schema-valid `ai_insight_v1` output in memory.

No live Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
EventBridge schedule enablement, DNS, ACM, alarm, budget, dashboard hosting
change, dashboard publish, raw prompt commit, raw model-response commit, or
validated payload commit was performed.

## Current Evidence Reviewed

- Phase 17N sixth live invocation:
  `docs/evidence/phase17n-mistral-sixth-live-invocation-summary-20260528.md`
- Phase 17N sanitized metadata:
  `docs/evidence/phase17n-mistral-sixth-live-invocation-metadata-20260528.json`
- Managed AI handler path:
  `lambda/news_ai_orchestration.py`
- Current Step Functions definition:
  `infra/terraform/lakehouse/stepfunctions.tf`
- Dashboard publish script:
  `scripts/publish_dashboard_static_site.sh`

## Current Operating Facts

- Phase 17N produced `status=validation_passed` in sanitized metadata.
- No parsed `ai_insight_v1` payload was committed because Phase 17N approved
  sanitized metadata only.
- The public dashboard snapshot was not changed.
- The deployed workflow remains deterministic by design unless the state
  machine is changed from `MergeAiInsightDeterministic` to
  `MergeAiInsightManaged`.
- The Terraform state-machine definition still routes to
  `MergeAiInsightDeterministic`.
- The managed handler exists in code, but a production workflow switch would
  also require IAM, environment, rollback, and state-machine proof.
- The dashboard static-site publish script intentionally preserves
  `dashboard_snapshot_v1.json` and `snapshots/*` during static asset publish.

## Decision

Recommendation: **no-go for immediate dashboard publish or handler/state-machine
deployment**.

Phase 17N is a major proof point, but it does not yet prove the operating path
that would safely publish managed AI output. The next state should capture a
public-safe validated `ai_insight_v1` payload as an evidence artifact before
any dashboard object or Step Functions definition is changed.

## Recommended Next Boundary

Recommended next slice: **Phase 17P: managed AI validated payload capture**.

Scope:

- one controlled managed AI invocation only if explicitly approved
- no retry
- no dashboard publish
- no Terraform apply
- no IAM, state-machine, schedule, DNS, ACM, alarm, budget, or hosting change
- commit only a public-safe parsed `ai_insight_v1` artifact if it validates
- keep raw prompt and raw model response uncommitted
- preserve deterministic fallback

## Future Publish/Deployment Split

Do not combine these decisions:

1. **Payload capture**: prove a public-safe validated `ai_insight_v1` payload
   can be captured as evidence.
2. **Dashboard publish**: decide whether that validated payload should become a
   dashboard snapshot, with rollback and CloudFront checks.
3. **Workflow deployment**: decide whether to switch Step Functions from
   `MergeAiInsightDeterministic` to `MergeAiInsightManaged`, with IAM,
   environment, failure-path, and rollback proof.

Keeping these separate prevents a successful model response from turning into
an unreviewed production workflow change.

## Rollback

No AWS rollback is required because this state performs no live AWS mutation.

To abandon this preflight, remove only this evidence and the related
documentation updates from the branch. Do not publish managed AI output or
change Step Functions wiring unless a later state explicitly approves it.

## Proof Commands

```bash
git status --short --branch
.venv/bin/python scripts/check_phase17a_managed_ai_adapter.py
python3 -m json.tool \
  docs/evidence/phase17n-mistral-sixth-live-invocation-metadata-20260528.json
npx markdownlint-cli2 README.md PLANS.md docs/demo-walkthrough.md \
  docs/phase-17-managed-ai-refresh-preflight.md \
  docs/evidence/phase17o-managed-ai-publish-deployment-preflight-20260528.md
git diff --check
```
