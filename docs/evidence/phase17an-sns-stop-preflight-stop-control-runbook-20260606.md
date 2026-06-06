# Phase 17AN Stop-Control Runbook

<!-- markdownlint-disable MD013 -->

Use this runbook only after a future phase explicitly enables the managed
workflow EventBridge schedule.

## Emergency Disable

Disable the managed workflow schedule immediately:

```bash
aws events disable-rule \
  --name energy-market-ai-orchestration-schedule \
  --region eu-west-2
```

## Terraform Reconciliation

After the emergency disable, reconcile Terraform back to schedule-disabled
truth:

```bash
terraform -chdir=infra/terraform/lakehouse apply \
  -var 'create_dashboard_bucket=true' \
  -var 'dashboard_cloudfront_enabled=true' \
  -var 'ai_orchestration_managed_ai_enabled=true' \
  -var 'ai_orchestration_schedule_enabled=false'
```

## Stop Criteria

Disable the schedule if any of these occur after future schedule enablement:

- one scheduled Step Functions execution fails
- SNS subscription is missing, pending, or test alert is not received
- dashboard snapshot fails schema validation
- public snapshot contains private references
- latest CloudFront dashboard path is unhealthy after the expected publish/cache
  window
- duplicate or unexpected Step Functions executions occur
- Bedrock/model cost exceeds the agreed cap
- post-enablement Terraform plan is not no-change
- Lambda or Step Functions throttling, repeated retries, or persistent errors
  appear

Operating posture: one scheduled failure means disable first, then investigate.
