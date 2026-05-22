# Phase 17 Managed AI Refresh Preflight Read-Only Evidence

Date: 2026-05-22

## Git State

```text
## docs/phase17-managed-ai-refresh-preflight
?? docs/evidence/phase17-managed-ai-refresh-preflight-readonly-20260522.md
```

## EventBridge Schedule

```json
{
    "name": "energy-market-ai-orchestration-schedule",
    "state": "DISABLED",
    "schedule": "cron(30 7 * * ? *)"
}
```

## Step Functions State Machine

```json
{
    "name": "energy-market-ai-insight-orchestration",
    "status": "ACTIVE",
    "type": "STANDARD",
    "roleArn": "arn:aws:iam::464975959576:role/energy-market-ai-orchestration-sfn-role"
}
```

## AI Orchestration Lambda Sanitized Config

```json
{
    "FunctionName": "energy-market-news-ai-orchestration",
    "Runtime": "python3.11",
    "Handler": "news_ai_orchestration.lambda_handler",
    "Timeout": 120,
    "MemorySize": 512,
    "LastModified": "2026-05-12T10:48:25.000+0000",
    "State": "Active"
}
```
