locals {
  ai_orchestration_merge_action     = var.ai_orchestration_managed_ai_enabled ? "MergeAiInsightManaged" : "MergeAiInsightDeterministic"
  ai_orchestration_workflow_comment = var.ai_orchestration_managed_ai_enabled ? "Managed Phase 17 AI insight orchestration with validation gates, deterministic rollback, and failure quarantine." : "Deterministic Phase 8 AI insight orchestration with validation gates and failure quarantine."

  ai_orchestration_lambda_retry = [
    {
      ErrorEquals = [
        "Lambda.ServiceException",
        "Lambda.AWSLambdaException",
        "Lambda.SdkClientException",
        "Lambda.TooManyRequestsException",
      ]
      IntervalSeconds = 2
      MaxAttempts     = 2
      BackoffRate     = 2
    }
  ]

  ai_orchestration_lambda_catch = [
    {
      ErrorEquals = ["States.ALL"]
      ResultPath  = "$.error"
      Next        = "WorkflowFailed"
    }
  ]
}

resource "aws_sns_topic" "ai_orchestration_failures" {
  count = var.ai_orchestration_enabled ? 1 : 0

  name = "${var.project_prefix}-ai-orchestration-failures"
  tags = local.phase8_tags
}

resource "aws_sns_topic_subscription" "ai_orchestration_failure_email" {
  count = var.ai_orchestration_enabled && var.ai_orchestration_sns_email != "" ? 1 : 0

  topic_arn = aws_sns_topic.ai_orchestration_failures[0].arn
  protocol  = "email"
  endpoint  = var.ai_orchestration_sns_email
}

resource "aws_sfn_state_machine" "ai_orchestration" {
  count = var.ai_orchestration_enabled ? 1 : 0

  name     = var.ai_orchestration_state_machine_name
  role_arn = aws_iam_role.ai_orchestration_state_machine[0].arn
  tags     = local.phase8_tags
  depends_on = [
    aws_iam_role_policy.ai_orchestration_state_machine,
  ]

  definition = jsonencode({
    Comment = local.ai_orchestration_workflow_comment
    StartAt = "InitializeRun"
    States = {
      InitializeRun = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.ai_orchestration[0].arn
          Payload = {
            action             = "InitializeRun"
            lake_bucket        = local.data_bucket_name
            dashboard_bucket   = local.dashboard_bucket_name
            dashboard_data_key = var.ai_orchestration_dashboard_data_key
          }
        }
        OutputPath = "$.Payload"
        Retry      = local.ai_orchestration_lambda_retry
        Catch      = local.ai_orchestration_lambda_catch
        Next       = "ExportEnergyInput"
      }
      ExportEnergyInput = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.ai_orchestration[0].arn
          Payload = {
            action               = "ExportEnergyInput"
            "run_id.$"           = "$.run_id"
            "lake_bucket.$"      = "$.lake_bucket"
            "dashboard_bucket.$" = "$.dashboard_bucket"
            "artifacts.$"        = "$.artifacts"
            "summary.$"          = "$.summary"
          }
        }
        OutputPath = "$.Payload"
        Retry      = local.ai_orchestration_lambda_retry
        Catch      = local.ai_orchestration_lambda_catch
        Next       = "IngestNewsSummary"
      }
      IngestNewsSummary = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.ai_orchestration[0].arn
          Payload = {
            action               = "IngestNewsSummary"
            "run_id.$"           = "$.run_id"
            "lake_bucket.$"      = "$.lake_bucket"
            "dashboard_bucket.$" = "$.dashboard_bucket"
            "artifacts.$"        = "$.artifacts"
            "summary.$"          = "$.summary"
          }
        }
        OutputPath = "$.Payload"
        Retry      = local.ai_orchestration_lambda_retry
        Catch      = local.ai_orchestration_lambda_catch
        Next       = "CreateAiInputBundle"
      }
      CreateAiInputBundle = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.ai_orchestration[0].arn
          Payload = {
            action               = "CreateAiInputBundle"
            "run_id.$"           = "$.run_id"
            "lake_bucket.$"      = "$.lake_bucket"
            "dashboard_bucket.$" = "$.dashboard_bucket"
            "artifacts.$"        = "$.artifacts"
            "summary.$"          = "$.summary"
          }
        }
        OutputPath = "$.Payload"
        Retry      = local.ai_orchestration_lambda_retry
        Catch      = local.ai_orchestration_lambda_catch
        Next       = local.ai_orchestration_merge_action
      }
      (local.ai_orchestration_merge_action) = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.ai_orchestration[0].arn
          Payload = {
            action               = local.ai_orchestration_merge_action
            "run_id.$"           = "$.run_id"
            "lake_bucket.$"      = "$.lake_bucket"
            "dashboard_bucket.$" = "$.dashboard_bucket"
            "artifacts.$"        = "$.artifacts"
            "summary.$"          = "$.summary"
          }
        }
        OutputPath = "$.Payload"
        Retry      = local.ai_orchestration_lambda_retry
        Catch      = local.ai_orchestration_lambda_catch
        Next       = "PublishDashboardSnapshot"
      }
      PublishDashboardSnapshot = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.ai_orchestration[0].arn
          Payload = {
            action               = "PublishDashboardSnapshot"
            "run_id.$"           = "$.run_id"
            "lake_bucket.$"      = "$.lake_bucket"
            "dashboard_bucket.$" = "$.dashboard_bucket"
            "artifacts.$"        = "$.artifacts"
            "summary.$"          = "$.summary"
          }
        }
        OutputPath = "$.Payload"
        Retry      = local.ai_orchestration_lambda_retry
        Catch      = local.ai_orchestration_lambda_catch
        End        = true
      }
      WorkflowFailed = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.ai_orchestration_failures[0].arn
          Subject     = "Energy market AI insight orchestration failed"
          "Message.$" = "States.JsonToString($)"
        }
        Next = "FailExecution"
      }
      FailExecution = {
        Type  = "Fail"
        Error = "AIInsightOrchestrationFailed"
        Cause = "A Phase 8 orchestration step failed. Inspect failed/ records and Step Functions execution history."
      }
    }
  })
}

resource "aws_cloudwatch_event_rule" "ai_orchestration_schedule" {
  count = var.ai_orchestration_enabled ? 1 : 0

  name                = "${var.project_prefix}-ai-orchestration-schedule"
  description         = "Scheduled deterministic AI insight orchestration trigger."
  schedule_expression = var.ai_orchestration_schedule_expression
  state               = var.ai_orchestration_schedule_enabled ? "ENABLED" : "DISABLED"
  tags                = local.phase8_tags
}

resource "aws_cloudwatch_event_target" "ai_orchestration_schedule" {
  count = var.ai_orchestration_enabled ? 1 : 0

  rule     = aws_cloudwatch_event_rule.ai_orchestration_schedule[0].name
  arn      = aws_sfn_state_machine.ai_orchestration[0].arn
  role_arn = aws_iam_role.ai_orchestration_eventbridge[0].arn
}
