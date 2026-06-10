locals {
  managed_workflow_cost_budget_notifications = var.managed_workflow_cost_budget_notification_email == "" ? [] : [
    {
      comparison_operator = "GREATER_THAN"
      notification_type   = "ACTUAL"
      threshold           = 80
      threshold_type      = "PERCENTAGE"
    },
    {
      comparison_operator = "GREATER_THAN"
      notification_type   = "ACTUAL"
      threshold           = 100
      threshold_type      = "PERCENTAGE"
    },
    {
      comparison_operator = "GREATER_THAN"
      notification_type   = "FORECASTED"
      threshold           = 100
      threshold_type      = "PERCENTAGE"
    },
  ]
}

resource "aws_budgets_budget" "managed_workflow_cost" {
  count = var.managed_workflow_cost_budget_enabled ? 1 : 0

  name         = var.managed_workflow_cost_budget_name
  budget_type  = "COST"
  limit_amount = format("%.2f", var.managed_workflow_cost_budget_limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"
  tags         = local.phase8_tags

  cost_filter {
    name   = "Service"
    values = var.managed_workflow_cost_budget_services
  }

  dynamic "notification" {
    for_each = local.managed_workflow_cost_budget_notifications

    content {
      comparison_operator        = notification.value.comparison_operator
      notification_type          = notification.value.notification_type
      subscriber_email_addresses = [var.managed_workflow_cost_budget_notification_email]
      threshold                  = notification.value.threshold
      threshold_type             = notification.value.threshold_type
    }
  }
}
