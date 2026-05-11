data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${var.project_prefix}-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "lambda_s3" {
  statement {
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
      "s3:PutObject",
      "s3:PutObjectAcl",
    ]

    resources = [
      "arn:aws:s3:::${local.data_bucket_name}",
      "arn:aws:s3:::${local.data_bucket_name}/*",
    ]
  }
}

resource "aws_iam_role_policy" "lambda_s3" {
  name   = "${var.project_prefix}-lambda-s3-policy"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_s3.json
}

resource "aws_iam_role" "ai_orchestration_lambda" {
  count = var.ai_orchestration_enabled ? 1 : 0

  name               = "${var.project_prefix}-ai-orchestration-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy_attachment" "ai_orchestration_lambda_basic_execution" {
  count = var.ai_orchestration_enabled ? 1 : 0

  role       = aws_iam_role.ai_orchestration_lambda[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "ai_orchestration_lambda_s3" {
  count = var.ai_orchestration_enabled ? 1 : 0

  statement {
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${local.data_bucket_name}"]
  }

  statement {
    actions = [
      "s3:GetObject",
      "s3:PutObject",
    ]

    resources = ["arn:aws:s3:::${local.data_bucket_name}/*"]
  }

  statement {
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${local.dashboard_bucket_name}"]
  }

  statement {
    actions = [
      "s3:GetObject",
      "s3:PutObject",
    ]

    resources = ["arn:aws:s3:::${local.dashboard_bucket_name}/*"]
  }
}

resource "aws_iam_role_policy" "ai_orchestration_lambda_s3" {
  count = var.ai_orchestration_enabled ? 1 : 0

  name   = "${var.project_prefix}-ai-orchestration-lambda-s3-policy"
  role   = aws_iam_role.ai_orchestration_lambda[0].id
  policy = data.aws_iam_policy_document.ai_orchestration_lambda_s3[0].json
}

data "aws_iam_policy_document" "step_functions_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ai_orchestration_state_machine" {
  count = var.ai_orchestration_enabled ? 1 : 0

  name               = "${var.project_prefix}-ai-orchestration-sfn-role"
  assume_role_policy = data.aws_iam_policy_document.step_functions_assume_role.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "ai_orchestration_state_machine" {
  count = var.ai_orchestration_enabled ? 1 : 0

  statement {
    actions = [
      "lambda:InvokeFunction",
    ]

    resources = [
      aws_lambda_function.ai_orchestration[0].arn,
      "${aws_lambda_function.ai_orchestration[0].arn}:*",
    ]
  }

  statement {
    actions = [
      "sns:Publish",
    ]

    resources = [aws_sns_topic.ai_orchestration_failures[0].arn]
  }
}

resource "aws_iam_role_policy" "ai_orchestration_state_machine" {
  count = var.ai_orchestration_enabled ? 1 : 0

  name   = "${var.project_prefix}-ai-orchestration-sfn-policy"
  role   = aws_iam_role.ai_orchestration_state_machine[0].id
  policy = data.aws_iam_policy_document.ai_orchestration_state_machine[0].json
}

data "aws_iam_policy_document" "eventbridge_step_functions_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ai_orchestration_eventbridge" {
  count = var.ai_orchestration_enabled ? 1 : 0

  name               = "${var.project_prefix}-ai-orchestration-events-role"
  assume_role_policy = data.aws_iam_policy_document.eventbridge_step_functions_assume_role.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "ai_orchestration_eventbridge" {
  count = var.ai_orchestration_enabled ? 1 : 0

  statement {
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.ai_orchestration[0].arn]
  }
}

resource "aws_iam_role_policy" "ai_orchestration_eventbridge" {
  count = var.ai_orchestration_enabled ? 1 : 0

  name   = "${var.project_prefix}-ai-orchestration-events-policy"
  role   = aws_iam_role.ai_orchestration_eventbridge[0].id
  policy = data.aws_iam_policy_document.ai_orchestration_eventbridge[0].json
}

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.project_prefix}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_s3" {
  statement {
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${local.data_bucket_name}"]
  }

  statement {
    actions = [
      "s3:DeleteObject",
      "s3:GetObject",
      "s3:PutObject",
    ]

    resources = ["arn:aws:s3:::${local.data_bucket_name}/*"]
  }
}

resource "aws_iam_role_policy" "glue_s3" {
  name   = "${var.project_prefix}-glue-s3-policy"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_s3.json
}
