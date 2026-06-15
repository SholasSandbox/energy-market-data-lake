data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_partition" "current" {}

data "aws_caller_identity" "current" {}

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
  tags               = local.phase8_tags
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

data "aws_iam_policy_document" "ai_orchestration_lambda_bedrock" {
  count = var.ai_orchestration_enabled && var.ai_orchestration_managed_ai_enabled ? 1 : 0

  statement {
    actions = [
      "bedrock:InvokeModel",
    ]

    resources = [
      var.ai_orchestration_bedrock_model_arn != "" ? var.ai_orchestration_bedrock_model_arn : "arn:${data.aws_partition.current.partition}:bedrock:${var.aws_region}::foundation-model/${var.ai_orchestration_bedrock_model_id}",
    ]
  }
}

resource "aws_iam_role_policy" "ai_orchestration_lambda_bedrock" {
  count = var.ai_orchestration_enabled && var.ai_orchestration_managed_ai_enabled ? 1 : 0

  name   = "${var.project_prefix}-ai-orchestration-lambda-bedrock-policy"
  role   = aws_iam_role.ai_orchestration_lambda[0].id
  policy = data.aws_iam_policy_document.ai_orchestration_lambda_bedrock[0].json
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
  tags               = local.phase8_tags
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
  tags               = local.phase8_tags
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
    sid       = "ReadBucketLocation"
    actions   = ["s3:GetBucketLocation"]
    resources = ["arn:aws:s3:::${local.data_bucket_name}"]
  }

  statement {
    sid = "ListRequiredPrefixes"

    actions = [
      "s3:ListBucket",
      "s3:ListBucketVersions",
    ]

    resources = ["arn:aws:s3:::${local.data_bucket_name}"]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        "raw",
        "raw/*",
        "curated",
        "curated/*",
        "scripts",
        "scripts/*",
      ]
    }
  }

  statement {
    sid = "ReadSourceCatalogAndScriptObjects"

    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
    ]

    resources = [
      "arn:aws:s3:::${local.data_bucket_name}/raw/*",
      "arn:aws:s3:::${local.data_bucket_name}/curated/*",
      "arn:aws:s3:::${local.data_bucket_name}/scripts/*",
    ]
  }

  statement {
    sid = "WriteCuratedObjectsOnly"

    actions = [
      "s3:AbortMultipartUpload",
      "s3:DeleteObject",
      "s3:ListMultipartUploadParts",
      "s3:PutObject",
    ]

    resources = ["arn:aws:s3:::${local.data_bucket_name}/curated/*"]
  }
}

resource "aws_iam_role_policy" "glue_s3" {
  name   = "${var.project_prefix}-glue-s3-policy"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_s3.json
}

data "aws_iam_policy_document" "athena_query_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = ["arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root"]
    }
  }
}

resource "aws_iam_role" "athena_query" {
  name                 = var.athena_query_role_name
  assume_role_policy   = data.aws_iam_policy_document.athena_query_assume_role.json
  max_session_duration = 3600
  tags                 = local.common_tags
}

data "aws_iam_policy_document" "athena_query" {
  statement {
    sid = "UseLakehouseWorkgroup"

    actions = [
      "athena:BatchGetQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:GetQueryResultsStream",
      "athena:GetQueryRuntimeStatistics",
      "athena:GetWorkGroup",
      "athena:ListQueryExecutions",
      "athena:StartQueryExecution",
      "athena:StopQueryExecution",
    ]

    resources = [aws_athena_workgroup.lakehouse.arn]
  }

  statement {
    sid = "ReadLakehouseCatalog"

    actions = [
      "glue:BatchGetPartition",
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetTableVersion",
      "glue:GetTableVersions",
    ]

    resources = [
      "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      aws_glue_catalog_database.lakehouse.arn,
      "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.lakehouse.name}/*",
    ]
  }

  statement {
    sid       = "ReadBucketLocation"
    actions   = ["s3:GetBucketLocation"]
    resources = ["arn:aws:s3:::${local.data_bucket_name}"]
  }

  statement {
    sid = "ListCuratedAndQueryResultPrefixes"

    actions = [
      "s3:ListBucket",
      "s3:ListBucketVersions",
    ]

    resources = ["arn:aws:s3:::${local.data_bucket_name}"]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        "curated",
        "curated/*",
        local.athena_results_prefix,
        "${local.athena_results_prefix}*",
      ]
    }
  }

  statement {
    sid = "ReadCuratedObjects"

    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
    ]

    resources = ["arn:aws:s3:::${local.data_bucket_name}/curated/*"]
  }

  statement {
    sid = "ManageBoundedQueryResults"

    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:ListMultipartUploadParts",
      "s3:PutObject",
    ]

    resources = ["arn:aws:s3:::${local.data_bucket_name}/${local.athena_results_prefix}*"]
  }
}

resource "aws_iam_role_policy" "athena_query" {
  name   = "${var.project_prefix}-athena-query-policy"
  role   = aws_iam_role.athena_query.id
  policy = data.aws_iam_policy_document.athena_query.json
}
