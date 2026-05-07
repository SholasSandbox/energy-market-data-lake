resource "aws_athena_workgroup" "lakehouse" {
  name          = var.athena_workgroup_name
  force_destroy = true
  tags          = local.common_tags

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = local.athena_output_location

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
