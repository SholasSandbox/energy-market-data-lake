resource "aws_s3_bucket" "data_lake" {
  count = var.create_data_bucket ? 1 : 0

  bucket = local.data_bucket_name
  tags   = local.common_tags
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  count = var.create_data_bucket ? 1 : 0

  bucket = aws_s3_bucket.data_lake[0].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  count = var.create_data_bucket ? 1 : 0

  bucket = aws_s3_bucket.data_lake[0].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  count = var.create_data_bucket ? 1 : 0

  bucket = aws_s3_bucket.data_lake[0].id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  count = var.create_data_bucket ? 1 : 0

  bucket = aws_s3_bucket.data_lake[0].id

  rule {
    id     = "raw-lifecycle"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    expiration {
      days = 180
    }
  }
}

resource "aws_s3_bucket" "dashboard_static" {
  count = var.create_dashboard_bucket ? 1 : 0

  bucket = local.dashboard_bucket_name
  tags   = local.common_tags
}

resource "aws_s3_bucket_public_access_block" "dashboard_static" {
  count = var.create_dashboard_bucket ? 1 : 0

  bucket = aws_s3_bucket.dashboard_static[0].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "dashboard_static" {
  count = var.create_dashboard_bucket ? 1 : 0

  bucket = aws_s3_bucket.dashboard_static[0].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "dashboard_static" {
  count = var.create_dashboard_bucket ? 1 : 0

  bucket = aws_s3_bucket.dashboard_static[0].id

  versioning_configuration {
    status = "Enabled"
  }
}
