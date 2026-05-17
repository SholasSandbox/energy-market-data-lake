data "aws_cloudfront_cache_policy" "dashboard_static" {
  count = local.dashboard_cloudfront_enabled ? 1 : 0

  name = "Managed-CachingOptimized"
}

resource "aws_cloudfront_origin_access_control" "dashboard_static" {
  count = local.dashboard_cloudfront_enabled ? 1 : 0

  name                              = "${var.project_prefix}-dashboard-static-oac"
  description                       = "OAC for private S3 dashboard delivery"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

resource "aws_cloudfront_response_headers_policy" "dashboard_static" {
  count = local.dashboard_cloudfront_enabled ? 1 : 0

  name    = "${var.project_prefix}-dashboard-static-security-headers"
  comment = "Security headers for public-safe dashboard delivery"

  security_headers_config {
    content_type_options {
      override = true
    }

    frame_options {
      frame_option = "DENY"
      override     = true
    }

    referrer_policy {
      referrer_policy = "strict-origin-when-cross-origin"
      override        = true
    }

    strict_transport_security {
      access_control_max_age_sec = 31536000
      include_subdomains         = true
      override                   = true
    }

    xss_protection {
      mode_block = true
      override   = true
      protection = true
    }
  }
}

resource "aws_cloudfront_distribution" "dashboard_static" {
  count = local.dashboard_cloudfront_enabled ? 1 : 0

  enabled             = true
  is_ipv6_enabled     = true
  comment             = "${var.project_prefix} public-safe dashboard delivery"
  default_root_object = "index.html"
  price_class         = var.dashboard_cloudfront_price_class
  tags                = local.phase12_tags

  origin {
    domain_name              = aws_s3_bucket.dashboard_static[0].bucket_regional_domain_name
    origin_access_control_id = aws_cloudfront_origin_access_control.dashboard_static[0].id
    origin_id                = local.dashboard_origin_id
  }

  default_cache_behavior {
    allowed_methods            = ["GET", "HEAD", "OPTIONS"]
    cached_methods             = ["GET", "HEAD"]
    cache_policy_id            = data.aws_cloudfront_cache_policy.dashboard_static[0].id
    compress                   = true
    response_headers_policy_id = aws_cloudfront_response_headers_policy.dashboard_static[0].id
    target_origin_id           = local.dashboard_origin_id
    viewer_protocol_policy     = "redirect-to-https"
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }
}

data "aws_iam_policy_document" "dashboard_static_cloudfront" {
  count = local.dashboard_cloudfront_enabled ? 1 : 0

  statement {
    sid     = "AllowCloudFrontRead"
    actions = ["s3:GetObject"]

    resources = [
      "${aws_s3_bucket.dashboard_static[0].arn}/*",
    ]

    principals {
      type        = "Service"
      identifiers = ["cloudfront.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "AWS:SourceArn"
      values   = [aws_cloudfront_distribution.dashboard_static[0].arn]
    }
  }
}

resource "aws_s3_bucket_policy" "dashboard_static_cloudfront" {
  count = local.dashboard_cloudfront_enabled ? 1 : 0

  bucket = aws_s3_bucket.dashboard_static[0].id
  policy = data.aws_iam_policy_document.dashboard_static_cloudfront[0].json
}
