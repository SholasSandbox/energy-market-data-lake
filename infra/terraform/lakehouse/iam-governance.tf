#
# IAM Governance for Dev OU
#

resource "aws_organizations_organizational_unit" "dev" {
  name      = "Dev"
  parent_id = aws_organizations_organization.root.roots[0].id
}

resource "aws_organizations_policy" "dev_boundary" {
  name = "DevBoundaryPolicy"
  content = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["ec2:*", "s3:*", "lambda:*"],
    "Resource": "*"
  }]
}
EOF
}
