# Domain 1 governance boundary.
#
# This lakehouse Terraform root manages workload infrastructure in the
# lakehouse account. AWS Organizations, OU placement, SCP attachment, Identity
# Center, Config delegated administration, and GuardDuty delegated
# administration are controlled by tracker-approved evidence/change notes and
# policy examples under docs/. Do not add aws_organizations_* resources here:
# they require separate explicit approval and a management-account execution
# boundary.

locals {
  governance_home_region = "eu-west-2"

  governance_account_boundaries = {
    management = {
      account_id = "349687196588"
      purpose    = "Organizations, Billing, IAM Identity Center, and SCP administration."
    }

    lakehouse_workload = {
      account_id = "464975959576"
      ou_id      = "ou-gbyf-m6ppfmpq"
      ou_name    = "Lakehouse Workloads OU"
      live_scp_guardrails = [
        "DenyLeavingOrganization",
        "DenyRootUserActions-LakehouseWorkloads",
      ]
    }

    security_log_archive = {
      account_id = "955659429518"
      ou_id      = "ou-gbyf-mug20ym0"
      purpose    = "Storage-only CloudTrail and AWS Config archive boundary."
    }

    security_tooling = {
      account_id = "668848431187"
      ou_id      = "ou-gbyf-mug20ym0"
      purpose    = "AWS Config aggregator/recorder and GuardDuty delegated administration."
    }

    so_aws_admin = {
      account_id = "054394900225"
      status     = "Decommission path; keep excluded from new delegated-admin scope until dependency checks are complete."
    }
  }
}
