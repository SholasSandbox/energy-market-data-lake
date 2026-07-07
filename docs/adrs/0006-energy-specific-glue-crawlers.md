# ADR 0006: Energy-Specific Glue Crawlers

- Status: Accepted for local implementation; live deployment requires explicit approval
- Date: 2026-07-07
- Decision owners: Energy Data Lakehouse repository owner
- Related tracker milestone: lakehouse readiness evidence and SAP-C02 Domain 2 review

## Context

The lakehouse already has broad raw and curated Glue crawlers over the shared
S3 data bucket. Raw objects are partitioned by source and dataset, and curated
Parquet is partitioned by energy dataset, source, region, and date.

The broad crawlers prove the core lakehouse path, but they do not make the
energy-domain boundaries obvious to a reviewer. The repository needs a clearer
local infrastructure pattern for electricity and gas cataloging without
changing live AWS resources as part of this documentation and code cleanup.

## Decision

Keep the existing broad raw and curated crawlers as the default operating
pattern. Add opt-in Terraform definitions for source- and dataset-specific
Glue crawlers:

- raw Elexon electricity data under `raw/source=elexon/`;
- raw ENTSO-E electricity data under `raw/source=entsoe/`;
- raw ENTSOG gas data under `raw/source=entsog/`;
- curated electricity Parquet under `curated/dataset=electricity/`; and
- curated gas Parquet under `curated/dataset=gas/`.

These crawlers are guarded by `enable_energy_specific_crawlers = false` by
default. Enabling and applying them is a separate live-change boundary that
requires a reviewed Terraform plan and explicit approval.

## Alternatives Considered

| Option | Decision | Why |
|---|---|---|
| Keep broad crawlers and add opt-in energy-specific crawlers | Accepted | Preserves the verified live path, adds clearer energy-domain catalog boundaries, and avoids accidental live resource creation. |
| Replace the broad crawlers with only energy-specific crawlers | Rejected | This would be a larger operational change and could disrupt existing table names or validation evidence. |
| Add always-on energy-specific crawlers | Rejected | The resources are low risk, but creating extra live crawlers should still be an explicit boundary because the repository is public and evidence-driven. |
| Model the catalog in Lake Formation now | Deferred | Lake Formation may become useful for cross-account or fine-grained sharing, but it is broader than the current crawler/query cleanup. |
| Leave the gap as documentation only | Rejected | The issue asks for Glue crawler implementation, and local Terraform gives a reviewable, testable artifact without applying it. |

## Consequences

- Reviewers can see the electricity and gas catalog boundaries directly in
  Terraform.
- The existing live broad crawler behavior remains unchanged unless the opt-in
  variable is enabled and applied.
- Future applies can introduce the narrower crawlers with predictable names,
  prefixes, tags, and table prefixes.
- The catalog may contain duplicate logical views of the same objects if both
  broad and energy-specific crawlers are enabled. That is acceptable for a demo
  and exam-readiness repository, but production naming would need governance.

## SAP-C02 Relevance

This decision supports Domain 2 by demonstrating data-platform component
selection, metadata cataloging, partition-aware analytics, and low-risk
infrastructure rollout. It also supports Domain 3 cost and operational review
because the crawler set is explicitly opt-in and scoped to known S3 prefixes.

## Implementation Artifacts

- `infra/terraform/lakehouse/glue.tf`
- `infra/terraform/lakehouse/locals.tf`
- `infra/terraform/lakehouse/variables.tf`
- `infra/terraform/lakehouse/outputs.tf`
- `glue/etl_raw_to_parquet.py`

## Revisit Conditions

Revisit this ADR if the broad crawlers are retired, table naming changes, Lake
Formation becomes the accepted governance layer, new market datasets are added,
or the live account moves to a production catalog-management model.
