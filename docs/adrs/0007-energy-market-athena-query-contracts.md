# ADR 0007: Energy Market Athena Query Contracts

- Status: Accepted
- Date: 2026-07-07
- Decision owners: Energy Data Lakehouse repository owner
- Related tracker milestone: lakehouse readiness evidence and SAP-C02 Domain 2 review

## Context

The repository already proves Athena access to curated electricity and gas
tables. The existing query file contains useful single-service examples, but
the portfolio case study needs more explicit energy-market analytics:
electricity demand and imbalance prices, ENTSO-E day-ahead prices, ENTSOG gas
flow and demand proxies, cross-market daily context, and freshness checks.

The query layer should remain simple enough to run from Athena and review in a
public repository. It should not introduce a new BI dependency, materialized
view dependency, or live deployment step.

## Decision

Treat `athena/queries.sql` as the durable query-contract file for demo and
review queries. Maintain `athena/query-contracts.json` as the machine-readable
inventory for those blocks. Extend the pair with energy-specific queries that:

- summarize GB electricity demand, imbalance prices, and settlement-row
  completeness;
- summarize ENTSO-E day-ahead prices by region;
- summarize ENTSOG flow and demand proxy by point direction;
- join electricity and gas daily signals into a dashboard-ready view; and
- expose curated-table freshness and null-field checks.

Each block remains standalone Athena SQL that can be copied into the Athena
console or submitted with `start-query-execution`.

The local validator must keep the numbered SQL blocks and inventory aligned,
reject mutating SQL, verify declared table dependencies, and check that the
declared output-column names remain represented in each query. CI runs the
validator and its unit tests without contacting AWS.

The inventory intentionally records business criticality as `not-approved`.
It identifies representative recovery-validation queries but does not invent
business ownership, expected result values, RTO, or RPO.

## Alternatives Considered

| Option | Decision | Why |
|---|---|---|
| Keep standalone query blocks in `athena/queries.sql` | Accepted | Matches the current repository pattern, keeps examples easy to inspect, and avoids adding deployment state. |
| Create Athena views in Terraform | Rejected for now | Views can be useful, but they would add catalog mutation and live deployment boundaries beyond the issue. |
| Move queries into application code only | Rejected | That would hide the SAP-C02 data-platform reasoning and make reviewer inspection harder. |
| Add a BI/dashboard semantic layer now | Deferred | Useful later, but polished dashboard expansion is a tracker deferral unless the tracker is updated. |
| Split each query into one file | Rejected | The current repo convention is one Athena demo SQL file, and the query count is still manageable. |

## Consequences

- The lakehouse has a clearer public-facing analytics contract for electricity
  and gas.
- Queries remain manually runnable and do not mutate AWS resources.
- Query-number, title, dependency, and output-contract drift now fails local
  validation and CI.
- Cross-market joins are intentionally daily-level and analytical rather than
  production forecasting models.
- Future schema changes must update both the ETL output and this query
  contract.
- Static validation proves repository consistency, not that a recovered Glue
  catalog or Athena workgroup can execute the queries successfully.

## SAP-C02 Relevance

This decision supports Domain 2 by showing how S3, Glue Data Catalog, Parquet,
and Athena combine into a queryable lakehouse. It also supports Domain 3 by
including freshness and data-quality checks that help operate the workload.

## Implementation Artifacts

- `athena/queries.sql`
- `athena/query-contracts.json`
- `scripts/validate_athena_query_contracts.py`
- `tests/test_validate_athena_query_contracts.py`
- `.github/workflows/validate.yml`
- `glue/etl_raw_to_parquet.py`
- `docs/evidence/athena-gas-query-summary-20260506.md`
- `docs/evidence/athena-gas-schema-20260506.md`

## Revisit Conditions

Revisit this ADR if the curated schema changes, Athena views become an approved
deployment target, the dashboard requires a managed semantic layer, or new
market datasets are added to the ETL output.
