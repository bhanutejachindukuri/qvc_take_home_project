# Data Pipeline Exercise

A cloud data pipeline that ingests a CSV from Blob Storage and a table from a
managed Postgres database, transforms both, and lands them in Azure SQL Database
for querying.

> **Status:** implementation files and setup scripts are complete; the Azure
> resources and pipeline run itself still need to be executed by hand in the
> Portal/ADF Studio/Databricks workspace. See `TODO` markers below for what to
> fill in with real values/screenshots once that run happens.

## Architecture

```
Azure Blob Storage              Azure DB for PostgreSQL
container: raw-csv              Flexible Server (Burstable)
  -> products.csv                 -> table: orders
         |                                |
         +----------------+---------------+
                          v
              ADF Notebook Activity
              -> Azure Databricks (new job cluster,
                 spins up per run, terminates after)
              notebook: transform_ecommerce
                - standardize column names
                - handle nulls / type issues
                - add ingestion_timestamp
                          |
                          v
              Azure SQL Database (serverless, auto-pause)
              dbo.products_loaded   dbo.orders_loaded
```

Orchestration: a single Azure Data Factory pipeline, `pl_ecommerce_ingest`,
with one Notebook Activity that runs `databricks/transform_ecommerce.py` on a
Databricks job cluster (created fresh per run, torn down after — this is a
cost control choice, not the default "always-on interactive cluster").

## Services used, and why

| Service | Role | Why |
|---|---|---|
| Azure Blob Storage | CSV source | Simplest, cheapest object storage for a single small file; no need for Data Lake Gen2 hierarchical namespace at this scale. |
| Azure Database for PostgreSQL Flexible Server | Relational DB source | The brief prefers Oracle; PostgreSQL is an explicitly acceptable substitute (see Assumptions below) and is what the rest of this take-home already uses, keeping tooling consistent. Burstable tier keeps cost minimal for a throwaway resource. |
| Azure Databricks | Transform compute | Chosen over ADF Mapping Data Flows specifically because Data Flows get hard to maintain as the number of pipelines grows — a lesson from hands-on production experience. A single PySpark notebook expresses the standardize/null-handle/cast/timestamp logic more directly and scales better as source count grows. A **new job cluster** (not an interactive one) keeps compute cost to the duration of the actual run. |
| Azure Data Factory | Orchestration | Explicitly encouraged in the brief as "closest to [QVC's] current stack." Used here purely for scheduling/monitoring/parameter-passing into the Databricks notebook, not for the transform logic itself. |
| Azure SQL Database | Target | Serverless tier with auto-pause — a small, throwaway analytical target that costs nothing while idle. Target tables are pre-created via DDL (`sql/create_target_tables.sql`) rather than left to auto-create, so column names/types are explicit and reviewable independent of the pipeline code. |

## Setup / run steps

1. Create a resource group (e.g. `qvc-pipeline-rg`) to hold everything below —
   makes cleanup a single delete afterward.
2. Create a storage account + Blob container `raw-csv`; upload `data/products.csv`.
3. Create an Azure Database for PostgreSQL Flexible Server (Burstable B1ms); allow
   access from Azure services; run `sql/seed_orders_postgres.sql` against it to
   create and seed the `orders` table.
4. Create an Azure SQL Database (serverless, auto-pause enabled); run
   `sql/create_target_tables.sql` against it to pre-create `dbo.products_loaded`
   and `dbo.orders_loaded`.
5. Create an Azure Databricks workspace (Standard tier). In it:
   - Create a Databricks-backed secret scope named `adf-pipeline-secrets` with
     three secrets: `storage-account-key`, `postgres-password`, `sql-password`.
   - Import `databricks/transform_ecommerce.py` as a notebook.
   - Confirm the Postgres JDBC driver is available (bundled with the Databricks
     Runtime); if the SQL Server JDBC driver isn't preinstalled, attach it as a
     cluster library (Maven coordinate `com.microsoft.sqlserver:mssql-jdbc`).
6. Create an Azure Data Factory instance. In ADF Studio:
   - Add an Azure Databricks linked service pointing at the workspace, configured
     with **New job cluster** (smallest single-node config, e.g. `Standard_DS3_v2`).
   - Create pipeline `pl_ecommerce_ingest` with a single Notebook Activity
     referencing `transform_ecommerce`, passing the non-secret connection details
     (storage account name, Postgres host, SQL Server host, etc.) as base
     parameters matching the widget names in the notebook.
7. Trigger the pipeline manually; confirm "Succeeded" in the Monitor tab.
8. Query both target tables to confirm the data landed correctly.

## Assumptions / trade-offs

- **Oracle substituted with PostgreSQL.** The brief's preferred relational source
  is Oracle; PostgreSQL is used instead (explicitly listed as an acceptable
  alternative) to stay consistent with the Postgres tooling already used
  elsewhere in this take-home.
- **Small sample data, reused from the SQL Exercise.** `products.csv` (6 rows)
  and the seed for the `orders` table (10 rows) are the SQL Exercise's own
  already-corrected CSVs, not new/larger data. The brief itself frames this as
  "not a fully production-ready solution" — volume wasn't the point being tested.
- **Overwrite, not incremental, load.** Each run truncates and reloads both
  target tables (`truncate=true` on the JDBC writer, preserving the pre-created
  schema rather than letting Spark infer/recreate it). Fine for a one-off
  demonstration; a real recurring pipeline would need an incremental pattern
  (see below).
- **Secrets via a Databricks-backed secret scope**, not hardcoded and not passed
  as plain ADF pipeline parameters. A production setup would back this scope
  with Azure Key Vault instead — simplified here for time.
- **Job cluster, not an interactive/always-on cluster.** Slower per-run
  (cold-start overhead) but avoids leaving paid compute running idle between
  runs — the right trade-off for a resource that's mostly not running.

## Optimizing this pipeline for a recurring daily/monthly run

- **Incremental loads instead of full overwrite.** Add a watermark column
  (e.g. `updated_at` on the source `orders` table, or an ETag/last-modified
  check on the Blob file) and only pull rows newer than the last successful
  run's watermark, merging (`MERGE`/upsert) into the target instead of
  truncate-and-reload.
- **A schedule or tumbling-window trigger** in ADF, with retry policy and
  failure alerting wired to Azure Monitor / Log Analytics (e.g. an action group
  emailing on pipeline failure), rather than the manual trigger used here.
- **Cluster pooling** in Databricks to cut the job cluster's cold-start time on
  each scheduled run, or a small always-warm pool if runs are frequent enough
  to justify it.
- **Partition the target tables** (e.g. by ingestion date) once volume grows
  past what a single unpartitioned table can serve efficiently — not needed at
  this row count.
- **Schema drift handling** — the source CSV/table could change shape over
  time; a production version would validate incoming schema against an
  expected contract before writing, and route mismatches to a
  quarantine/exceptions path rather than failing the whole run silently.
- **Move secrets to Key Vault-backed scopes** and parameterize environment
  (dev/test/prod) via ADF global parameters instead of hardcoded resource names.

## Evidence of execution

<!-- TODO: fill in after running the pipeline -->

- [ ] `screenshots/pipeline_run_succeeded.png` — ADF Monitor tab showing the
  pipeline run status as "Succeeded."
- [ ] `screenshots/query_products_loaded.png` — query output against
  `dbo.products_loaded` (expect 6 rows).
- [ ] `screenshots/query_orders_loaded.png` — query output against
  `dbo.orders_loaded` (expect 10 rows).

## Repo contents

```
Data Pipeline Exercise/
  Data Pipeline Exercise.docx   # original brief
  README.md                     # this file
  sql/
    create_target_tables.sql    # Azure SQL target DDL
    seed_orders_postgres.sql    # Postgres source table DDL + seed data
  data/
    products.csv                # CSV source, staged for blob upload
  databricks/
    transform_ecommerce.py      # PySpark transform notebook source
  adf/
    pipeline_export/            # TODO: exported pipeline/linked-service JSON
  screenshots/                  # TODO: execution evidence
```
