# ADF pipeline design — `pl_olist_ingest`

Build this in ADF Studio, then export the JSON (Manage → ARM template, or
connect the factory to a Git repo) and commit it under `adf/` as the
implementation artifact.

> **Relational-source note:** the five transactional entities are copied
> from a real PostgreSQL 16 — the SQL Exercise's local Docker container
> (`qvc_sql_exercise`, host port 5432), seeded with the five `src.*`
> tables by `postgres_source/postgres_seed.sql`. Because that database
> lives on the laptop, ADF reaches it through a **Self-Hosted Integration
> Runtime** installed on the same machine — deliberately the same pattern
> used for on-prem Oracle sources in production (Oracle→Postgres
> substitution noted per the brief; Azure Database for PostgreSQL could
> not be provisioned in this subscription).
>
> **Verified fallback** (if the SHIR path is blocked): typed parquet
> extracts of the same five tables, built by
> `local_test/build_dbextract_parquet.py` and uploaded to the `dbextract`
> container. Swapping a copy's source from `ds_pg_table` to
> `ds_adls_parquet_dbextract` is the ONLY change — sink, run-scoped raw
> layout and notebook are identical in both versions.

## Integration runtimes

- **AutoResolveIntegrationRuntime** — all ADLS↔ADLS copies and the
  Databricks activity.
- **`shir-laptop`** (Self-Hosted) — Manage → Integration runtimes → New →
  Self-Hosted → install the MSI on this machine and register it with the
  key ADF shows. Required because the Postgres source is local to the
  laptop. The laptop, Docker Desktop and the `qvc_sql_exercise` container
  must all be running during a pipeline run.

## Linked services

| Name | Type | Notes |
|---|---|---|
| `ls_adls` | Azure Data Lake Storage Gen2 | Auth: **system-assigned managed identity** of the factory; grant it *Storage Blob Data Contributor* on the storage account |
| `ls_postgres_src` | PostgreSQL (V2 connector) | **connectVia: `shir-laptop`**; host `localhost`, port `5432`, database `olist`, user `qvc`, password from Key Vault (`pg-password`); **SSL mode: disable** (the local container runs without TLS — noted as exercise-grade; production would require TLS) |
| `ls_kv` | Azure Key Vault | Managed identity; grant the factory *Key Vault Secrets User* |
| `ls_databricks` | Azure Databricks | Point at the workspace; auth via managed identity (grant the factory *Contributor* on the workspace) or a PAT stored in Key Vault; cluster: **existing interactive cluster** (single node) for the exercise |

## Datasets

| Dataset | Type | Parameters | Path / config | Serves |
|---|---|---|---|---|
| `ds_pg_table` | PostgreSQL table (on `ls_postgres_src`) | `schema`, `table` | one dataset serves all five source tables | 5 relational copy sources (primary) |
| `ds_adls_parquet_raw` | Parquet (`ls_adls`) | `entity` | container `raw`, folder `@{dataset().entity}/run_id=@{pipeline().RunId}` | 5 parquet sinks |
| `ds_adls_csv_inbox` | DelimitedText (`ls_adls`) | `folder`, `file` | container `inbox`, folder `@{dataset().folder}`, file `@{dataset().file}`; header: true; quote `"`; **escape char `"`** | 4 file copy sources |
| `ds_adls_csv_raw` | DelimitedText (`ls_adls`) | `entity` | container `raw`, folder `@{dataset().entity}/run_id=@{pipeline().RunId}`; header: true; quote `"`; escape `"` | 4 csv snapshot sinks |
| `ds_adls_parquet_dbextract` | Parquet (`ls_adls`) | `entity` | container `dbextract`, folder `@{dataset().entity}`, file `@{dataset().entity}.parquet` | **fallback** source for the 5 relational copies |

Escape char `"` (not the default `\`) preserves the RFC-4180
doubled-quote style the Olist files use.

## Activities

```
[cp_orders]         ─┐  (parquet dbextract → parquet raw)
[cp_customers]      ─┤
[cp_order_items]    ─┤
[cp_order_payments] ─┤
[cp_order_reviews]  ─┤  (all 9 parallel)
[cp_products]       ─┼──► on success (ALL) ──► [nb_transform]
[cp_sellers]        ─┤  (csv inbox → csv raw)
[cp_geolocation]    ─┤
[cp_translation]    ─┘
```

1. **Copies `cp_orders` / `cp_customers` / `cp_order_items` /
   `cp_order_payments` / `cp_order_reviews`** — 1:1 structural moves
   - Source: `ds_adls_parquet_dbextract`, `entity` = orders / customers /
     order_items / order_payments / order_reviews
   - Sink: `ds_adls_parquet_raw`, same `entity`
   - No mapping logic — transform lives in Spark
2. **Copies `cp_products` / `cp_sellers` / `cp_geolocation` /
   `cp_translation`** — snapshot the inbox files into the run-scoped raw
   folder so every run is reproducible against its own input
   - Source: `ds_adls_csv_inbox` with (`folder`, `file`) =
     (`products`, `olist_products_dataset.csv`) /
     (`sellers`, `olist_sellers_dataset.csv`) /
     (`geolocation`, `olist_geolocation_dataset.csv`) /
     (`product_category_translation`, `product_category_name_translation.csv`)
   - Sink: `ds_adls_csv_raw`, `entity` = products / sellers / geolocation /
     product_category_translation
3. **Databricks Notebook `nb_transform`**
   - Depends on **all nine** copies (success)
   - Notebook path: `/Shared/transform_olist` (with `olist_transforms`
     imported alongside it — the notebook `%run`s `./olist_transforms`)
   - Base parameters:
     - `run_id` = `@pipeline().RunId`
     - `raw_base_path` = `abfss://raw@<storageaccount>.dfs.core.windows.net`

Build tip: create `cp_orders` and `cp_products` first, verify both in a
Debug run, then clone each and edit only the dataset parameter values —
the remaining seven copies are ~2 minutes each.

## Trigger

Add a **schedule trigger** (daily, disabled or enabled briefly for a
screenshot) to demonstrate scheduling; note tumbling-window + retry policy
as the production choice in the README.

## Production evolution (documented, not built)

At higher table counts the nine hand-wired copies become two parameterised
**ForEach** loops driven by a config array
(`[{"entity":"orders","kind":"parquet"}, …]`) — same datasets, one copy
activity per loop. Hand-wired is deliberately kept here: the canvas
screenshot shows nine named sources converging on one notebook, and the
Monitor list shows per-copy row counts — better evidence and easier
first-build debugging than `@item()` expressions.

## Gotchas encountered / to expect

- Azure SQL firewall: enable *Allow Azure services* (for the Databricks
  JDBC write) or add the workspace NAT IPs.
- Databricks → ADLS access: the notebook sets the storage account key from
  the secret scope (`kv-olist/storage-key`) in its first cells; Unity
  Catalog external locations are the production-grade answer.
- The five Postgres copies REQUIRE the SHIR (source is on the laptop);
  the four csv copies and the geolocation ~61 MB move run cloud-to-cloud
  on the AutoResolve IR in seconds.
- If a Postgres copy fails with an SSL/handshake error, re-check
  **SSL mode: disable** on `ls_postgres_src` — the local container runs
  without TLS.
- Review comment text never passes through a CSV parser in the cloud: the
  multiline-quoted reviews file (99,224 records) is parsed by Postgres
  COPY at seed time (primary) or by the verified local extract builder
  (fallback); ADF then only ever moves typed parquet for that entity.
- The translation CSV carries a UTF-8 BOM; irrelevant to the copy (byte
  snapshot) and neutralized in Spark by the explicit-schema read.
