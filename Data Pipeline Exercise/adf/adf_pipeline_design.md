# ADF pipeline design — `pl_olist_ingest` (metadata-driven)

The pipeline is driven by ONE config file: `adf/config/entities.json`,
uploaded to the storage container `config/`. A Lookup reads it, two Filter
activities split it by source kind, and two ForEach loops run one
parameterised Copy activity each. Adding a tenth entity later = one line
of JSON + one Spark config entry — zero pipeline edits.

Build this in ADF Studio, then export the JSON (Manage → ARM template, or
connect the factory to a Git repo) and commit it under `adf/` as the
implementation artifact.

> **Relational-source note:** the five `postgres`-kind entities are copied
> from a real PostgreSQL 16 — the SQL Exercise's local Docker container
> (`qvc_sql_exercise`, host port 5432, database `olist`), seeded by
> `postgres_source/postgres_seed.sql`. Because that database lives on the
> laptop, ADF reaches it through a **Self-Hosted Integration Runtime** —
> the same pattern used for on-prem Oracle sources in production
> (Oracle→Postgres substitution noted per the brief; Azure Database for
> PostgreSQL could not be provisioned in this subscription).
>
> **Verified fallback** (if the SHIR path is blocked): typed parquet
> extracts of the same five tables, built by
> `local_test/build_dbextract_parquet.py`, uploaded to a `dbextract`
> container. The only change is `cp_pg_entity`'s source dataset:
> `ds_pg_table` → `ds_adls_parquet_dbextract` with
> `entity = @item().entity` — sink, raw layout and notebook are identical.
>
> **No Azure Key Vault, no role assignments:** this subscription does not
> allow granting Key Vault access (policy or RBAC) or assigning ANY Azure
> role to ANY principal — not the factory's managed identity, not the
> `AzureDatabricks` app, nothing. So there is deliberately no `ls_kv`
> linked service and no managed-identity auth anywhere in this design.
> Every secret is entered as a plain **secure string** directly into the
> consumer that needs it (ADF linked-service fields are encrypted at rest
> by the factory itself; Databricks secrets live in a **Databricks-native**
> secret scope, `olist-secrets`, created via CLI + PAT — see
> `SETUP_GUIDE.md` Phase 4). Both mechanisms need zero Azure RBAC/Graph
> permission. Production would use Key Vault + managed identity throughout
> once the subscription allows it (see README).

## The config file (single source of truth)

Upload `adf/config/entities.json` to `config/entities.json` in the
storage account:

```json
[
  { "entity": "orders",         "kind": "postgres", "schema": "src", "table": "orders" },
  { "entity": "customers",      "kind": "postgres", "schema": "src", "table": "customers" },
  { "entity": "order_items",    "kind": "postgres", "schema": "src", "table": "order_items" },
  { "entity": "order_payments", "kind": "postgres", "schema": "src", "table": "order_payments" },
  { "entity": "order_reviews",  "kind": "postgres", "schema": "src", "table": "order_reviews" },
  { "entity": "products",       "kind": "csv", "folder": "products",    "file": "olist_products_dataset.csv" },
  { "entity": "sellers",        "kind": "csv", "folder": "sellers",     "file": "olist_sellers_dataset.csv" },
  { "entity": "geolocation",    "kind": "csv", "folder": "geolocation", "file": "olist_geolocation_dataset.csv" },
  { "entity": "product_category_translation", "kind": "csv", "folder": "product_category_translation", "file": "product_category_name_translation.csv" }
]
```

`entity` must match both the `raw/<entity>/` folder name and the
`ENTITY_CONFIG` key in the Spark module — it is the join key of the whole
system. `kind` decides which loop picks the row up; `schema`/`table`
apply to postgres rows, `folder`/`file` to csv rows.

## Integration runtimes

- **AutoResolveIntegrationRuntime** — all ADLS↔ADLS work and the
  Databricks activity.
- **`shir-laptop`** (Self-Hosted) — Manage → Integration runtimes → New →
  Self-Hosted → install the MSI on this machine and register it with the
  key ADF shows. Required because the Postgres source is local to the
  laptop. The laptop, Docker Desktop and the `qvc_sql_exercise` container
  must all be running during a pipeline run.

## Linked services

None of these need a role assignment — each authenticates with a secret
typed directly into the linked service (Authentication method / type
selector on each), which ADF encrypts internally. This is deliberate: see
the "No Azure Key Vault, no role assignments" note above.

| Name                | Type                         | Notes                                                                                                                                                                                                                                                |
| ------------------- | ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ls_adls`         | Azure Data Lake Storage Gen2 | **Authentication method: Account key**; storage account name + key1 (Storage account → Access keys) entered directly — the same key value used for the Databricks `storage-key` secret. No role assignment (reading your own account's key is not a role grant). |
| `ls_postgres_src` | PostgreSQL (V2 connector)    | **connectVia: `shir-laptop`**; host `localhost`, port `5432`, database `olist`, user `qvc`, **password entered directly** (`qvc` — the Docker container's password) as a secure string; **SSL mode: disable** (the local container runs without TLS — noted as exercise-grade) |
| `ls_databricks`   | Azure Databricks             | **Authentication type: Access Token** (not Managed Service Identity); paste a Databricks **personal access token** (same one generated for the CLI in SETUP_GUIDE Phase 4) directly as a secure string; cluster: **existing interactive cluster** (single node), referenced by its cluster ID |

## Datasets

`run_id` is a dataset *parameter* (not `@pipeline().RunId` inside the
dataset) because pipeline system variables are not available in dataset
definitions — the Copy activities pass it in.

| Dataset                       | Type                                   | Parameters             | Path / config                                                                                                                    | Serves                                         |
| ----------------------------- | -------------------------------------- | ---------------------- | -------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------- |
| `ds_json_config`            | JSON (`ls_adls`)                     | —                     | container`config`, file `entities.json`                                                                                      | the Lookup                                     |
| `ds_pg_table`               | PostgreSQL table (`ls_postgres_src`) | `schema`, `table`  | table =`@{dataset().schema}.@{dataset().table}`                                                                                | source of`cp_pg_entity`                      |
| `ds_adls_parquet_raw`       | Parquet (`ls_adls`)                  | `entity`, `run_id` | *as designed:* container`raw`, folder `@{dataset().entity}/run_id=@{dataset().run_id}` — **as actually built here: container `olistdata`, static folder `raw/parquet`, `run_id`+`entity` went into the file name instead — see "Deviation" below*  | sink of`cp_pg_entity`                        |
| `ds_adls_csv_inbox`         | DelimitedText (`ls_adls`)            | `folder`, `file`   | container`inbox`, folder `@{dataset().folder}`, file `@{dataset().file}`; header true; quote `"`; **escape `"`** — *as actually built: container `olistdata`, folder `inbox/@{dataset().folder}`* | source of`cp_csv_entity`                     |
| `ds_adls_csv_raw`           | DelimitedText (`ls_adls`)            | `entity`, `run_id` | *as designed:* container`raw`, folder `@{dataset().entity}/run_id=@{dataset().run_id}` — **as actually built: container `olistdata`, folder `raw/csv/@{dataset().entity}`, no `run_id` in the path — see "Deviation" below**; header true; quote `"`; escape `"`              | sink of`cp_csv_entity`                       |
| `ds_adls_parquet_dbextract` | Parquet (`ls_adls`)                  | `entity`             | container`dbextract`, folder `@{dataset().entity}`, file `@{dataset().entity}.parquet`                                     | **fallback** source for `cp_pg_entity` |

Escape char `"` (not the default `\`) preserves the RFC-4180
doubled-quote style the Olist files use.

## Pipeline canvas

```
                     ┌─► [flt_pg]  ─► [fe_pg:  ⟳ cp_pg_entity ]  ─┐
[lkp_entities] ──────┤                                            ├─► [nb_transform]
                     └─► [flt_csv] ─► [fe_csv: ⟳ cp_csv_entity]  ─┘
```

## Activities — exact settings

| Activity                                    | Setting                  | Value                                                                                                               |
| ------------------------------------------- | ------------------------ | ------------------------------------------------------------------------------------------------------------------- |
| `lkp_entities` (Lookup)                   | Source dataset           | `ds_json_config`                                                                                                  |
|                                             | **First row only** | **OFF** (returns the whole array)                                                                             |
| `flt_pg` (Filter)                         | Items                    | `@activity('lkp_entities').output.value`                                                                          |
|                                             | Condition                | `@equals(item().kind, 'postgres')`                                                                                |
| `flt_csv` (Filter)                        | Items                    | `@activity('lkp_entities').output.value`                                                                          |
|                                             | Condition                | `@equals(item().kind, 'csv')`                                                                                     |
| `fe_pg` (ForEach)                         | Items                    | `@activity('flt_pg').output.Value`  ← capital **V**                                                        |
|                                             | Sequential               | off (parallel); Batch count 5                                                                                       |
| `cp_pg_entity` (Copy, inside `fe_pg`)   | Source dataset           | `ds_pg_table` — `schema` = `@item().schema`, `table` = `@item().table`                                   |
|                                             | Sink dataset             | `ds_adls_parquet_raw` — `entity` = `@item().entity`, `run_id` = `@pipeline().RunId`                      |
|                                             | Mapping                  | none — 1:1 structural copy (transform lives in Spark)                                                              |
| `fe_csv` (ForEach)                        | Items                    | `@activity('flt_csv').output.Value`                                                                               |
|                                             | Sequential               | off; Batch count 4                                                                                                  |
| `cp_csv_entity` (Copy, inside `fe_csv`) | Source dataset           | `ds_adls_csv_inbox` — `folder` = `@item().folder`, `file` = `@item().file`                               |
|                                             | Sink dataset             | `ds_adls_csv_raw` — `entity` = `@item().entity`, `run_id` = `@pipeline().RunId`                          |
| `nb_transform` (Databricks Notebook)      | Depends on               | `fe_pg` success **AND** `fe_csv` success                                                                  |
|                                             | Notebook path            | `/Shared/transform_olist` (`olist_transforms` imported alongside — the shell `%run`s `./olist_transforms`) |
|                                             | Base parameters          | `run_id` = `@pipeline().RunId`; `raw_base_path` = `abfss://raw@<storageaccount>.dfs.core.windows.net` — *as actually built: `abfss://olistdata@<storageaccount>.dfs.core.windows.net/raw`*       |

Build tip: before wiring the full graph, Debug once with a temporary
2-item `entities.json` (one postgres row + one csv row — e.g. customers +
product_category_translation) to shake out expressions cheaply, then
upload the full 9-item file. Expression typos inside ForEach fail only at
runtime, so debug small.

## Trigger

Add a **schedule trigger** (daily, disabled or enabled briefly for a
screenshot) to demonstrate scheduling; note tumbling-window + retry policy
as the production choice in the README.

## Why metadata-driven (and the trade-off)

- **Chosen:** the entity list is data, not pipeline topology — new tables
  are config edits; the pipeline definition never changes and neither do
  its tests/exports. This mirrors how multi-source ingestion frameworks
  are built in production.
- **Trade-off:** the canvas shows the *pattern*, not nine named boxes, and
  inner-activity runs must be found in Monitor's activity list rather than
  on the canvas. Monitor still records **one Copy run per iteration with
  full row counts** — click a `cp_pg_entity` run and its input shows which
  entity it processed. Evidence stays intact.
- **Alternative rejected:** nine hand-wired Copy activities — fastest to
  author and the friendliest canvas screenshot, but every new table is a
  pipeline edit, and nine near-identical activities is repetition the
  config file exists to remove.

## Deviation actually observed in this build

The build in this project used a single `olistdata` container (folder
`raw/` inside it, rather than a separate `raw` container) — fine, purely
cosmetic. Less fine: the two sink datasets ended up parameterised
**inconsistently**. `ds_adls_csv_raw`'s Directory correctly includes
`@{dataset().entity}` (→ `raw/csv/<entity>/`), but its file name was left
to ADF's auto-generated default with no `run_id` anywhere in the path.
`ds_adls_parquet_raw` went the other way — its Directory is a **static**
`raw/parquet` with `@{dataset().entity}` and `@{dataset().run_id}`
concatenated into the **file name** field instead
(`orders083ead2f-75c9-4bf8-8a76-80da2c1bf691`, no extension), so every
entity's parquet lands flat in the same folder.

The notebook (`read_raw()` in `transform_olist.py`) was adapted to read
both shapes as-built — see the "as actually built" tree in
`SETUP_GUIDE.md`'s ADLS appendix — rather than requiring a pipeline
rebuild. Net effect: the parquet side is still run-scoped (RunId is in
the filename) but the csv side is **not** — a second run risks doubling
csv-sourced entity rows unless the folder is cleared first. **Durable
fix**, if there's time before a second run: edit `ds_adls_csv_raw`'s
Directory to `raw/csv/@{dataset().entity}/run_id=@{dataset().run_id}`
(add a `run_id` dataset parameter, wire `cp_csv_entity`'s sink to pass
`run_id = @pipeline().RunId`, matching how `ds_adls_parquet_raw` was
*supposed* to work), then update `read_raw`'s csv branch back to a
run_id-scoped path.

## Gotchas encountered / to expect

- Filter output is referenced as `.output.Value` with a **capital V** —
  lowercase `.value` works for Lookup but not Filter; this is the classic
  silent-empty-loop mistake.
- Lookup **First row only** must be OFF or the ForEach receives a single
  object and iterates its properties.
- ADF forbids ForEach-inside-ForEach; the Filter + two-loop shape is the
  standard answer for two source types.
- The five postgres iterations REQUIRE the SHIR (source is on the
  laptop); the csv iterations and the ~61 MB geolocation copy run
  cloud-to-cloud on AutoResolve in seconds.
- If a postgres copy fails with an SSL/handshake error, re-check
  **SSL mode: disable** on `ls_postgres_src` — the local container runs
  without TLS.
- Azure SQL firewall: enable *Allow Azure services* (for the Databricks
  JDBC write) or add the workspace NAT IPs.
- Review comment text never passes through a CSV parser in the cloud: the
  multiline-quoted reviews file (99,224 records) is parsed by Postgres
  COPY at seed time (primary) or by the verified local extract builder
  (fallback); ADF only ever moves typed parquet for that entity.
- The translation CSV carries a UTF-8 BOM; irrelevant to the copy (byte
  snapshot) and neutralized in Spark by the explicit-schema read.
- Databricks → ADLS access: the notebook sets the storage account key from
  the secret scope (`olist-secrets/storage-key`); Unity Catalog external
  locations are the production-grade answer.
