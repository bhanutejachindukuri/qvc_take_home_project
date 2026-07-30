# ADF pipeline design — `pl_ol_ingest_onprem_to_adls` (metadata-driven)

> Verified against the actual exported pipeline JSON
> (`adf/pl_ol_ingest_onprem_to_adls.json`) — activity names, dataset
> parameters and the notebook path below are the real, as-built values,
> not the originally planned ones. Differences from the original design
> are called out explicitly where they matter.

The pipeline is driven by ONE config file: `adf/config/entities.json`,
uploaded to the storage container `config/`. A Lookup reads it, two Filter
activities split it by source kind, and two ForEach loops run one
parameterised Copy activity each. Adding a tenth entity later = one line
of JSON + one Spark config entry — zero pipeline edits.

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
> container. The only change is `Copy_onprem_to_adls_raw`'s source
> dataset: `ds_pg_table` → `ds_adls_parquet_dbextract` with
> `entity = @item().entity` — sink, raw layout and notebook are identical.
>
> **No Azure Key Vault, no role assignments:** this subscription does not
> allow granting Key Vault access (policy or RBAC) or assigning ANY Azure
> role to ANY principal — not the factory's managed identity, not the
> `AzureDatabricks` app, nothing. So there is deliberately no Key Vault
> linked service and no managed-identity auth anywhere in this design.
> Every secret is entered as a plain **secure string** directly into the
> consumer that needs it (ADF linked-service fields are encrypted at rest
> by the factory itself; Databricks secrets live in a **Databricks-native**
> secret scope, `olist-secrets`, created via CLI + PAT — see
> `SETUP_GUIDE.md` Phase 4; confirmed by `Ls_Data_bricks`'s Access Token
> auth in the real export below). Both mechanisms need zero Azure
> RBAC/Graph permission. Production would use Key Vault + managed identity
> throughout once the subscription allows it (see README).

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

`entity` must match both the raw-zone naming and the `ENTITY_CONFIG` key
in the Spark module — it is the join key of the whole system. `kind`
decides which loop picks the row up; `schema`/`table` apply to postgres
rows, `folder`/`file` to csv rows.

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
typed directly into the linked service, which ADF encrypts internally.

| Name | Type | Notes |
|---|---|---|
| `ls_adls` | Azure Data Lake Storage Gen2 | **Authentication method: Account key**; storage account `datasource4dbs`, key1 entered directly — the same key value used for the Databricks `storage-key` secret. No role assignment. |
| `ls_postgres_src` | PostgreSQL (V2 connector) | **connectVia: `shir-laptop`**; host `localhost`, port `5432`, database `olist`, user `qvc`, password entered directly as a secure string; **SSL mode: disable**. |
| `Ls_Data_bricks` | Azure Databricks | **Authentication type: Access Token** (confirmed — not Managed Service Identity); Databricks personal access token entered directly as a secure string; cluster: existing interactive cluster. |

## Datasets

Confirmed from the real pipeline export. Two datasets ended up doing
double duty in ways the original design didn't anticipate — see the notes
column.

| Dataset | Type | Real parameters | Path / config | Serves |
|---|---|---|---|---|
| `Ls_adls_json` | JSON (`ls_adls`) | — | `config/entities.json` | source of `Lookup_entity_config`. Named with an `Ls_` prefix despite being a **dataset**, not a linked service — a naming-convention quirk, not a functional issue. |
| `ds_pg_table` | PostgreSQL table (`ls_postgres_src`) | `schema`, `table` | table = `@{dataset().schema}.@{dataset().table}` | source of `Copy_onprem_to_adls_raw` |
| `ds_adls_parquet_raw` | Parquet (`ls_adls`) | `folder`, `file_name` (**not** `entity`/`run_id` as originally designed) | `folder` = `@concat('raw','/','parquet')` — a **static** value, same for every entity, no per-entity subfolder; `file_name` = `@concat(item().entity, pipeline().RunId)` — entity and RunId concatenated with no separator and no extension, computed at the Copy activity, not baked into the dataset itself | sink of `Copy_onprem_to_adls_raw`. Real path: `raw/parquet/<entity><RunId>` (e.g. `raw/parquet/orders083ead2f-75c9-4bf8-8a76-80da2c1bf691`) |
| `ds_adls_csv_inbox` | DelimitedText (`ls_adls`) | `filesytem` (sic — real parameter name has this typo), `folder`, `file` | Reused for **both** source and sink of the same Copy — see below | source **and** sink of `Copy_Csv_to_raw` (the design originally planned a second dataset, `ds_adls_csv_raw`, for the sink; the real build reuses this one dataset with different parameter values instead) |
| `ds_adls_parquet_dbextract` | Parquet (`ls_adls`) | `entity` | `dbextract/@{dataset().entity}/@{dataset().entity}.parquet` | **fallback** source for `Copy_onprem_to_adls_raw` — designed, not present in the current build (Postgres/SHIR path is live) |

**`ds_adls_csv_inbox`'s two roles, exact real values:**

| Role | `filesytem` | `folder` | `file` |
|---|---|---|---|
| Source (read from inbox) | `"olistdata"` (literal) | `@concat('inbox','/',item().folder)` | `@item().file` |
| Sink (write to raw) | `"olistdata"` (literal) | `@concat('raw','/','csv','/',item().entity)` | `@pipeline().RunId` |

The sink's format settings add `fileExtension: ".txt"` and
`quoteAllText: true` — so the real landed path is
**`raw/csv/<entity>/<RunId>.txt`**, a fully-quoted delimited file. This
matters: it means the csv side **is** run-scoped after all (the filename
is deterministically the RunId) — the notebook's `read_raw()` now reads
that exact file rather than the whole entity folder (see "Fix applied"
below); earlier design notes describing the csv side as unscoped were
based on an incomplete read of the storage browser and have been
corrected here against the real pipeline definition.

## Pipeline canvas (real activity names)

```
                                  ┌─► [Filter_postgresql] ─► [ForEachPostgreSql: ⟳ Copy_onprem_to_adls_raw] ─┐
[Lookup_entity_config] ──────────┤                                                                            ├─► [Notebook_Transform_Load]
                                  └─► [Filter_csv_src]     ─► [ForEachCsv:        ⟳ Copy_Csv_to_raw]        ─┘
```

## Activities — exact settings (as built)

| Activity | Setting | Value |
|---|---|---|
| `Lookup_entity_config` (Lookup) | Source dataset | `Ls_adls_json` |
| | **First row only** | **OFF** (`firstRowOnly: false` — returns the whole array) |
| `Filter_postgresql` (Filter) | Items | `@activity('Lookup_entity_config').output.value` |
| | Condition | `@equals(item().kind, 'postgres')` |
| `Filter_csv_src` (Filter) | Items | `@activity('Lookup_entity_config').output.value` |
| | Condition | `@equals(item().kind, 'csv')` |
| `ForEachPostgreSql` (ForEach) | Items | `@activity('Filter_postgresql').output.value` — **lowercase `value`, confirmed working** (see Gotchas) |
| | Sequential | `false` (parallel) |
| `Copy_onprem_to_adls_raw` (Copy, inside `ForEachPostgreSql`) | Source | `ds_pg_table` — `schema` = `@item().schema`, `table` = `@item().table` (`PostgreSqlV2Source`) |
| | Sink | `ds_adls_parquet_raw` — `folder` = `@concat('raw','/','parquet')`, `file_name` = `@concat(item().entity,pipeline().RunId)` (`ParquetSink`) |
| | Mapping | No explicit column mapping — `TabularTranslator` with `typeConversion: true` still applies ADF's automatic relational→Parquet type mapping, which is unavoidable moving from a typed relational source into a typed columnar sink; this is not a hand-authored mapping |
| `ForEachCsv` (ForEach) | Items | `@activity('Filter_csv_src').output.value` |
| `Copy_Csv_to_raw` (Copy, inside `ForEachCsv`) | Source | `ds_adls_csv_inbox` — `filesytem` = `olistdata`, `folder` = `@concat('inbox','/',item().folder)`, `file` = `@item().file` (`DelimitedTextSource`) |
| | Sink | `ds_adls_csv_inbox` (same dataset, reused) — `filesytem` = `olistdata`, `folder` = `@concat('raw','/','csv','/',item().entity)`, `file` = `@pipeline().RunId`; format: `quoteAllText: true`, `fileExtension: ".txt"` (`DelimitedTextSink`) |
| `Notebook_Transform_Load` (Databricks Notebook) | Depends on | `ForEachPostgreSql` success **AND** `ForEachCsv` success |
| | Linked service | `Ls_Data_bricks` |
| | Notebook path | `/Users/bhanutejachindukuri@gmail.com/Qvc_data_engineer_problem/transform_olist` — a personal workspace folder, not `/Shared/`; `olist_transforms` must be imported into that **same** folder (the shell `%run`s `./olist_transforms`, a relative path) |
| | Base parameters | `run_id` = `@pipeline().RunId` (expression); `raw_base_path` = `abfss://olistdata@datasource4dbs.dfs.core.windows.net/raw` (plain string). **No `only_entity` parameter** — confirmed absent, correct for a full 9-entity run. |

Build tip: before wiring the full graph, Debug once with a temporary
2-item `entities.json` (one postgres row + one csv row) to shake out
expressions cheaply, then upload the full 9-item file. Expression typos
inside ForEach fail only at runtime, so debug small.

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
  full row counts** — click a `Copy_onprem_to_adls_raw` run and its input
  shows which entity it processed. Evidence stays intact.
- **Alternative rejected:** nine hand-wired Copy activities — fastest to
  author and the friendliest canvas screenshot, but every new table is a
  pipeline edit, and nine near-identical activities is repetition the
  config file exists to remove.

## Fix applied: csv reads are now run-scoped

Earlier notes (based on the storage browser alone) described the csv
side as not run-scoped, risking double-counted rows on a second run. The
real pipeline JSON shows this isn't actually true: the csv sink's `file`
parameter is `@pipeline().RunId` — deterministic, just like the parquet
side's filename. `read_raw()` in `transform_olist.py` now reads the exact
file `raw/csv/<entity>/<RunId>.txt` (glob-matched by RunId prefix, same
robustness pattern as the parquet branch) instead of the whole entity
folder, so a second run reading a different RunId's file can no longer
double-count. One consequence: the Phase 4 smoke-test upload must now be
**named to match the `run_id` widget value** — see `SETUP_GUIDE.md`.

## Gotchas encountered / to expect

- **Correction from earlier guidance:** the real, executed pipeline uses
  lowercase `.output.value` to reference **both** Lookup's and Filter's
  output — not `.output.Value` with a capital V as earlier notes here
  claimed. The Debug/Monitor output pane displays Filter's result fields
  capitalized (`ItemsCount` / `FilteredItemsCount` / `Value`) for
  readability, but that's a display convention, not what the expression
  needs — `.output.value` (lowercase) is what the working pipeline
  actually uses. Trust this over the capital-V claim if the two conflict.
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
