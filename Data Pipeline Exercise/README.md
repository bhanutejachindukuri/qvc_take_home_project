# Cloud data pipeline — Olist e-commerce (ADF + Databricks + Azure SQL)

An ELT pipeline over the complete Kaggle **Olist Brazilian e-commerce
dataset** (all 9 files, ~1.55M source rows): ingests from two source types —
CSV files in cloud object storage and relational tables in PostgreSQL —
transforms with PySpark on Azure Databricks, and loads typed, query-ready
tables into Azure SQL Database, with per-entity process logging,
reconciliation and data-quality flags built in. The full real dataset was
reviewed locally before the first cloud run, and the design — the DQ
flags, dedup keys and geolocation aggregation — reflects what that
review turned up.

> **Note on the relational source (per the brief: "if you substitute
> Oracle, mention it clearly"):** Oracle was substituted with
> **PostgreSQL 16**. Azure Database for PostgreSQL could not be provisioned
> in the available subscription, so the source is a local Docker Postgres
> reached by ADF through a **Self-Hosted Integration Runtime** — the same
> connector pattern used for on-prem Oracle sources in production
> pipelines, just pointed at Postgres. A verified fallback (typed parquet
> extracts of the same five tables, copied by ADF from a `dbextract`
> container) is included and swaps in with a single source-dataset change;
> everything downstream is identical either way.

## Architecture

Visual diagram: [architecture.jpg](architecture.jpg). Reflects the
actual as-built architecture, including
both real deviations from the original design (Postgres via a
Self-Hosted Integration Runtime instead of Azure Database for PostgreSQL;
secrets as plain secure strings / a native Databricks scope instead of
Key Vault — see Assumptions & trade-offs for why).

```
PostgreSQL 16 (Docker, on-prem-style)        ADLS Gen2 inbox container
src.orders · src.customers · src.order_items   products · sellers · geolocation
src.order_payments · src.order_reviews         · category translation (CSV)
        │  via Self-Hosted                            │
        │  Integration Runtime                        │
        └───────────► Azure Data Factory ◄────────────┘
        metadata-driven: Lookup(config/entities.json) →
        Filter ×2 → ForEach ×2 → 9 parameterised copies per run
                          │
        ADLS Gen2 raw zone — raw/<entity>/run_id=<ADF RunId>/
        (typed parquet for the 5 relational entities, csv snapshots
         for the 4 file entities)
                          │
        Azure Databricks — transform_olist + olist_transforms
        config-driven: rename · cast · null handling · dedup ·
        DQ flags · ingestion metadata · per-entity process logging
        + bespoke geolocation aggregation (1,000,163 pts → 19,015 zips)
                          │  (JDBC, truncate-load)
        Azure SQL Database (serverless)
        ├── curated schema  → 9 typed tables
        ├── etl schema      → pipeline_process_log (reconciliation)
        └── mart schema     → v_sales_overview · v_payment_mix ·
                              v_review_delivery
```

Design principles carried over from the layered enterprise DWH pattern:

- **Dumb ingestion, smart transformation.** Copy activities do 1:1
  structural moves only; a transform bug never forces re-extraction.
- **Run-scoped raw zone.** Every run lands under
  `raw/<entity>/run_id=<guid>` — each run is reproducible against its
  exact input.
- **Flag, don't drop.** Invalid payment types and impossible delivery
  dates are kept and flagged; legitimate business nulls (undelivered
  orders) are preserved and documented.
- **Operational metadata everywhere.** Every curated row carries
  `ingestion_timestamp` + `pipeline_run_id`; every entity load writes
  read/written/flagged counts to `etl.pipeline_process_log`.
- **Verify before you spend.** The whole transform layer was run locally
  against the real files first; the cloud phases only had to prove the
  seams (auth, copies, JDBC).

## Services used

| Service | Role | Why this one |
|---|---|---|
| Azure Data Factory | Orchestration + ingestion | Required-stack fit; Copy activity covers both source connectors natively; the SHIR demonstrates the on-prem-source pattern; dependency graph + Monitor give free observability |
| Self-Hosted Integration Runtime | Bridge to the local Postgres | The relational source lives outside Azure — exactly how on-prem Oracle is reached in production ADF setups |
| ADLS Gen2 | Landing zones (`inbox`, `raw`) | Decouples extraction from transformation; parquet as the typed intermediate |
| Azure Databricks (PySpark) | Transformation | Set-based, testable transforms; config-driven entity pipeline + window-function dedup are natural in Spark; the pure-module split makes the logic locally testable |
| Azure SQL Database (serverless) | Analytical target | Queryable SQL target required; auto-pause keeps cost near zero; Synapse dedicated pools disproportionate for ~570k curated rows |
| Databricks-native secret scope (`olist-secrets`) | Secrets | Not Key Vault — this subscription blocks all Azure role assignments, including the ones Key Vault-backed scopes and managed-identity auth need. Secrets live in Databricks' own store (CLI + PAT) instead; ADF's secrets are typed directly into each linked service. See Assumptions & trade-offs |

### Alternatives considered

| Option | Verdict |
|---|---|
| ADF Mapping Data Flows | Rejected: Spark-cluster spin-up cost/latency for renames+casts; logic buried in ADF JSON is hard to review |
| T-SQL stored procedures (pure ELT in target) | Strong option, deliberately not chosen — demonstrates Spark-based transformation; sprocs would win if the target owned all compute |
| dbt on the target | Best long-term home for transform logic (tests, lineage, docs); out of proportion for this scope — noted as the production evolution |
| 9 explicit hand-wired Copy activities | Rejected: fastest to author and the friendliest canvas, but every new table is a pipeline edit; the metadata-driven Lookup → Filter → ForEach design keeps the entity list as config (`adf/config/entities.json`) while Monitor still records per-iteration row counts |
| Full 1M-row geolocation load to SQL | Rejected — see the judgement call below |

## Data model & transformations

Row counts below reflect the real files, confirmed during the local
review before the cloud run.

| Entity | Source | Rows in → curated | Key transformations |
|---|---|---|---|
| orders | Postgres | 99,441 → 99,441 | 5 timestamp casts, lower-cased status, `is_valid_delivery_date` flag (0 flagged — a contract check), dedup on `order_id` |
| customers | Postgres | 99,441 → 99,441 | `INITCAP` city, upper state, dedup; `customer_unique_id` documented as the person-level key (`customer_id` is per-order) |
| order_items | Postgres | 112,650 → 112,650 | int/decimal/timestamp casts, dedup on (`order_id`,`order_item_id`) |
| order_payments | Postgres | 103,886 → 103,886 | casts, lower-cased type, `is_valid_payment` flag — catches exactly **3** `not_defined` rows, kept not dropped |
| order_reviews | Postgres | 99,224 → 99,224 | multiline-quoted CSV parsed at seed time (99,224 records, not the naive ~104.7k line count); `review_id` alone is NOT unique (789 duplicates) → key is (`review_id`,`order_id`); score-domain flag (0 flagged) |
| products | inbox CSV | 32,951 → 32,951 | rename `*_lenght` typo columns, 7 int casts, `COALESCE(category,'unknown')` (**610** nulls), dedup |
| sellers | inbox CSV | 3,095 → 3,095 | `INITCAP` city, upper state, dedup |
| geolocation | inbox CSV | 1,000,163 → **19,015** | zip-prefix-grain dimension (below) |
| category translation | inbox CSV | 71 → 71 | UTF-8 BOM neutralized by explicit-schema read; 71 rows (naive line counts say 70 — no trailing newline); 2 categories in products have no translation (`pc_gamer`, `portateis_cozinha_e_preparadores_de_alimentos`) → mart falls back to the Portuguese name |

### The geolocation judgement call

The raw file is a 1,000,163-row point cloud with no key — many rows per zip
prefix. Its only consumers (`customers` / `sellers`) join at **zip-prefix
grain**, and a 1M-row JDBC truncate-load into min-capacity serverless SQL
costs minutes-to-tens-of-minutes per run for a table nothing can join at
that grain. So: ADF still lands all 1M rows in raw (dumb ingestion,
full fidelity retained), and the curated table is a **19,015-row zip
dimension** — centroid lat/lng averaged over points inside a Brazil
bounding box (29 out-of-bounds points excluded from centroids but counted
in `invalid_point_count`; 4 zips whose points are all invalid kept with
NULL coordinates), modal city/state, `point_count` per zip. The process
log shows `rows_read=1,000,163 / rows_written=19,015` — a deliberate,
documented grain change, not a reconciliation failure.

### Consumption layer (mart)

- `mart.v_sales_overview` — orders, GMV and avg delivery days by state ×
  **English** category name (translation LEFT JOIN with Portuguese
  fallback); delivered + DQ-valid orders only.
- `mart.v_payment_mix` — order coverage, value share and instalment
  behaviour per payment type (credit_card 76,795 / boleto 19,784 /
  voucher 5,775 / debit_card 1,529 payments; the 3 `not_defined` rows are
  excluded here, visible in the DQ evidence).
- `mart.v_review_delivery` — avg delivery days and %-late per review score
  (1★ 11,424 · 2★ 3,151 · 3★ 8,179 · 4★ 19,142 · 5★ 57,328) — makes the
  late-delivery→bad-review story queryable.

## Setup and run

See [SETUP_GUIDE.md](SETUP_GUIDE.md) for the phased build with
checkpoints. Short version:

0. Real files reviewed locally first, to design the transforms around
   what the data actually looks like rather than the brief's assumptions.
1. Install the SHIR; verify storage HNS; create containers; SQL firewall;
   copy the storage account key (no Key Vault — see Assumptions).
2. Seed sources: 5 `\copy` loads into Docker Postgres
   (`postgres_source/postgres_seed.sql`) + 4 CSVs to `inbox/`.
3. Run `sql/01_azure_sql_ddl.sql` on Azure SQL.
4. Import `databricks/*.ipynb` to a workspace folder (any folder — the
   real build used a personal `/Users/...` folder, not `/Shared`);
   CLI-create the native secret
   scope `olist-secrets` (5 secrets); 71-row smoke run (`only_entity`
   widget).
5. Build `pl_olist_ingest` per `adf/adf_pipeline_design.md`; Debug run.
6. Evidence: `sql/02_evidence_queries.sql` + screenshots into `evidence/`.

## Assumptions & trade-offs

- **Local Docker Postgres behind a SHIR as the relational source** — Azure
  PG was not provisionable in this subscription; the SHIR pattern is the
  honest equivalent of production on-prem sources, at the cost of the
  laptop needing to be up during runs. The **verified parquet-extract
  fallback** removes even that risk for re-runs.
- **Full reload (truncate-load) per run** — appropriate for the volume and
  scope; incremental is designed but not implemented (below). JDBC
  `truncate=true` preserves the typed DDL contract instead of letting
  Spark re-infer tables.
- **Failure isolation per entity** — one failing entity is logged FAILED
  in the process log, the other eight still load, and the run is failed at
  the end so Monitor shows red.
- **Actual landed raw-zone layout deviates from the original design** —
  verified against the exported pipeline JSON
  (`adf/pl_ol_ingest_onprem_to_adls.json`), not just inferred: csv sinks
  use an entity-named folder with a RunId-named file inside
  (`raw/csv/<entity>/<RunId>.txt`); parquet sinks are flat, with
  entity+RunId concatenated into the file name instead of a folder
  (`raw/parquet/<entity><RunId>`). Both are fully run-scoped despite the
  shape difference. `read_raw()` reads the exact file for the current run
  in both cases via a shared `find_run_file()` glob-by-prefix helper — no
  risk of a second run double-counting a stale file. Full detail in
  `adf/adf_pipeline_design.md` ("Fix applied: csv reads are now
  run-scoped") and `SETUP_GUIDE.md`'s ADLS appendix.
- **No Azure Key Vault, no role assignments anywhere** — this subscription
  blocks granting Key Vault access (policy or RBAC) and assigning any
  Azure role to any principal, which also rules out managed-identity auth
  for `ls_adls` and `ls_databricks`. Every secret is instead a secure
  string typed directly where it's needed: ADF linked-service fields
  (encrypted internally by the factory) for the storage account key and
  Postgres password; a **Databricks-native** secret scope (`olist-secrets`,
  created via CLI + personal access token, not Key Vault) for the storage
  key and Azure SQL credentials the notebook reads. Functionally
  equivalent for this exercise; production would use Key Vault + managed
  identity throughout once the subscription allows the grants. Full detail
  in `adf/adf_pipeline_design.md`'s "No Azure Key Vault" note.
- **Storage-key ADLS auth from both ADF and Databricks** for the same
  reason above; production answer is Unity Catalog external locations /
  service principal. SSL disabled on the local Postgres link (container
  has no TLS) — exercise-grade, called out in the ADF doc.
- **Single-node smallest Databricks cluster, 15-min auto-terminate** —
  right-sized (~1.55M rows total); the transforms are partition-agnostic
  and scale to a multi-node cluster unchanged.
- **Local review instead of unit tests** — within the timebox, going
  through the full dataset by hand and building the DQ flags/dedup keys
  around what that review surfaced, plus the process-log reconciliation,
  gives stronger evidence than a couple of token unit tests would;
  keeping the transform functions free of dbutils/JDBC calls is what
  makes that review practical, and is also the seam where pytest would
  attach in a production repo.

## Productionising for a daily/monthly schedule

1. **Incremental extraction:** watermark control table
   (`etl.load_watermarks`); ADF Lookup reads the high-water mark, the
   Postgres copy's query becomes `WHERE updated_at > @watermark`;
   CDC/Debezium if the source gains change tracking.
2. **Merge instead of truncate-load:** stage + keyed `MERGE` into curated;
   opens the door to SCD2 on customers/sellers.
3. **Reference data cadence:** the geolocation dimension and category
   translation are slowly changing — refresh monthly, not per run.
4. **Trigger & recovery:** tumbling-window trigger with retry instead of a
   schedule trigger; idempotency already guaranteed by run-scoped raw
   folders (+ keyed merges once added).
5. **Alerting & observability:** Azure Monitor alerts on pipeline failure
   and on reconciliation drift from the process log; lifecycle management
   on the raw zone (cool/archive tiers).
6. **Security hardening:** private endpoints, managed identity end-to-end
   where supported, TLS to the source, secret rotation.
7. **CI/CD:** factory attached to Git; notebooks + SQL versioned in this
   repo already; transform logic migrated to dbt as the model count grows.
8. **Cost at scale:** job clusters (spot-backed) instead of an interactive
   cluster; partitioned parallel copies for large tables; Synapse/Fabric
   only when volume justifies it.

## Evidence of execution

See `evidence/EvidenceDocument.docx` — embedded screenshots and query
output from the actual cloud run (2026-07-30): Postgres source tables,
ADF pipeline execution to the raw staging layer, end-to-end pipeline
success, Databricks notebook run log, curated-layer row counts,
`etl.pipeline_process_log` reconciliation for the run, and full output
from all three mart views.

## Repository layout

```
Data Pipeline Exercise/
├── README.md
├── SETUP_GUIDE.md                    phased build with checkpoints
├── Data Pipeline Exercise.docx       original brief
├── architecture.jpg                 as-built architecture diagram
├── sql/
│   ├── 01_azure_sql_ddl.sql          curated/etl/mart DDL (9 tables + 3 views)
│   └── 02_evidence_queries.sql       run after the pipeline; screenshot grids
├── databricks/
│   ├── olist_transforms.ipynb        transform logic (rename/cast/dedup/DQ flags)
│   └── transform_olist.ipynb         notebook shell (widgets/JDBC/log/loop)
├── adf/
│   ├── adf_pipeline_design.md        build notes
│   ├── pl_ol_ingest_onprem_to_adls.json   exported factory pipeline JSON
│   └── config/entities.json          the metadata that drives the pipeline
├── postgres_source/
│   └── postgres_seed.sql             src.* DDL + \copy loads (Docker Postgres)
├── results.xlsx                     results summary
└── evidence/                         screenshots + evidence doc from the cloud run
```
