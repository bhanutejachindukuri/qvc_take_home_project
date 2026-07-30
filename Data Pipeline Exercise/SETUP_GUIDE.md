# Setup guide — step by step (target: 4–5 hours)

Starting state this guide assumes: **Azure SQL Database, Azure Databricks,
Azure Data Factory and the Storage account already exist as empty shells**;
the 9 Olist CSVs are extracted in `C:\Users\bhanu\Downloads`; Python 3.12 +
PySpark 3.5 are installed locally; Docker Desktop with the SQL Exercise's
`qvc_sql_exercise` PostgreSQL 16 container is available (it becomes the
relational source, reached by ADF through a Self-Hosted Integration
Runtime on this machine).

Work through the phases in order. Each phase ends with a **checkpoint** — do
not move on until it passes. Screenshot every checkpoint; they become the
evidence pack (`evidence/`, naming convention at the end).

---

## Phase 0 — Local pre-flight (15 min, before touching Azure)

1. `cd "Data Pipeline Exercise\local_test"` and run
   `python run_local_transforms.py`
2. This builds the five `dbextract` parquet extracts (including the
   multiline reviews parse — asserted at exactly 99,224 records) and runs
   all nine entity transforms against the full real dataset.

**Checkpoint 0:** `34/34 checks passed` → screenshot
(`01_local_harness.png`). The transforms are now proven before any cloud
spend; the cloud phases only have to prove the *seams* (auth, copies, JDBC).

## Phase 1 — Complete the infrastructure (30–45 min)

One resource to create plus one agent to install (Key Vault in the same
region as the existing resources):

1. Create the **Azure Key Vault**; add 6 secrets:
   `sql-server-name` (just the server name, no `.database.windows.net`),
   `sql-db-name`, `sql-user`, `sql-password`,
   `pg-password` (= `qvc`, the Docker container's password),
   `storage-key` (storage account → Access keys → key1).
2. Install the **Self-Hosted Integration Runtime** on this machine: ADF
   Studio → Manage → Integration runtimes → New → Self-Hosted (name it
   `shir-laptop`) → download the MSI, install, paste the registration key.
   Wait for status **Running**.
3. Storage account: **verify hierarchical namespace is enabled**
   (Overview → "Data Lake Storage" should say enabled; if not: Settings →
   Data Lake Gen2 upgrade). `abfss://` access requires it — this is the one
   silent blocker among the pre-created resources.
4. Storage account: create containers `inbox`, `raw` (create `dbextract`
   too only if you end up on the fallback path, below).
5. Azure SQL server → Networking: add your client IP; enable
   *Allow Azure services and resources to access this server*.

**Checkpoint 1:** Key Vault shows 6 secrets; SHIR status Running; storage
shows HNS enabled + containers; SQL firewall saved.

## Phase 2 — Seed the two sources (30–40 min)

### 2a. Relational source — Docker PostgreSQL

Start Docker Desktop, then in PowerShell:

```powershell
docker start qvc_sql_exercise
docker exec qvc_sql_exercise psql -U qvc -d qvc_sql_exercise -c "CREATE DATABASE olist;"
docker cp "Data Pipeline Exercise\postgres_source\postgres_seed.sql" qvc_sql_exercise:/tmp/
foreach ($f in "olist_orders_dataset.csv","olist_customers_dataset.csv",
               "olist_order_items_dataset.csv","olist_order_payments_dataset.csv",
               "olist_order_reviews_dataset.csv") {
    docker cp "C:\Users\bhanu\Downloads\$f" qvc_sql_exercise:/tmp/
}
docker exec -it qvc_sql_exercise psql -U qvc -d olist -f /tmp/postgres_seed.sql
docker exec -it qvc_sql_exercise psql -U qvc -d olist
```

Inside that psql session: `\cd /tmp`, then run the five `\copy` commands
and the sanity-count block from the seed script's comments.

**Checkpoint 2a:** exact counts — orders 99,441 / customers 99,441 /
order_items 112,650 / order_payments 103,886 / **order_reviews 99,224**
(if reviews shows ~104k the file was loaded line-wise — reload) →
screenshot (`02_sources_seeded.png`, part 1).

### 2b. File source — inbox uploads

Via Portal → Storage account → Storage browser, keep the
folder-per-entity layout exactly:

- `inbox/products/olist_products_dataset.csv`
- `inbox/sellers/olist_sellers_dataset.csv`
- `inbox/geolocation/olist_geolocation_dataset.csv`  (~61 MB — the big one)
- `inbox/product_category_translation/product_category_name_translation.csv`

**Checkpoint 2b:** storage browser shows the 4 inbox folders →
screenshot (`02_sources_seeded.png`, part 2).

### 2c. FALLBACK ONLY — parquet extracts instead of Postgres

If the SHIR/Postgres path fails in Phase 5, upload the five verified
extracts built in Phase 0 (`local_test\output\dbextract\`) to a
`dbextract` container — `dbextract/orders/orders.parquet` (~10 MB),
`customers` (~7 MB), `order_items` (~6.5 MB), `order_payments` (~4 MB),
`order_reviews` (~9 MB) — and swap the five copies' source dataset to
`ds_adls_parquet_dbextract` per `adf/adf_pipeline_design.md`. Everything
downstream is unchanged.

## Phase 3 — Target DDL (15 min)

1. Azure SQL → Query editor (or SSMS / Azure Data Studio), run
   `sql/01_azure_sql_ddl.sql`.

**Checkpoint 3:** schemas `curated` / `etl` / `mart` exist; 9 curated
tables + 3 views created; `SELECT * FROM etl.pipeline_process_log` returns
an empty result (not an error) → screenshot (`03_target_ddl.png`).

## Phase 4 — Databricks, interactively first (45–60 min)

> Prove the cloud-only seams (ADLS auth, JDBC write, process log) on a
> 71-row entity before wiring orchestration.

1. Create a **single-node cluster**: smallest VM available, latest LTS
   runtime, **auto-terminate 15 min**.
2. Create the Key-Vault-backed secret scope **`kv-olist`**: open
   `https://<workspace-url>#secrets/createScope` (name `kv-olist`, vault
   DNS + resource ID from the Key Vault's Properties page).
3. Import BOTH notebooks into the same folder (Workspace → `/Shared` →
   Import): `databricks/olist_transforms.ipynb` and
   `databricks/transform_olist.ipynb` (the shell `%run`s
   `./olist_transforms`, so they must sit side by side). The `.py`
   sources import identically if you prefer; the `.ipynb` files are
   generated from them by `local_test/make_notebooks.py` — regenerate
   rather than editing the `.ipynb` directly.
4. Smoke test with the smallest entity: upload
   `product_category_name_translation.csv` (again, from Downloads) to
   `raw/product_category_translation/run_id=manual-smoke/` via storage
   browser, then run `transform_olist` with widgets
   `run_id = manual-smoke`, `only_entity = product_category_translation`,
   `raw_base_path = abfss://raw@<storageaccount>.dfs.core.windows.net`.
   Expected friction lives here: secret scope names, storage auth, the
   JDBC write (verify *Allow Azure services* if it times out).

**Checkpoint 4:** `SELECT COUNT(*) FROM curated.product_category_translation`
returns **71** and `etl.pipeline_process_log` has one SUCCESS row →
screenshot (`04_databricks_smoke.png`).

## Phase 5 — ADF pipeline (60–75 min)

Laptop, Docker Desktop and the `qvc_sql_exercise` container must be
running (the SHIR reads Postgres at `localhost:5432`). Follow
`adf/adf_pipeline_design.md`:

1. 4 linked services (ADLS via managed identity — grant the factory
   *Storage Blob Data Contributor* on the storage account; PostgreSQL via
   `shir-laptop` with **SSL mode: disable** and the Key Vault password;
   Key Vault — grant *Key Vault Secrets User*; Databricks).
2. 4 parameterised datasets (plus the fallback `ds_adls_parquet_dbextract`
   if you want it pre-staged).
3. 9 Copy activities: 5 Postgres→parquet + 4 csv→csv (build `cp_orders`
   and `cp_products` first, Debug both, then clone the remaining seven and
   edit parameters).
4. Notebook activity depending on all nine, passing `run_id` and
   `raw_base_path`.
5. **Debug run** end-to-end — the first full-9 cloud run. Safe: transforms
   passed locally (Phase 0), the JDBC seam passed on real infrastructure
   (Phase 4). If the Postgres copies fail here and the SHIR can't be
   unblocked quickly, switch to the fallback (Phase 2c) and re-Debug.

**Checkpoint 5:** Monitor fully green; per-copy rows visible
(orders 99,441 / customers 99,441 / items 112,650 / payments 103,886 /
reviews 99,224 / products 32,951 / sellers 3,095 / geolocation 1,000,163 /
translation 71); process log shows 9 SUCCESS rows under the ADF run GUID →
screenshots (`05_adf_canvas.png`, `06_monitor_green.png`,
`07_process_log.png`).

## Phase 6 — Evidence + packaging (45 min)

1. Run `sql/02_evidence_queries.sql`; screenshot each grid — at minimum the
   DQ distributions (`08_dq_distributions.png`: the 3 flagged
   `not_defined` payments, review-score spread, geolocation compression
   1,000,163 → 19,015) and the three mart views (`09_mart_views.png`).
2. Add a daily schedule trigger, **disabled**; screenshot
   (`10_trigger.png`).
3. Export the factory: connect to Git (preferred) or Manage → ARM template
   → export; commit the JSON under `adf/`.
4. Fill the README placeholders (resource names, run duration, any
   deviations); drop all screenshots into `evidence/`.

**Checkpoint 6 (final):** a stranger with only this repo + screenshots can
tell what ran, where, and with what result.

## Phase 7 — Cost hygiene (5 min)

Nothing new bills meaningfully: Key Vault is pennies, Azure SQL serverless
auto-pauses, the Databricks cluster auto-terminates, storage holds ~200 MB,
and the SHIR is free (uninstall the MSI whenever you like — the Docker
Postgres is only reachable while your laptop runs anyway). Delete the Key
Vault (and the containers' contents) if the account should go fully quiet;
the four pre-existing resources are yours to keep or remove.

---

## If you fall behind — pre-agreed scope cuts, in order

1. Drop the **curated geolocation dimension** — keep its Copy activity
   (raw fidelity retained, costs nothing) and point at the designed
   aggregation in `olist_transforms.aggregate_geolocation` + the README.
2. Drop `mart.v_review_delivery` (keep `v_payment_mix`).
3. If the ADF↔Databricks linked service fights back: run the notebook
   manually with the ADF run's `run_id` and document orchestration as
   wired-but-manually-triggered, with the canvas screenshot.
4. Skip the trigger screenshot (describe it in the README only).

**Never cut:** the process log, the DQ flags, ingestion metadata, the
README rationale sections, or the Phase 0 harness evidence — those carry
the scoring weight.

## Appendix — ADLS folder structure (reference)

Container and folder names are load-bearing: the ADF datasets and the
notebook build paths from these exact lowercase strings (the entity names
match `ENTITY_CONFIG` keys). Create only the containers — ADF creates the
`raw` subfolders itself on the first run.

```
<storage account>  (hierarchical namespace ENABLED)
│
├── inbox/                        ← manual upload, once (file source)
│   ├── products/olist_products_dataset.csv                        (~2.3 MB)
│   ├── sellers/olist_sellers_dataset.csv                          (~0.2 MB)
│   ├── geolocation/olist_geolocation_dataset.csv                  (~61 MB)
│   └── product_category_translation/product_category_name_translation.csv
│
├── raw/                          ← written ONLY by ADF; starts EMPTY
│   │                               (one run_id folder per entity per run —
│   │                                that is the reproducibility feature)
│   ├── orders/run_id=<ADF RunId>/                 *.parquet
│   ├── customers/run_id=<ADF RunId>/              *.parquet
│   ├── order_items/run_id=<ADF RunId>/            *.parquet
│   ├── order_payments/run_id=<ADF RunId>/         *.parquet
│   ├── order_reviews/run_id=<ADF RunId>/          *.parquet
│   ├── products/run_id=<ADF RunId>/               *.csv (snapshot)
│   ├── sellers/run_id=<ADF RunId>/                *.csv
│   ├── geolocation/run_id=<ADF RunId>/            *.csv
│   ├── product_category_translation/run_id=<ADF RunId>/  *.csv
│   └── product_category_translation/run_id=manual-smoke/ ← Phase 4 only:
│                                     upload the translation csv here by
│                                     hand for the Databricks smoke test
│
└── dbextract/                    ← FALLBACK ONLY (Phase 2c; skip otherwise)
    ├── orders/orders.parquet                    (~10 MB)
    ├── customers/customers.parquet              (~7 MB)
    ├── order_items/order_items.parquet          (~6.5 MB)
    ├── order_payments/order_payments.parquet    (~4 MB)
    └── order_reviews/order_reviews.parquet      (~9 MB)
```

The notebook reads `abfss://raw@<storageaccount>.dfs.core.windows.net/`
(`raw` is the container in the URL) + `<entity>/run_id=<RUN_ID>/`. Old
`run_id=` folders accumulate by design; production would apply lifecycle
management (cool/archive) to them.

## Evidence file naming

```
evidence/
├── 01_local_harness.png        34/34 checks against the full dataset
├── 02_sources_seeded.png       psql exact counts + inbox layout
├── 03_target_ddl.png           schemas/tables/views created
├── 04_databricks_smoke.png     71-row smoke load + process log row
├── 05_adf_canvas.png           9 copies → notebook
├── 06_monitor_green.png        per-copy row counts
├── 07_process_log.png          9 SUCCESS rows, ADF run GUID
├── 08_dq_distributions.png     flagged payments / scores / geo compression
├── 09_mart_views.png           v_sales_overview, v_payment_mix, v_review_delivery
└── 10_trigger.png              disabled daily schedule
```
