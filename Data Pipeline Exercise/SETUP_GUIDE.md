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

**Checkpoint 0:** `34/34 checks passe` → screenshot
(`01_local_harness.png`). The transforms are now proven before any cloud
spend; the cloud phases only have to prove the *seams* (auth, copies, JDBC).

## Phase 1 — Complete the infrastructure (20–30 min)

> **No Key Vault, no role assignments.** This subscription doesn't allow
> granting Key Vault access or assigning ANY Azure role to ANY principal
> (checked directly — Access policies, RBAC, and the factory's managed
> identity are all blocked). So this design uses none of them: every
> secret is a plain secure string typed directly into whatever needs it
> (ADF encrypts linked-service secrets internally; Databricks gets a
> **native**, non-Key-Vault secret scope in Phase 4). Nothing below
> requires creating a Key Vault or assigning a role to anything — if you
> already created one while troubleshooting, it's unused and safe to
> ignore or delete.

One resource to create:

1. Install the **Self-Hosted Integration Runtime** on this machine: ADF
   Studio → Manage → Integration runtimes → New → Self-Hosted (name it
   `shir-laptop`) → download the MSI, install, paste the registration key.
   Wait for status **Running**. (No role assignment involved — the
   registration key is a resource-scoped secret, not an IAM grant.)
2. Storage account: **verify hierarchical namespace is enabled**
   (Overview → "Data Lake Storage" should say enabled; if not: Settings →
   Data Lake Gen2 upgrade). `abfss://` access requires it — this is the one
   silent blocker among the pre-created resources.
3. Storage account: create containers `config`, `inbox`, `raw` (create
   `dbextract` too only if you end up on the fallback path, below).
4. Storage account → **Access keys** → copy `key1`. You'll paste this same
   value into two places: the `ls_adls` ADF linked service (Phase 5) and
   the Databricks `storage-key` secret (Phase 4). Copying your own
   account's key isn't a role assignment — it's a plain data-plane read on
   a resource you already own, so this works regardless of the RBAC block.
5. Azure SQL server → Networking: add your client IP; enable
   *Allow Azure services and resources to access this server*.

**Checkpoint 1:** SHIR status Running; storage shows HNS enabled +
containers, key1 copied somewhere handy; SQL firewall saved.

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
`order_reviews` (~9 MB) — and swap `cp_pg_entity`'s source dataset to
`ds_adls_parquet_dbextract` (`entity` = `@item().entity`) per
`adf/adf_pipeline_design.md`. Everything downstream is unchanged.

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
2. Generate a **Databricks personal access token** (User Settings →
   Developer → Access tokens → Generate new token). Save it somewhere
   safe — you'll use it twice: once now for the CLI, and again in Phase 5
   for the `ls_databricks` linked service. This is a pure Databricks-
   workspace permission, unrelated to the Azure RBAC block.
3. Install the Databricks CLI locally (`pip install databricks-cli`) and
   configure it: `databricks configure --token` → paste your workspace URL
   and the PAT from step 2.
4. Create the **native** (non-Key-Vault) secret scope and its 5 secrets.
   The CLI defaults to making the creating user the scope's sole admin,
   which needs Premium tier; on a **Standard tier** workspace that's
   rejected (`BAD_REQUEST: Premium Tier is disabled ... initial_manage_principal "users"`) — pass `--initial-manage-principal users` explicitly, which
   Standard tier does allow (it grants MANAGE to every workspace user; on
   a single-user workspace that's no different in practice):

   ```powershell
   databricks secrets create-scope --scope olist-secrets --initial-manage-principal users
   databricks secrets put --scope olist-secrets --key sql-server-name --string-value "<server, no .database.windows.net>"
   databricks secrets put --scope olist-secrets --key sql-db-name --string-value "<db name>"
   databricks secrets put --scope olist-secrets --key sql-user --string-value "<admin user>"
   databricks secrets put --scope olist-secrets --key sql-password --string-value "<admin password>"
   databricks secrets put --scope olist-secrets --key storage-key --string-value "<key1 from Phase 1 step 4>"
   databricks secrets list --scope olist-secrets    # verify: lists key names, never values
   ```

   (Flags shown are for the **legacy** `databricks-cli` PyPI package —
   `pip install databricks-cli` installs this one, hence the deprecation
   warning it prints; that's expected and harmless for this exercise. The
   newer standalone CLI uses positional args instead, e.g.
   `databricks secrets create-scope olist-secrets` with no `--scope` — if
   `--scope` errors as unrecognized, you have the new CLI and should drop
   the flag names back to positional.) This is the direct swap for the old
   `#secrets/createScope` UI flow — same
   `dbutils.secrets.get("olist-secrets", ...)` calls in the notebook, just
   backed by Databricks' own store instead of Key Vault, so no Azure role
   assignment is needed at all.
5. Import BOTH notebooks into the **same** workspace folder — any folder
   works (a personal `/Users/<you>/...` folder is fine, doesn't have to be
   `/Shared`): `databricks/olist_transforms.ipynb` and
   `databricks/transform_olist.ipynb` (the shell `%run`s
   `./olist_transforms`, a relative path, so they must sit side by side).
   The `.py` sources import identically if you prefer; the `.ipynb` files
   are generated from them by `local_test/make_notebooks.py` — regenerate
   rather than editing the `.ipynb` directly. Note the exact path you used
   (e.g. `/Users/you@example.com/transform_olist`) — you'll need it for
   the ADF Notebook activity's Notebook path field in Phase 5.
6. Smoke test with the smallest entity: reads are exact-file, RunId-scoped
   (see `adf/adf_pipeline_design.md` "Fix applied"), so the uploaded file
   must be **named to match the `run_id` widget value** — upload
   `product_category_name_translation.csv` (from Downloads) into
   `raw/csv/product_category_translation/` via storage browser, renamed to
   `manual-smoke.txt` (extension doesn't matter, only the name prefix
   does). Then run `transform_olist` with widgets `run_id = manual-smoke`,
   `only_entity = product_category_translation` (bare word, no quotes —
   an empty widget must be truly empty, not `''`),
   `raw_base_path = abfss://olistdata@<storageaccount>.dfs.core.windows.net/raw`.
   Expected friction lives here: a secret name typo in step 4 (error names
   the missing key), storage auth, the JDBC write (verify *Allow Azure
   services* if it times out).

**Checkpoint 4:** `SELECT COUNT(*) FROM curated.product_category_translation`
returns **71** and `etl.pipeline_process_log` has one SUCCESS row →
screenshot (`04_databricks_smoke.png`).

## Phase 5 — ADF pipeline (60–75 min)

Laptop, Docker Desktop and the `qvc_sql_exercise` container must be
running (the SHIR reads Postgres at `localhost:5432`). The pipeline is
**metadata-driven**: one config file lists all 9 entities; a Lookup + two
Filters + two ForEach loops do the copying. Follow
`adf/adf_pipeline_design.md` (it has every expression to type):

1. Upload `adf/config/entities.json` to the `config` container as
   `config/entities.json`.
2. 3 linked services, **all secret-based, no role assignments**
   (`adf/adf_pipeline_design.md` has the exact fields): ADLS via
   **Account key** (paste storage `key1` from Phase 1 step 4); PostgreSQL
   via `shir-laptop`, password `qvc` entered directly, **SSL mode:
   disable**; Databricks via **Access Token** (paste the PAT from Phase 4
   step 2 — not Managed Service Identity).
3. 5 datasets: `ds_json_config`, `ds_pg_table(schema, table)`,
   `ds_adls_parquet_raw(entity, run_id)`,
   `ds_adls_csv_inbox(folder, file)`, `ds_adls_csv_raw(entity, run_id)`.
4. Pipeline graph: `lkp_entities` (First row only OFF) → `flt_pg` /
   `flt_csv` → `fe_pg` / `fe_csv` (items = `@activity('flt_…').output.Value`
   — capital V) each containing one parameterised Copy → `nb_transform`
   depending on both loops, passing `run_id` and `raw_base_path`.
5. Debug small first: temporarily upload a 2-item `entities.json` (one
   postgres + one csv row), Debug, fix expressions, then upload the full
   9-item file.
6. **Debug run** end-to-end — the first full-9 cloud run. Safe: transforms
   passed locally (Phase 0), the JDBC seam passed on real infrastructure
   (Phase 4). If the postgres iterations fail and the SHIR can't be
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

Nothing new bills meaningfully: Azure SQL serverless auto-pauses, the
Databricks cluster auto-terminates, storage holds ~200 MB, and the SHIR is
free (uninstall the MSI whenever you like — the Docker Postgres is only
reachable while your laptop runs anyway). If you created a Key Vault while
troubleshooting the RBAC block, it's unused — delete it any time. Also
revoke the Databricks PAT (User Settings → Developer → Access tokens) once
you're done, since it's a standing credential. Delete the containers'
contents if the account should go fully quiet;
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
match `ENTITY_CONFIG` keys).

### As designed (one container per zone)

```
<storage account>  (hierarchical namespace ENABLED)
│
├── config/    entities.json
├── inbox/     <entity>/<original filename>.csv     (4 reference files)
├── raw/       <entity>/run_id=<ADF RunId>/  *.parquet|*.csv
└── dbextract/ <entity>/<entity>.parquet             (fallback only)
```

### As actually built (single `olistdata` container) — verified against the exported pipeline JSON

The real build put everything in one container (`olistdata`) with `raw`
as a folder, and the two ADF loops land data in **different shapes** —
confirmed against `adf/pl_ol_ingest_onprem_to_adls.json` (the exported
pipeline definition), not just inferred from the storage browser:

```
olistdata/                        (container; HNS enabled)
├── raw/
│   ├── csv/                      ← Copy_Csv_to_raw sink: entity as FOLDER
│   │   ├── products/<RunId>.txt
│   │   ├── sellers/<RunId>.txt
│   │   ├── geolocation/<RunId>.txt
│   │   └── product_category_translation/<RunId>.txt
│   │
│   └── parquet/                  ← Copy_onprem_to_adls_raw sink: FLAT —
│       ├── orders<RunId>              entity+RunId concatenated into the
│       ├── customers<RunId>           FILE NAME instead of a folder (no
│       ├── order_items<RunId>         entity subfolder, no extension —
│       ├── order_payments<RunId>      confirmed from the dataset's real
│       └── order_reviews<RunId>       file_name expression)
```

Both shapes are fully run-scoped: the csv sink's file is named exactly
`@pipeline().RunId` with `.txt` appended (its `fileExtension` setting),
and the parquet sink's file name is `@concat(item().entity, pipeline().RunId)`. `transform_olist.py::read_raw()` reads the **exact
file** for the current run in both cases (via the shared `find_run_file()`
helper, which globs by RunId prefix rather than hardcoding an extension) —
not the whole folder, so old files from a previous run are never
accidentally included. `raw_base_path` widget =
`abfss://olistdata@<storageaccount>.dfs.core.windows.net/raw`.

**Phase 4 smoke test, adjusted for this layout:** the uploaded file must
be **named to match your `run_id` widget value** (e.g. `manual-smoke.txt`
if `run_id = manual-smoke`) and land inside
`raw/csv/product_category_translation/` — see Phase 4 step 6.

### `dbextract/` (fallback only — Phase 2c; skip otherwise)

```
olistdata/dbextract/
├── orders/orders.parquet                    (~10 MB)
├── customers/customers.parquet              (~7 MB)
├── order_items/order_items.parquet          (~6.5 MB)
├── order_payments/order_payments.parquet    (~4 MB)
└── order_reviews/order_reviews.parquet      (~9 MB)
```

## Evidence file naming

```
evidence/
├── 01_local_harness.png        34/34 checks against the full dataset
├── 02_sources_seeded.png       psql exact counts + inbox layout
├── 03_target_ddl.png           schemas/tables/views created
├── 04_databricks_smoke.png     71-row smoke load + process log row
├── 05_adf_canvas.png           Lookup → Filters → ForEach ×2 → notebook
├── 06_monitor_green.png        per-copy row counts
├── 07_process_log.png          9 SUCCESS rows, ADF run GUID
├── 08_dq_distributions.png     flagged payments / scores / geo compression
├── 09_mart_views.png           v_sales_overview, v_payment_mix, v_review_delivery
└── 10_trigger.png              disabled daily schedule
```
