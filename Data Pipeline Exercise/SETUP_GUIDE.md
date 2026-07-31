# Build log — how this was set up, phase by phase (~4–5 hours)

Starting state: Azure SQL Database, Azure Databricks, Azure Data Factory
and the Storage account already existed as empty shells; the 9 Olist CSVs
were extracted in `C:\Users\bhanu\Downloads`; Python 3.12 + PySpark 3.5
were installed locally; Docker Desktop with the SQL Exercise's
`qvc_sql_exercise` PostgreSQL 16 container was available (it became the
relational source, reached by ADF through a Self-Hosted Integration
Runtime on this machine).

The phases below were worked through in order, each one ending in a
**checkpoint** before moving on to the next. Every checkpoint was
screenshotted; together they form the evidence pack (`evidence/`, naming
convention at the end).

---

## Phase 0 — Local pre-flight (15 min, before touching Azure)

Went through all nine Olist datasets locally before touching Azure, and a
few things stood out: the reviews file needs multiline-aware CSV parsing
(99,224 genuine records, not the ~104,700 a naive line count gives); 3
rows in `order_payments` carry a `payment_type` of `not_defined`; ~610
products have no category; and the geolocation file is a 1,000,163-row
point cloud that only makes sense joined at zip-prefix grain. Those
observations are what shaped the transform design — the DQ flags, the
dedup keys, and the geolocation aggregation — before any of it was
pointed at Azure. It's also why the row counts and DQ numbers referenced
throughout this doc and the README are exact, not estimates.

## Phase 1 — Complete the infrastructure (20–30 min)

> **No Key Vault, no role assignments.** This subscription doesn't allow
> granting Key Vault access or assigning ANY Azure role to ANY principal
> (checked directly — Access policies, RBAC, and the factory's managed
> identity are all blocked). So this design uses none of them: every
> secret is a plain secure string typed directly into whatever needs it
> (ADF encrypts linked-service secrets internally; Databricks got a
> **native**, non-Key-Vault secret scope in Phase 4). None of this
> required creating a Key Vault or assigning a role to anything — a Key
> Vault created earlier while troubleshooting was left unused.

One resource needed to be created:

1. Installed the **Self-Hosted Integration Runtime** on this machine: ADF
   Studio → Manage → Integration runtimes → New → Self-Hosted (named
   `shir-laptop`) → downloaded the MSI, installed it, pasted the
   registration key. Waited for status **Running**. (No role assignment
   involved — the registration key is a resource-scoped secret, not an
   IAM grant.)
2. Storage account: **verified hierarchical namespace was enabled**
   (Overview → "Data Lake Storage" said enabled — this is the one silent
   blocker among the pre-created resources; `abfss://` access requires
   it).
3. Storage account: created containers `config`, `inbox`, `raw`.
4. Storage account → **Access keys** → copied `key1`, pasted into two
   places: the `ls_adls` ADF linked service (Phase 5) and the Databricks
   `storage-key` secret (Phase 4). Copying an account's own key isn't a
   role assignment — it's a plain data-plane read on a resource already
   owned, so this works regardless of the RBAC block.
5. Azure SQL server → Networking: added the client IP; enabled
   *Allow Azure services and resources to access this server*.

**Checkpoint 1:** SHIR status Running; storage showed HNS enabled +
containers; key1 saved; SQL firewall configured.

## Phase 2 — Seed the two sources (30–40 min)

### 2a. Relational source — Docker PostgreSQL

Docker Desktop was started, then in PowerShell:

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

Inside that psql session: `\cd /tmp`, then the five `\copy` commands and
the sanity-count block from the seed script's comments were run.

**Checkpoint 2a:** exact counts — orders 99,441 / customers 99,441 /
order_items 112,650 / order_payments 103,886 / **order_reviews 99,224**
(a ~104k count would mean the file loaded line-wise instead of
multiline-parsed) — captured as `02_sources_seeded.png`, part 1.

### 2b. File source — inbox uploads

Via Portal → Storage account → Storage browser, the four reference files
were uploaded keeping the folder-per-entity layout exactly:

- `inbox/products/olist_products_dataset.csv`
- `inbox/sellers/olist_sellers_dataset.csv`
- `inbox/geolocation/olist_geolocation_dataset.csv`  (~61 MB — the big one)
- `inbox/product_category_translation/product_category_name_translation.csv`

**Checkpoint 2b:** storage browser showed the 4 inbox folders — captured
as `02_sources_seeded.png`, part 2.

## Phase 3 — Target DDL (15 min)

1. Azure SQL → Query editor (SSMS / Azure Data Studio work equally well)
   — ran `sql/01_azure_sql_ddl.sql`.

**Checkpoint 3:** schemas `curated` / `etl` / `mart` existed; 9 curated
tables + 3 views created; `SELECT * FROM etl.pipeline_process_log`
returned an empty result (not an error) — captured as `03_target_ddl.png`.

## Phase 4 — Databricks, interactively first (45–60 min)

> Goal: prove the cloud-only seams (ADLS auth, JDBC write, process log)
> on a 71-row entity before wiring orchestration.

1. Created a **single-node cluster**: smallest VM available, latest LTS
   runtime, **auto-terminate 15 min**.
2. Generated a **Databricks personal access token** (User Settings →
   Developer → Access tokens → Generate new token). Saved it for two
   uses: the CLI here, and the `ls_databricks` linked service in Phase 5.
   This is a pure Databricks-workspace permission, unrelated to the Azure
   RBAC block.
3. Installed the Databricks CLI locally (`pip install databricks-cli`)
   and configured it: `databricks configure --token` with the workspace
   URL and the PAT from step 2.
4. Created the **native** (non-Key-Vault) secret scope and its 5 secrets.
   The CLI defaults to making the creating user the scope's sole admin,
   which needs Premium tier; on this **Standard tier** workspace that was
   rejected (`BAD_REQUEST: Premium Tier is disabled ... initial_manage_principal "users"`) — `--initial-manage-principal users` was passed
   explicitly, which Standard tier does allow (it grants MANAGE to every
   workspace user; on a single-user workspace that's no different in
   practice):

   ```powershell
   databricks secrets create-scope --scope olist-secrets --initial-manage-principal users
   databricks secrets put --scope olist-secrets --key sql-server-name --string-value "<server, no .database.windows.net>"
   databricks secrets put --scope olist-secrets --key sql-db-name --string-value "<db name>"
   databricks secrets put --scope olist-secrets --key sql-user --string-value "<admin user>"
   databricks secrets put --scope olist-secrets --key sql-password --string-value "<admin password>"
   databricks secrets put --scope olist-secrets --key storage-key --string-value "<key1 from Phase 1 step 4>"
   databricks secrets list --scope olist-secrets    # verify: lists key names, never values
   ```

   (The flags above are for the **legacy** `databricks-cli` PyPI package —
   `pip install databricks-cli` installs this one, hence the deprecation
   warning it prints; that's expected and harmless for this exercise. The
   newer standalone CLI uses positional args instead, e.g.
   `databricks secrets create-scope olist-secrets` with no `--scope` — a
   `--scope` "unrecognized flag" error would indicate the new CLI, which
   takes the same names positionally.) This is the direct swap for the
   old `#secrets/createScope` UI flow — same
   `dbutils.secrets.get("olist-secrets", ...)` calls in the notebook, just
   backed by Databricks' own store instead of Key Vault, so no Azure role
   assignment was needed at all.
5. Imported both notebooks into the same workspace folder — any folder
   works (a personal `/Users/<user>/...` folder, doesn't have to be
   `/Shared`): `databricks/olist_transforms.ipynb` and
   `databricks/transform_olist.ipynb` (the shell `%run`s
   `./olist_transforms`, a relative path, so they had to sit side by
   side). The exact path used (e.g.
   `/Users/you@example.com/transform_olist`) was needed for the ADF
   Notebook activity's Notebook path field in Phase 5.
6. Smoke-tested with the smallest entity: reads are exact-file,
   RunId-scoped (see `adf/adf_pipeline_design.md` "Fix applied"), so the
   uploaded file had to be **named to match the `run_id` widget value** —
   `product_category_name_translation.csv` (from Downloads) was uploaded
   into `raw/csv/product_category_translation/` via storage browser,
   renamed to `manual-smoke.txt` (extension doesn't matter, only the name
   prefix does). `transform_olist` was then run with widgets
   `run_id = manual-smoke`,
   `only_entity = product_category_translation` (bare word, no quotes —
   an empty widget must be truly empty, not `''`),
   `raw_base_path = abfss://olistdata@<storageaccount>.dfs.core.windows.net/raw`.
   The friction points here were a secret name typo in step 4 (error
   names the missing key), storage auth, and the JDBC write (needs
   *Allow Azure services* enabled or it times out).

**Checkpoint 4:** `SELECT COUNT(*) FROM curated.product_category_translation`
returned **71** and `etl.pipeline_process_log` had one SUCCESS row —
captured as `04_databricks_smoke.png`.

## Phase 5 — ADF pipeline (60–75 min)

Laptop, Docker Desktop and the `qvc_sql_exercise` container were running
(the SHIR reads Postgres at `localhost:5432`). The pipeline is
**metadata-driven**: one config file lists all 9 entities; a Lookup + two
Filters + two ForEach loops do the copying, per
`adf/adf_pipeline_design.md` (which has every expression):

1. Uploaded `adf/config/entities.json` to the `config` container as
   `config/entities.json`.
2. Created 3 linked services, **all secret-based, no role assignments**
   (`adf/adf_pipeline_design.md` has the exact fields): ADLS via
   **Account key** (storage `key1` from Phase 1 step 4); PostgreSQL via
   `shir-laptop`, password `qvc` entered directly, **SSL mode: disable**;
   Databricks via **Access Token** (the PAT from Phase 4 step 2 — not
   Managed Service Identity).
3. Created 5 datasets: `ds_json_config`, `ds_pg_table(schema, table)`,
   `ds_adls_parquet_raw(entity, run_id)`,
   `ds_adls_csv_inbox(folder, file)`, `ds_adls_csv_raw(entity, run_id)`.
4. Built the pipeline graph: `lkp_entities` (First row only OFF) →
   `flt_pg` / `flt_csv` → `fe_pg` / `fe_csv`
   (items = `@activity('flt_…').output.Value` — capital V) each
   containing one parameterised Copy → `nb_transform` depending on both
   loops, passing `run_id` and `raw_base_path`.
5. Debugged small first: temporarily uploaded a 2-item `entities.json`
   (one postgres + one csv row), fixed expressions, then uploaded the
   full 9-item file.
6. Ran **Debug** end-to-end — the first full-9 cloud run. This was safe:
   transforms had already passed locally (Phase 0), and the JDBC seam had
   already passed on real infrastructure (Phase 4).

**Checkpoint 5:** Monitor fully green; per-copy rows visible
(orders 99,441 / customers 99,441 / items 112,650 / payments 103,886 /
reviews 99,224 / products 32,951 / sellers 3,095 / geolocation 1,000,163 /
translation 71); process log showed 9 SUCCESS rows under the ADF run GUID
— captured as `05_adf_canvas.png`, `06_monitor_green.png`,
`07_process_log.png`.

## Phase 6 — Evidence + packaging (45 min)

1. Ran `sql/02_evidence_queries.sql`; screenshotted each grid — at
   minimum the DQ distributions (`08_dq_distributions.png`: the 3
   flagged `not_defined` payments, review-score spread, geolocation
   compression 1,000,163 → 19,015) and the three mart views
   (`09_mart_views.png`).
2. Added `Daily_schedule_olist_trigger`, a schedule trigger recurring
   weekly on Mon/Fri (name is a holdover from the original daily plan;
   the recurrence was set to Mon/Fri instead), then **stopped** so it
   won't fire on its own — captured as `10_trigger.png`.
3. Exported the factory: connected to Git (preferred) / Manage → ARM
   template → export; committed the JSON under `adf/`.
4. Filled in the README placeholders (resource names, run duration, any
   deviations); dropped all screenshots into `evidence/`.

**Checkpoint 6 (final):** a stranger with only this repo + screenshots
can tell what ran, where, and with what result.

## Phase 7 — Cost hygiene (5 min)

Nothing new bills meaningfully: Azure SQL serverless auto-pauses, the
Databricks cluster auto-terminates, storage holds ~200 MB, and the SHIR
is free (the MSI can be uninstalled any time — the Docker Postgres is
only reachable while the laptop is running anyway). Any Key Vault created
while troubleshooting the RBAC block was left unused and can be deleted.
The Databricks PAT should be revoked (User Settings → Developer → Access
tokens) once it's no longer needed, since it's a standing credential.
Container contents can be deleted if the account should go fully quiet;
the four pre-existing resources can be kept or removed.

---

## Pre-agreed scope cuts (not needed — all items were built as designed)

Decided in advance, in case time ran short. None of these were actually
triggered — the geolocation dimension, both mart views, and the trigger
all made it into the final build.

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
└── raw/       <entity>/run_id=<ADF RunId>/  *.parquet|*.csv
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
and the parquet sink's file name is `@concat(item().entity, pipeline().RunId)`. The notebook's `read_raw()` function reads the **exact
file** for the current run in both cases (via the shared `find_run_file()`
helper, which globs by RunId prefix rather than hardcoding an extension) —
not the whole folder, so old files from a previous run are never
accidentally included. `raw_base_path` widget =
`abfss://olistdata@<storageaccount>.dfs.core.windows.net/raw`.

**Phase 4 smoke test, adjusted for this layout:** the uploaded file had to
be **named to match the `run_id` widget value** (e.g. `manual-smoke.txt`
for `run_id = manual-smoke`) and land inside
`raw/csv/product_category_translation/` — see Phase 4 step 6.

## Evidence file naming

```
evidence/
├── 02_sources_seeded.png       psql exact counts + inbox layout
├── 03_target_ddl.png           schemas/tables/views created
├── 04_databricks_smoke.png     71-row smoke load + process log row
├── 05_adf_canvas.png           Lookup → Filters → ForEach ×2 → notebook
├── 06_monitor_green.png        per-copy row counts
├── 07_process_log.png          9 SUCCESS rows, ADF run GUID
├── 08_dq_distributions.png     flagged payments / scores / geo compression
├── 09_mart_views.png           v_sales_overview, v_payment_mix, v_review_delivery
└── 10_trigger.png              schedule trigger, recurring Mon/Fri
```
