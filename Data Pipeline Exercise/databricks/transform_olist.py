# Databricks notebook source
# MAGIC %md
# MAGIC # Olist transform notebook (raw → curated)
# MAGIC Reads the raw files landed by ADF in ADLS — csv reference files at
# MAGIC `<raw base>/csv/<entity>/<RunId>.txt`, Postgres-sourced parquet flat at
# MAGIC `<raw base>/parquet/<entity><RunId>` (see `find_run_file`/`read_raw`
# MAGIC below for why the two shapes differ — both are exact, run-scoped file
# MAGIC reads, confirmed against the real exported pipeline JSON) — applies the
# MAGIC transformations defined in `olist_transforms` (rename / cast / null
# MAGIC handling / dedup / DQ flags / ingestion metadata), writes typed tables
# MAGIC to Azure SQL via JDBC, and logs per-entity row counts to
# MAGIC `etl.pipeline_process_log`.
# MAGIC
# MAGIC Parameters are passed by the ADF Notebook activity via widgets.
# MAGIC Import BOTH files into the same workspace folder: this notebook
# MAGIC `%run`s `./olist_transforms`, a relative path, so they must sit
# MAGIC side by side (any folder works — e.g. a personal `/Users/...` folder,
# MAGIC not necessarily `/Shared/`).

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------
# Parameters (supplied by ADF: @pipeline().RunId and the raw folder path).
# `only_entity` is a debug aid for the interactive smoke test: set it to a
# single entity name (e.g. product_category_translation) to process just
# that one; leave empty (not '' — a truly empty widget) for the full
# 9-entity run. For a manual csv smoke test: reads are exact-file, RunId-
# scoped, so the uploaded file must be NAMED to match the run_id widget
# value, e.g. widget run_id=manual-smoke -> upload as
# raw/csv/<entity>/manual-smoke.txt (extension doesn't matter, only the
# name prefix does — see find_run_file()).

dbutils.widgets.text("run_id", "manual-local-run")
dbutils.widgets.text("raw_base_path", "abfss://olistdata@<storageaccount>.dfs.core.windows.net/raw")
dbutils.widgets.text("only_entity", "")

RUN_ID = dbutils.widgets.get("run_id")
RAW = dbutils.widgets.get("raw_base_path").rstrip("/")
ONLY_ENTITY = dbutils.widgets.get("only_entity").strip()

# COMMAND ----------
# ADLS access — storage account key from a Databricks-NATIVE secret scope
# (`olist-secrets`, created via the CLI — see SETUP_GUIDE Phase 4). Not
# Key-Vault-backed: this subscription doesn't allow granting Key Vault or
# any other Azure RBAC role, so secrets live in Databricks' own store
# instead. dbutils.secrets.get() is identical either way — only how the
# scope was created differs. Exercise-grade either way; Unity Catalog
# external locations / a service principal are the production answer.
# The account name is parsed from raw_base_path: abfss://raw@<acct>.dfs...

STORAGE_ACCOUNT = RAW.split("@")[1].split(".")[0]
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    dbutils.secrets.get("olist-secrets", "storage-key"),
)

# COMMAND ----------
# JDBC target config — secrets from the same scope

JDBC_URL = (
    "jdbc:sqlserver://{server}.database.windows.net:1433;"
    "database={db};encrypt=true;trustServerCertificate=false;loginTimeout=30;"
).format(
    server=dbutils.secrets.get("olist-secrets", "sql-server-name"),
    db=dbutils.secrets.get("olist-secrets", "sql-db-name"),
)
JDBC_PROPS = {
    "user": dbutils.secrets.get("olist-secrets", "sql-user"),
    "password": dbutils.secrets.get("olist-secrets", "sql-password"),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}


def write_curated(df: DataFrame, table: str) -> None:
    """Truncate-load into a pre-created Azure SQL table.

    mode('overwrite') + truncate=true keeps the DDL (types, constraints)
    defined in 01_azure_sql_ddl.sql instead of letting Spark drop and
    recreate the table with inferred types.
    """
    (
        df.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .option("user", JDBC_PROPS["user"])
        .option("password", JDBC_PROPS["password"])
        .option("driver", JDBC_PROPS["driver"])
        .option("truncate", "true")
        .option("batchsize", 10000)
        .mode("overwrite")
        .save()
    )


def log_process(entity: str, stage: str, rows_read, rows_written,
                rows_flagged, status: str, message: str,
                started_at: datetime, finished_at: datetime) -> None:
    """Append one row to etl.pipeline_process_log."""
    schema = T.StructType([
        T.StructField("pipeline_run_id", T.StringType()),
        T.StructField("entity_name", T.StringType()),
        T.StructField("stage", T.StringType()),
        T.StructField("rows_read", T.LongType()),
        T.StructField("rows_written", T.LongType()),
        T.StructField("rows_flagged", T.LongType()),
        T.StructField("status", T.StringType()),
        T.StructField("message", T.StringType()),
        T.StructField("started_at_utc", T.TimestampType()),
        T.StructField("finished_at_utc", T.TimestampType()),
    ])
    row = [(RUN_ID, entity, stage, rows_read, rows_written, rows_flagged,
            status, message, started_at, finished_at)]
    (
        spark.createDataFrame(row, schema)
        .write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "etl.pipeline_process_log")
        .option("user", JDBC_PROPS["user"])
        .option("password", JDBC_PROPS["password"])
        .option("driver", JDBC_PROPS["driver"])
        .mode("append")
        .save()
    )

# COMMAND ----------
# MAGIC %run ./olist_transforms

# COMMAND ----------
# Fail loudly on a bad only_entity value instead of silently processing
# zero entities and still reporting SUCCESS. Without this check, a typo'd
# or stale widget value makes every entity's "if entity != ONLY_ENTITY:
# continue" skip fire and the geolocation cell's guard skip too — the loop
# does no work, touches no data, calls log_process() zero times, and the
# run still exits SUCCESS in a couple of seconds. A fast "successful" run
# with nothing written anywhere is the signature of exactly this.

_VALID_ENTITIES = set(ENTITY_CONFIG.keys()) | {"geolocation"}
if ONLY_ENTITY and ONLY_ENTITY not in _VALID_ENTITIES:
    raise Exception(
        f"only_entity = '{ONLY_ENTITY}' does not match any known entity. "
        f"Valid values: {sorted(_VALID_ENTITIES)}. Leave the widget blank "
        f"to process all 9 entities.")

# COMMAND ----------
# MAGIC %md ## The 8 uniform entities (config-driven)
# MAGIC One loop over `ENTITY_CONFIG`. Per entity: read raw → transform →
# MAGIC JDBC truncate-load → process log. A failing entity is logged as
# MAGIC FAILED and the loop continues (failure isolation); the run is
# MAGIC failed at the end so ADF Monitor still shows red.

# COMMAND ----------


def find_run_file(folder: str, prefix: str) -> str:
    """Return the single file under `folder` whose name starts with `prefix`.

    Both ADF sink datasets end up writing exactly one deterministically-
    named file per run (confirmed against the exported pipeline JSON,
    adf/pl_ol_ingest_onprem_to_adls.json): the Postgres-sourced parquet
    sink's file_name is @concat(item().entity, pipeline().RunId), and the
    csv sink's file is @pipeline().RunId (with .txt appended by its format
    settings). Either way, this run's file starts with the exact value of
    RUN_ID — glob rather than hardcode the extension since ADF's format
    settings could change it.
    """
    matches = [f.path for f in dbutils.fs.ls(folder) if f.name.startswith(prefix)]
    if len(matches) != 1:
        raise Exception(
            f"expected exactly 1 file starting with '{prefix}' under {folder}, "
            f"found {len(matches)}: {matches}. If ADF's sink naming changed, "
            f"update find_run_file()/read_raw() in transform_olist.py.")
    return matches[0]


def read_raw(entity: str, cfg: dict) -> DataFrame:
    # The two ADF loops land data in DIFFERENT shapes:
    #   csv (inbox-sourced):        raw/csv/<entity>/<RunId>.txt
    #   parquet (Postgres-sourced): raw/parquet/<entity><RunId>  (flat, no
    #                               entity subfolder — entity+RunId are
    #                               concatenated into the file name instead)
    # Both are exact-file reads scoped to the current run, not directory
    # reads, so a second run can never double-count a stale file left over
    # from an earlier one.
    if cfg["source_kind"] == "csv":
        file_path = find_run_file(f"{RAW}/csv/{entity}/", RUN_ID)
        return spark.read.schema(cfg["csv_schema"]).option("header", "true").csv(file_path)

    file_path = find_run_file(f"{RAW}/parquet/", f"{entity}{RUN_ID}")
    return spark.read.parquet(file_path)


failures = []
n_processed = 0

for entity, cfg in ENTITY_CONFIG.items():
    if ONLY_ENTITY and entity != ONLY_ENTITY:
        continue
    t0 = datetime.now(timezone.utc)
    try:
        df_raw = read_raw(entity, cfg)
        n_read = df_raw.count()
        df_out = apply_transforms(df_raw, cfg, RUN_ID)
        n_written = df_out.count()
        n_flagged = count_flagged(df_out, cfg)
        write_curated(df_out, cfg["target"])
        log_process(entity, "curated_write", n_read, n_written, n_flagged,
                    "SUCCESS", f"{cfg['source_kind']} → {cfg['target']}",
                    t0, datetime.now(timezone.utc))
        n_processed += 1
        print(f"[OK]     {entity:<30} read={n_read:>9,}  written={n_written:>9,}  flagged={n_flagged}")
    except Exception as exc:
        failures.append(entity)
        log_process(entity, "curated_write", None, None, None,
                    "FAILED", str(exc)[:900], t0, datetime.now(timezone.utc))
        print(f"[FAILED] {entity}: {exc}")

# COMMAND ----------
# MAGIC %md ## Geolocation (bespoke: grain-changing aggregation)
# MAGIC 1,000,163 raw points → one row per zip prefix (~19k) via
# MAGIC `aggregate_geolocation`. `rows_flagged` = points excluded from the
# MAGIC centroids by the Brazil bounding box (counted per zip in
# MAGIC `invalid_point_count`). Full point-level fidelity stays in raw.

# COMMAND ----------

if not ONLY_ENTITY or ONLY_ENTITY == "geolocation":
    t0 = datetime.now(timezone.utc)
    try:
        geo_file = find_run_file(f"{RAW}/csv/geolocation/", RUN_ID)
        geo_raw = (
            spark.read.schema(GEOLOCATION_SCHEMA)
            .option("header", "true")
            .csv(geo_file)
        )
        n_read = geo_raw.count()
        geo_dim = aggregate_geolocation(geo_raw, RUN_ID)
        n_written = geo_dim.count()
        n_flagged = geo_dim.agg(F.sum("invalid_point_count")).collect()[0][0] or 0
        write_curated(geo_dim, "curated.geolocation")
        log_process("geolocation", "curated_write", n_read, n_written, n_flagged,
                    "SUCCESS", "csv → curated.geolocation (zip-grain dim)",
                    t0, datetime.now(timezone.utc))
        n_processed += 1
        print(f"[OK]     {'geolocation':<30} read={n_read:>9,}  written={n_written:>9,}  flagged={n_flagged}")
    except Exception as exc:
        failures.append("geolocation")
        log_process("geolocation", "curated_write", None, None, None,
                    "FAILED", str(exc)[:900], t0, datetime.now(timezone.utc))
        print(f"[FAILED] geolocation: {exc}")

# COMMAND ----------
# MAGIC %md ## Run outcome

# COMMAND ----------

if failures:
    raise Exception(f"run_id={RUN_ID}: {len(failures)} entities FAILED: {', '.join(failures)} "
                    f"(successful entities are loaded and logged; see etl.pipeline_process_log)")

dbutils.notebook.exit(f"SUCCESS run_id={RUN_ID} entities={n_processed}")
