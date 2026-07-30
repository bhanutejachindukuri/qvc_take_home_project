# Databricks notebook source
# MAGIC %md
# MAGIC # Olist transform notebook (raw → curated)
# MAGIC Reads the raw files landed by ADF in ADLS — csv reference files under
# MAGIC `<raw base>/csv/<entity>/`, Postgres-sourced parquet flat under
# MAGIC `<raw base>/parquet/` named `<entity><RunId>` (see `read_raw` below for
# MAGIC why the two shapes differ) — applies the transformations defined in
# MAGIC `olist_transforms` (rename / cast / null handling / dedup / DQ flags /
# MAGIC ingestion metadata), writes typed tables to Azure SQL via JDBC, and logs
# MAGIC per-entity row counts to `etl.pipeline_process_log`.
# MAGIC
# MAGIC Parameters are passed by the ADF Notebook activity via widgets.
# MAGIC Import BOTH files into the same workspace folder (e.g. `/Shared/`):
# MAGIC this notebook `%run`s `./olist_transforms`.

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------
# Parameters (supplied by ADF: @pipeline().RunId and the raw folder path).
# `only_entity` is a debug aid for the interactive smoke test: set it to a
# single entity name (e.g. product_category_translation) to process just
# that one; leave empty for the full 9-entity run. For a manual csv smoke
# test, just drop a file straight into raw/csv/<entity>/ — no run_id
# subfolder needed, see read_raw().

dbutils.widgets.text("run_id", "manual-local-run")
dbutils.widgets.text("raw_base_path", "abfss://olistdata@<storageaccount>.dfs.core.windows.net/raw")
dbutils.widgets.text("only_entity", "")

RUN_ID = dbutils.widgets.get("run_id")
RAW = dbutils.widgets.get("raw_base_path").rstrip("/")
ONLY_ENTITY = dbutils.widgets.get("only_entity").strip()

# COMMAND ----------
# ADLS access — storage account key from the Key-Vault-backed secret scope.
# (Exercise-grade auth; Unity Catalog external locations / a service
# principal are the production answer — see README.)
# The account name is parsed from raw_base_path: abfss://raw@<acct>.dfs...

STORAGE_ACCOUNT = RAW.split("@")[1].split(".")[0]
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    dbutils.secrets.get("kv-olist", "storage-key"),
)

# COMMAND ----------
# JDBC target config — secrets from the same scope

JDBC_URL = (
    "jdbc:sqlserver://{server}.database.windows.net:1433;"
    "database={db};encrypt=true;trustServerCertificate=false;loginTimeout=30;"
).format(
    server=dbutils.secrets.get("kv-olist", "sql-server-name"),
    db=dbutils.secrets.get("kv-olist", "sql-db-name"),
)
JDBC_PROPS = {
    "user": dbutils.secrets.get("kv-olist", "sql-user"),
    "password": dbutils.secrets.get("kv-olist", "sql-password"),
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
# MAGIC %md ## The 8 uniform entities (config-driven)
# MAGIC One loop over `ENTITY_CONFIG`. Per entity: read raw → transform →
# MAGIC JDBC truncate-load → process log. A failing entity is logged as
# MAGIC FAILED and the loop continues (failure isolation); the run is
# MAGIC failed at the end so ADF Monitor still shows red.

# COMMAND ----------


def read_raw(entity: str, cfg: dict) -> DataFrame:
    # The two ADF loops land data in DIFFERENT shapes (confirmed against
    # the actual storage browser output, not the originally designed
    # per-entity/run_id-folder layout for both — see README "Actual ADLS
    # layout" note):
    #   csv (inbox-sourced):   <raw>/csv/<entity>/<whatever ADF named it>
    #                          — a normal folder; read the whole directory.
    #   parquet (Postgres-sourced): <raw>/parquet/<entity><RunId>[.ext]
    #                          — FLAT, no entity folder; the sink's file-name
    #                          expression baked entity+RunId together instead
    #                          of using them as folder segments. RUN_ID here
    #                          is exactly ADF's @pipeline().RunId, so the
    #                          current run's file is deterministically
    #                          "<entity><RUN_ID>" (extension uncertain from
    #                          the storage browser view, hence the glob).
    if cfg["source_kind"] == "csv":
        path = f"{RAW}/csv/{entity}/"
        return spark.read.schema(cfg["csv_schema"]).option("header", "true").csv(path)

    prefix = f"{entity}{RUN_ID}"
    matches = [f.path for f in dbutils.fs.ls(f"{RAW}/parquet/") if f.name.startswith(prefix)]
    if len(matches) != 1:
        raise Exception(
            f"{entity}: expected exactly 1 file starting with '{prefix}' under "
            f"{RAW}/parquet/, found {len(matches)}: {matches}. If ADF's sink "
            f"naming changed, update read_raw() in transform_olist.py.")
    return spark.read.parquet(matches[0])


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
        geo_raw = (
            spark.read.schema(GEOLOCATION_SCHEMA)
            .option("header", "true")
            .csv(f"{RAW}/csv/geolocation/")
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
