"""Build the five transactional-entity parquet extracts (fallback source +
local-harness input).

The pipeline's primary relational source is the local Docker PostgreSQL
(seeded by postgres_source/postgres_seed.sql, read by ADF through a
Self-Hosted Integration Runtime). These extracts serve two purposes:

1. FALLBACK SOURCE — if the SHIR path is unavailable, upload each
   <entity>.parquet to the storage container `dbextract/<entity>/` and
   point the five ADF copies at it (one source-dataset swap); the files
   are typed exactly like ADF's Postgres->parquet output
   (`olist_transforms.EXTRACT_SCHEMAS` mirrors the src.* DDL).
2. HARNESS INPUT — local_test/run_local_transforms.py verifies every
   transform against these exact files before anything touches Azure.

Reads the Kaggle CSVs (default: C:\\Users\\bhanu\\Downloads) and writes one
tidy parquet file per entity to local_test/output/dbextract/<entity>/.

The reviews CSV is the one non-trivial parse: quoted fields with embedded
newlines and doubled "" quotes (99,224 records across ~104.7k physical
lines) — parsed here once, locally and verifiably; in the primary path the
same parse is done by Postgres COPY's RFC-4180 parser at seed time. Either
way no cloud component ever parses that file.

Usage:  python build_dbextract_parquet.py [<folder-with-olist-csvs>]
"""

import glob
import os
import shutil
import sys

# Required on this Windows machine BEFORE any pyspark import: worker
# processes intermittently fail to connect back without these pins.
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(HERE, "..", "databricks")))

SRC_FILES = {
    "orders": "olist_orders_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
}


def make_spark(app_name="build-dbextract"):
    # Delta configs: this Windows machine has no winutils.exe, so Spark's
    # plain-parquet writer fails at job commit (FileOutputCommitter lists
    # directories through Hadoop native IO). The Delta writer uses its own
    # commit protocol that avoids that path, so we write THROUGH Delta and
    # ship its part file — which is an ordinary parquet file — as the
    # extract artifact. On a normal machine df.write.parquet() would do.
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def read_extract_df(spark, entity, data_dir):
    import olist_transforms as ot
    reader = spark.read.schema(ot.EXTRACT_SCHEMAS[entity]).option("header", "true")
    if entity == "order_reviews":
        # multiline quoted comments; escape='"' for the doubled-"" quote style
        reader = reader.option("multiLine", "true").option("escape", '"')
    return reader.csv(os.path.join(data_dir, SRC_FILES[entity]))


def build(spark, data_dir, out_dir):
    """Write one <entity>.parquet per entity; return {entity: DataFrame}."""
    dfs = {}
    for entity in SRC_FILES:
        df = read_extract_df(spark, entity, data_dir)
        target = os.path.join(out_dir, entity)
        tmp = os.path.join(out_dir, f"_tmp_{entity}")
        # single part file via the Delta writer (see make_spark note), then
        # keep just that plain-parquet file as <entity>/<entity>.parquet
        df.coalesce(1).write.format("delta").mode("overwrite").save(tmp)
        part = glob.glob(os.path.join(tmp, "part-*.parquet"))
        assert len(part) == 1, f"{entity}: expected 1 part file, found {len(part)}"
        if os.path.isdir(target):
            shutil.rmtree(target)
        os.makedirs(target)
        os.replace(part[0], os.path.join(target, f"{entity}.parquet"))
        shutil.rmtree(tmp)
        dfs[entity] = df
    return dfs


if __name__ == "__main__":
    data_dir = sys.argv[1] if len(sys.argv) > 1 else os.environ.get(
        "OLIST_DATA_DIR", r"C:\Users\bhanu\Downloads")
    out_dir = os.path.join(HERE, "output", "dbextract")
    spark = make_spark()
    spark.sparkContext.setLogLevel("WARN")
    dfs = build(spark, data_dir, out_dir)
    print("\ndbextract parquet extracts built:")
    for entity, df in dfs.items():
        print(f"  {entity:<16} {df.count():>9,} rows -> output/dbextract/{entity}/{entity}.parquet")
    print("\nUpload each <entity>.parquet to the storage container "
          "'dbextract/<entity>/' (keep the folder-per-entity layout).")
