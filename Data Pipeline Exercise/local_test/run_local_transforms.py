"""Local verification harness: run all 9 Olist entity transforms against the
REAL Kaggle CSVs before anything touches Azure.

What it proves
  - the parquet extracts build correctly (incl. the multiline reviews parse:
    exactly 99,224 records — the same count any correct loader must report)
  - every transform in `olist_transforms` produces the expected row counts,
    dedup no-ops, DQ flag counts and metadata columns on the full dataset
  - the geolocation aggregation compresses 1,000,163 points into the
    expected 19,015 zip-prefix rows without losing any point from the counts

What it CANNOT prove (cloud-only seams, covered by SETUP_GUIDE checkpoints):
  JDBC writes to Azure SQL (incl. truncate=true), Key Vault / secret scope,
  abfss:// auth, and the ADF copies. The one simulation gap vs the cloud
  run: the notebook reads the csv-sourced entities from ADF's raw snapshot
  rather than the original file — a 1:1 structural copy by design.

Usage:  python run_local_transforms.py [<folder-with-olist-csvs>]
Exit code 0 = all checks pass.
"""

import os
import sys
import time

# Required on this Windows machine BEFORE any pyspark import
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(HERE, "..", "databricks")))
sys.path.insert(0, HERE)

from build_dbextract_parquet import SRC_FILES, build, make_spark  # noqa: E402

CSV_FILES = {
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "product_category_translation": "product_category_name_translation.csv",
    "geolocation": "olist_geolocation_dataset.csv",
}

# (rows_read, rows_written) — written == read everywhere: every dedup is a
# verified no-op guard on this dataset
EXPECTED_COUNTS = {
    "orders": (99_441, 99_441),
    "customers": (99_441, 99_441),
    "order_items": (112_650, 112_650),
    "order_payments": (103_886, 103_886),
    "order_reviews": (99_224, 99_224),
    "products": (32_951, 32_951),
    "sellers": (3_095, 3_095),
    # 71, not 70: the file has no trailing newline, so naive line counts
    # miss the last record (verified with a real CSV parse)
    "product_category_translation": (71, 71),
}
EXPECTED_FLAGGED = {"order_payments": 3, "order_reviews": 0}
EXPECTED_GEO_ZIPS = 19_015
EXPECTED_GEO_POINTS = 1_000_163
EXPECTED_UNKNOWN_PRODUCTS = 610
RUN_ID = "local-harness"

checks = []


def check(name, cond, detail=""):
    checks.append((name, bool(cond)))
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f"  ({detail})" if detail else ""))


def main():
    data_dir = sys.argv[1] if len(sys.argv) > 1 else os.environ.get(
        "OLIST_DATA_DIR", r"C:\Users\bhanu\Downloads")
    out_dir = os.path.join(HERE, "output")
    t_start = time.time()

    spark = make_spark("olist-local-harness")
    spark.sparkContext.setLogLevel("ERROR")
    import olist_transforms as ot
    from pyspark.sql import functions as F

    # ---- 1. Build the dbextract parquet files (the relational-source
    # substitute the user uploads to storage) ----
    print("\n== Building dbextract parquet extracts ==")
    extract_dfs = build(spark, data_dir, os.path.join(out_dir, "dbextract"))
    for entity, df in extract_dfs.items():
        print(f"  built {entity}")

    # Read the extracts BACK from parquet where possible — then the harness
    # consumes byte-identical input to what the notebook reads from raw/.
    # Exact FILE paths (not folders): directory listing needs the Hadoop
    # native IO this machine lacks, single-file reads do not. Falls back to
    # the in-memory frames if even that is unavailable.
    try:
        reread = {e: spark.read.parquet(
                      os.path.join(out_dir, "dbextract", e, f"{e}.parquet"))
                  for e in SRC_FILES}
        _ = {e: df.count() for e, df in reread.items()}  # force materialization
        extract_dfs = reread
        print("  parquet read-back OK — harness input is the exact upload artifact")
    except Exception as exc:
        print(f"  NOTE: local parquet read-back unavailable ({type(exc).__name__}); "
              "verifying against the pre-write DataFrames instead")

    print("\n== Extract-level checks ==")
    n_reviews = extract_dfs["order_reviews"].count()
    check("reviews multiline parse == 99,224", n_reviews == 99_224, f"got {n_reviews:,}")
    dup_review_ids = (extract_dfs["order_reviews"].groupBy("review_id").count()
                      .filter(F.col("count") > 1).count())
    print(f"  info: duplicate review_ids across orders: {dup_review_ids:,}")
    dup_pairs = (extract_dfs["order_reviews"].groupBy("review_id", "order_id").count()
                 .filter(F.col("count") > 1).count())
    check("(review_id, order_id) unique in source", dup_pairs == 0, f"{dup_pairs} dup pairs")

    # ---- 2. The 8 uniform entities through apply_transforms ----
    print("\n== Entity transforms ==")
    outputs = {}
    for entity, cfg in ot.ENTITY_CONFIG.items():
        if cfg["source_kind"] == "parquet":
            df_raw = extract_dfs[entity]
        else:
            df_raw = (spark.read.schema(cfg["csv_schema"]).option("header", "true")
                      .csv(os.path.join(data_dir, CSV_FILES[entity])))
        n_read = df_raw.count()
        df_out = ot.apply_transforms(df_raw, cfg, RUN_ID)
        n_written = df_out.count()
        n_flagged = ot.count_flagged(df_out, cfg)
        outputs[entity] = df_out

        exp_read, exp_written = EXPECTED_COUNTS[entity]
        check(f"{entity}: rows read == {exp_read:,}", n_read == exp_read, f"got {n_read:,}")
        check(f"{entity}: rows written == {exp_written:,} (dedup is a no-op)",
              n_written == exp_written, f"got {n_written:,}")
        if entity in EXPECTED_FLAGGED:
            check(f"{entity}: flagged == {EXPECTED_FLAGGED[entity]}",
                  n_flagged == EXPECTED_FLAGGED[entity], f"got {n_flagged}")
        else:
            print(f"  info: {entity} flagged={n_flagged}")
        missing_meta = [c for c in ("ingestion_timestamp", "pipeline_run_id")
                        if c not in df_out.columns]
        check(f"{entity}: ingestion metadata present", not missing_meta,
              f"missing {missing_meta}" if missing_meta else "")

    # ---- 3. Entity-specific content checks ----
    print("\n== Content checks ==")
    n_unknown = outputs["products"].filter(F.col("product_category_name") == "unknown").count()
    check(f"products: 'unknown' category == {EXPECTED_UNKNOWN_PRODUCTS}",
          n_unknown == EXPECTED_UNKNOWN_PRODUCTS, f"got {n_unknown}")

    n_null_english = (outputs["product_category_translation"]
                      .filter(F.col("product_category_name_english").isNull()).count())
    check("translation: no null english names", n_null_english == 0, f"got {n_null_english}")

    missing_translations = (
        outputs["products"].select("product_category_name").distinct()
        .filter(F.col("product_category_name") != "unknown")
        .join(outputs["product_category_translation"], "product_category_name", "left_anti")
        .collect()
    )
    print("  info: categories with no English translation (expect 2): "
          + ", ".join(sorted(r[0] for r in missing_translations)))

    payment_dist = (outputs["order_payments"].groupBy("payment_type", "is_valid_payment")
                    .count().orderBy(F.desc("count")).collect())
    print("  info: payment_type distribution: "
          + "; ".join(f"{r['payment_type']}={r['count']:,}"
                      f"{'' if r['is_valid_payment'] else ' (FLAGGED)'}" for r in payment_dist))

    score_dist = (outputs["order_reviews"].groupBy("review_score").count()
                  .orderBy("review_score").collect())
    print("  info: review score distribution: "
          + "; ".join(f"{r['review_score']}:{r['count']:,}" for r in score_dist))

    n_bad_delivery = outputs["orders"].filter(~F.col("is_valid_delivery_date")).count()
    print(f"  info: orders flagged (delivery before purchase): {n_bad_delivery}")

    # ---- 4. Geolocation aggregation ----
    print("\n== Geolocation aggregation ==")
    geo_raw = (spark.read.schema(ot.GEOLOCATION_SCHEMA).option("header", "true")
               .csv(os.path.join(data_dir, CSV_FILES["geolocation"])))
    n_geo_read = geo_raw.count()
    geo_dim = ot.aggregate_geolocation(geo_raw, RUN_ID)
    n_zips = geo_dim.count()
    total_points = geo_dim.agg(F.sum("point_count")).collect()[0][0]
    invalid_points = geo_dim.agg(F.sum("invalid_point_count")).collect()[0][0]
    null_coord_zips = geo_dim.filter(F.col("latitude").isNull()).count()

    check(f"geolocation: raw points read == {EXPECTED_GEO_POINTS:,}",
          n_geo_read == EXPECTED_GEO_POINTS, f"got {n_geo_read:,}")
    check(f"geolocation: dim rows == {EXPECTED_GEO_ZIPS:,} zip prefixes",
          n_zips == EXPECTED_GEO_ZIPS, f"got {n_zips:,}")
    check("geolocation: every raw point accounted for in point_count",
          total_points == EXPECTED_GEO_POINTS, f"sum={total_points:,}")
    check("geolocation: metadata present",
          all(c in geo_dim.columns for c in ("ingestion_timestamp", "pipeline_run_id")))
    print(f"  info: points excluded from centroids by Brazil bounds: {invalid_points:,}; "
          f"zips left with NULL coords: {null_coord_zips}")

    # ---- Summary ----
    n_fail = sum(1 for _, ok in checks if not ok)
    print("\n" + "=" * 72)
    print(f"{len(checks) - n_fail}/{len(checks)} checks passed "
          f"({time.time() - t_start:.0f}s)")
    if n_fail:
        print("FAILED checks:")
        for name, ok in checks:
            if not ok:
                print(f"  - {name}")
        return 1
    print("ALL CHECKS PASSED — transforms are verified against the full real "
          "dataset;\nready for the Azure phases (SETUP_GUIDE.md Phase 1+).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
