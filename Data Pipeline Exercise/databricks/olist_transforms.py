# Databricks notebook source
# olist_transforms.py — pure transformation module for the Olist pipeline.
#
# Deliberately free of dbutils, secrets, JDBC and file I/O so the SAME code
# runs two ways:
#   - in Databricks:  %run ./olist_transforms   (from transform_olist)
#   - locally:        import olist_transforms   (see local_test/run_local_transforms.py)
#
# ENTITY_CONFIG is the single source of truth for the 8 structurally uniform
# entities (rename -> cast -> standardize -> coalesce -> DQ flags -> dedup ->
# ingestion metadata). Geolocation changes grain (1M points -> zip-prefix
# dimension) and is handled by the bespoke aggregate_geolocation() instead.
#
# DQ flag expressions are wrapped in lambdas so no Spark Column objects are
# built at import time (Column construction needs an active SparkContext).

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------
# Shared helpers


def add_ingestion_metadata(df: DataFrame, run_id: str) -> DataFrame:
    return (
        df.withColumn("ingestion_timestamp", F.current_timestamp())
          .withColumn("pipeline_run_id", F.lit(run_id))
    )


def dedup_latest(df: DataFrame, business_key: list, order_col: str) -> DataFrame:
    """Keep one row per business key (latest by order_col, nulls last)."""
    w = Window.partitionBy(*business_key).orderBy(F.col(order_col).desc_nulls_last())
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def rename_columns(df: DataFrame, mapping: dict) -> DataFrame:
    for old, new in mapping.items():
        df = df.withColumnRenamed(old, new)
    return df

# COMMAND ----------
# Explicit schemas for the CSV-sourced entities (file-source side of the
# pipeline). All-string is intentional where casts happen in config below.
# Zip prefixes MUST stay strings: they carry leading zeros ("01037").
# Reading with an explicit schema + header=true also neutralizes the UTF-8
# BOM on the translation file (the BOM sits on the discarded header line).

PRODUCTS_SCHEMA = T.StructType([
    T.StructField("product_id", T.StringType()),
    T.StructField("product_category_name", T.StringType()),
    T.StructField("product_name_lenght", T.StringType()),        # source typo, renamed in config
    T.StructField("product_description_lenght", T.StringType()),
    T.StructField("product_photos_qty", T.StringType()),
    T.StructField("product_weight_g", T.StringType()),
    T.StructField("product_length_cm", T.StringType()),
    T.StructField("product_height_cm", T.StringType()),
    T.StructField("product_width_cm", T.StringType()),
])

SELLERS_SCHEMA = T.StructType([
    T.StructField("seller_id", T.StringType()),
    T.StructField("seller_zip_code_prefix", T.StringType()),
    T.StructField("seller_city", T.StringType()),
    T.StructField("seller_state", T.StringType()),
])

GEOLOCATION_SCHEMA = T.StructType([
    T.StructField("geolocation_zip_code_prefix", T.StringType()),
    T.StructField("geolocation_lat", T.DoubleType()),
    T.StructField("geolocation_lng", T.DoubleType()),
    T.StructField("geolocation_city", T.StringType()),
    T.StructField("geolocation_state", T.StringType()),
])

TRANSLATION_SCHEMA = T.StructType([
    T.StructField("product_category_name", T.StringType()),
    T.StructField("product_category_name_english", T.StringType()),
])

# COMMAND ----------
# Typed schemas of the five transactional-entity extracts — the contract
# ADF's Postgres→parquet copies land in raw/ (types mirror the src.* DDL
# in postgres_source/postgres_seed.sql exactly). Used by
# local_test/build_dbextract_parquet.py to build the verified fallback
# parquet extracts (and the local harness input); kept here so the whole
# data contract lives in one module.

EXTRACT_SCHEMAS = {
    "orders": T.StructType([
        T.StructField("order_id", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("order_status", T.StringType()),
        T.StructField("order_purchase_timestamp", T.TimestampType()),
        T.StructField("order_approved_at", T.TimestampType()),
        T.StructField("order_delivered_carrier_date", T.TimestampType()),
        T.StructField("order_delivered_customer_date", T.TimestampType()),
        T.StructField("order_estimated_delivery_date", T.TimestampType()),
    ]),
    "customers": T.StructType([
        T.StructField("customer_id", T.StringType()),
        T.StructField("customer_unique_id", T.StringType()),
        T.StructField("customer_zip_code_prefix", T.StringType()),   # leading zeros
        T.StructField("customer_city", T.StringType()),
        T.StructField("customer_state", T.StringType()),
    ]),
    "order_items": T.StructType([
        T.StructField("order_id", T.StringType()),
        T.StructField("order_item_id", T.IntegerType()),
        T.StructField("product_id", T.StringType()),
        T.StructField("seller_id", T.StringType()),
        T.StructField("shipping_limit_date", T.TimestampType()),
        T.StructField("price", T.DecimalType(12, 2)),
        T.StructField("freight_value", T.DecimalType(12, 2)),
    ]),
    "order_payments": T.StructType([
        T.StructField("order_id", T.StringType()),
        T.StructField("payment_sequential", T.IntegerType()),
        T.StructField("payment_type", T.StringType()),
        T.StructField("payment_installments", T.IntegerType()),
        T.StructField("payment_value", T.DecimalType(12, 2)),
    ]),
    "order_reviews": T.StructType([
        T.StructField("review_id", T.StringType()),
        T.StructField("order_id", T.StringType()),
        T.StructField("review_score", T.IntegerType()),
        T.StructField("review_comment_title", T.StringType()),
        T.StructField("review_comment_message", T.StringType()),
        T.StructField("review_creation_date", T.TimestampType()),
        T.StructField("review_answer_timestamp", T.TimestampType()),
    ]),
}

# COMMAND ----------
# DQ flag builders (True = valid). Each is null-safe: isNotNull() guards come
# first so three-valued logic can never leave the flag column NULL (the
# target columns are BIT NOT NULL).

KNOWN_PAYMENT_TYPES = ["boleto", "credit_card", "debit_card", "voucher"]


def _valid_delivery_date():
    return (
        F.when(F.col("order_delivered_customer_date").isNull(), F.lit(True))   # not delivered yet: valid
         .when(F.col("order_delivered_customer_date") >= F.col("order_purchase_timestamp"), F.lit(True))
         .otherwise(F.lit(False))
    )


def _valid_payment():
    return (
        F.col("payment_value").isNotNull() & (F.col("payment_value") >= 0)
        & F.col("payment_type").isNotNull()
        & F.col("payment_type").isin(KNOWN_PAYMENT_TYPES)
    )


def _valid_review_score():
    return F.col("review_score").isNotNull() & F.col("review_score").between(1, 5)

# COMMAND ----------
# ENTITY_CONFIG — the 8 uniform entities, in processing order.
#   source_kind : how ADF landed it in raw/ ("parquet" = Postgres-sourced,
#                 "csv" = inbox-file-sourced; csv_schema applies to csv only)
#   renames     : source column -> curated column
#   casts       : column -> Spark type string (defensive on parquet inputs)
#   standardize : (column, op) with op in initcap|upper|lower|trim; trim is
#                 always applied before the op
#   coalesce    : column -> default for NULL (applied after trim)
#   dq_flags    : (flag_column, zero-arg builder returning a boolean Column)
#   dedup       : business key + tiebreak order column (latest kept)
#   target      : Azure SQL table (must exist; truncate-loaded)

ENTITY_CONFIG = {
    "orders": {
        "source_kind": "parquet",
        "csv_schema": None,
        "renames": {},
        "casts": {
            "order_purchase_timestamp": "timestamp",
            "order_approved_at": "timestamp",
            "order_delivered_carrier_date": "timestamp",
            "order_delivered_customer_date": "timestamp",
            "order_estimated_delivery_date": "timestamp",
        },
        "standardize": [("order_status", "lower")],
        "coalesce": {},
        "dq_flags": [("is_valid_delivery_date", _valid_delivery_date)],
        "dedup": {"keys": ["order_id"], "order_col": "order_purchase_timestamp"},
        "target": "curated.orders",
    },
    "customers": {
        "source_kind": "parquet",
        "csv_schema": None,
        "renames": {},
        "casts": {},
        "standardize": [("customer_city", "initcap"), ("customer_state", "upper")],
        "coalesce": {},
        "dq_flags": [],
        "dedup": {"keys": ["customer_id"], "order_col": "customer_id"},
        "target": "curated.customers",
    },
    "order_items": {
        "source_kind": "parquet",
        "csv_schema": None,
        "renames": {},
        "casts": {
            "order_item_id": "int",
            "shipping_limit_date": "timestamp",
            "price": "decimal(12,2)",
            "freight_value": "decimal(12,2)",
        },
        "standardize": [],
        "coalesce": {},
        "dq_flags": [],
        "dedup": {"keys": ["order_id", "order_item_id"], "order_col": "order_item_id"},
        "target": "curated.order_items",
    },
    "order_payments": {
        "source_kind": "parquet",
        "csv_schema": None,
        "renames": {},
        "casts": {
            "payment_sequential": "int",
            "payment_installments": "int",
            "payment_value": "decimal(12,2)",
        },
        "standardize": [("payment_type", "lower")],
        "coalesce": {},
        "dq_flags": [("is_valid_payment", _valid_payment)],
        "dedup": {"keys": ["order_id", "payment_sequential"], "order_col": "payment_sequential"},
        "target": "curated.order_payments",
    },
    "order_reviews": {
        "source_kind": "parquet",
        "csv_schema": None,
        "renames": {},
        "casts": {
            "review_score": "int",
            "review_creation_date": "timestamp",
            "review_answer_timestamp": "timestamp",
        },
        "standardize": [("review_comment_title", "trim"), ("review_comment_message", "trim")],
        "coalesce": {},
        "dq_flags": [("is_valid_review_score", _valid_review_score)],
        # review_id alone is NOT unique (789 dups); (review_id, order_id) is
        "dedup": {"keys": ["review_id", "order_id"], "order_col": "review_answer_timestamp"},
        "target": "curated.order_reviews",
    },
    "products": {
        "source_kind": "csv",
        "csv_schema": PRODUCTS_SCHEMA,
        "renames": {
            "product_name_lenght": "product_name_length",
            "product_description_lenght": "product_description_length",
        },
        "casts": {
            "product_name_length": "int",       # non-numeric -> NULL, not a failed run
            "product_description_length": "int",
            "product_photos_qty": "int",
            "product_weight_g": "int",
            "product_length_cm": "int",
            "product_height_cm": "int",
            "product_width_cm": "int",
        },
        "standardize": [],
        "coalesce": {"product_category_name": "unknown"},   # ~610 null categories
        "dq_flags": [],
        "dedup": {"keys": ["product_id"], "order_col": "product_id"},
        "target": "curated.products",
    },
    "sellers": {
        "source_kind": "csv",
        "csv_schema": SELLERS_SCHEMA,
        "renames": {},
        "casts": {},
        "standardize": [("seller_city", "initcap"), ("seller_state", "upper")],
        "coalesce": {},
        "dq_flags": [],
        "dedup": {"keys": ["seller_id"], "order_col": "seller_id"},
        "target": "curated.sellers",
    },
    "product_category_translation": {
        "source_kind": "csv",
        "csv_schema": TRANSLATION_SCHEMA,
        "renames": {},
        "casts": {},
        "standardize": [("product_category_name", "trim"),
                        ("product_category_name_english", "trim")],
        "coalesce": {},
        "dq_flags": [],
        "dedup": {"keys": ["product_category_name"], "order_col": "product_category_name"},
        "target": "curated.product_category_translation",
    },
}

# COMMAND ----------
# Generic transform pipeline for ENTITY_CONFIG entities


def apply_transforms(df: DataFrame, cfg: dict, run_id: str) -> DataFrame:
    df = rename_columns(df, cfg["renames"])
    for col, dtype in cfg["casts"].items():
        df = df.withColumn(col, F.col(col).cast(dtype))
    for col, op in cfg["standardize"]:
        trimmed = F.trim(F.col(col))
        if op == "initcap":
            df = df.withColumn(col, F.initcap(trimmed))
        elif op == "upper":
            df = df.withColumn(col, F.upper(trimmed))
        elif op == "lower":
            df = df.withColumn(col, F.lower(trimmed))
        elif op == "trim":
            df = df.withColumn(col, trimmed)
        else:
            raise ValueError(f"unknown standardize op: {op}")
    for col, default in cfg["coalesce"].items():
        df = df.withColumn(col, F.coalesce(F.trim(F.col(col)), F.lit(default)))
    for flag_col, flag_builder in cfg["dq_flags"]:
        df = df.withColumn(flag_col, flag_builder())
    if cfg["dedup"]:
        df = dedup_latest(df, cfg["dedup"]["keys"], cfg["dedup"]["order_col"])
    return add_ingestion_metadata(df, run_id)


def count_flagged(df: DataFrame, cfg: dict) -> int:
    """Rows failing at least one DQ flag (kept in the data, counted here)."""
    n = 0
    for flag_col, _ in cfg["dq_flags"]:
        n += df.filter(~F.col(flag_col)).count()
    return n

# COMMAND ----------
# Bespoke geolocation aggregation: 1,000,163 raw points -> one row per zip
# prefix (~19k). The consumers (customers.customer_zip_code_prefix,
# sellers.seller_zip_code_prefix) join at zip grain anyway; full row-level
# fidelity stays in the raw zone. Out-of-bounds points are EXCLUDED from
# the centroid but COUNTED in invalid_point_count — a zip whose points are
# all invalid is kept with NULL coordinates (flag, don't drop, at dim grain).

# Generous Brazil bounding box (includes the Atlantic islands)
BRAZIL_LAT_MIN, BRAZIL_LAT_MAX = -35.0, 6.0
BRAZIL_LNG_MIN, BRAZIL_LNG_MAX = -75.0, -28.0


def aggregate_geolocation(df: DataFrame, run_id: str) -> DataFrame:
    is_valid = (
        F.col("geolocation_lat").isNotNull() & F.col("geolocation_lng").isNotNull()
        & F.col("geolocation_lat").between(BRAZIL_LAT_MIN, BRAZIL_LAT_MAX)
        & F.col("geolocation_lng").between(BRAZIL_LNG_MIN, BRAZIL_LNG_MAX)
    )
    pts = df.withColumn("_valid", is_valid)

    centroids = (
        pts.groupBy("geolocation_zip_code_prefix")
        .agg(
            F.avg(F.when(F.col("_valid"), F.col("geolocation_lat"))).alias("latitude"),
            F.avg(F.when(F.col("_valid"), F.col("geolocation_lng"))).alias("longitude"),
            F.count(F.lit(1)).alias("point_count"),
            F.sum(F.when(~F.col("_valid"), 1).otherwise(0)).alias("invalid_point_count"),
        )
    )

    def modal(source_col: str) -> DataFrame:
        counted = (
            pts.filter(F.col(source_col).isNotNull())
            .groupBy("geolocation_zip_code_prefix", source_col)
            .agg(F.count(F.lit(1)).alias("_n"))
        )
        w = (
            Window.partitionBy("geolocation_zip_code_prefix")
            .orderBy(F.col("_n").desc(), F.col(source_col).asc())   # deterministic tie-break
        )
        return (
            counted.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .select("geolocation_zip_code_prefix", source_col)
        )

    result = (
        centroids
        .join(modal("geolocation_city"), "geolocation_zip_code_prefix", "left")
        .join(modal("geolocation_state"), "geolocation_zip_code_prefix", "left")
        .select(
            "geolocation_zip_code_prefix",
            F.col("latitude").cast("decimal(9,6)").alias("latitude"),
            F.col("longitude").cast("decimal(9,6)").alias("longitude"),
            "geolocation_city",
            "geolocation_state",
            F.col("point_count").cast("int").alias("point_count"),
            F.col("invalid_point_count").cast("int").alias("invalid_point_count"),
        )
    )
    return add_ingestion_metadata(result, run_id)
