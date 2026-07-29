# Databricks notebook source

# COMMAND ----------
# MAGIC # QVC Data Engineer - Technical Challenge: Daily Returns Dataset Transformation Pipeline 
# MAGIC <hr style="border:2px solid gray">
# MAGIC 
# MAGIC **Objective:** Develop a robust production-ready notebook to transform a raw one-day returns dataset into a clean, analytics-ready final dataset.
# MAGIC 
# MAGIC **Time Allotment:** 4-6 hours. We are looking for :
# MAGIC 1. clean, modular code
# MAGIC 2. strong PySpark fundamentals
# MAGIC 3. sound data engineering judgement
# MAGIC 4. clear handling of data quality issues
# MAGIC 5. sensible optimisation and storage choices
# MAGIC 

# COMMAND ----------
# MAGIC ## Setup and Introduction (Candidate Preamble)
# MAGIC ---
# MAGIC 

# COMMAND ----------
# MAGIC Please fill out this section upon starting the notebook.
# MAGIC 
# MAGIC | Parameter | Response |
# MAGIC | :--- | :--- |
# MAGIC | **Candidate Name** | Bhanu Teja |
# MAGIC | **Time Spent (Approx.)** | ~5 hours |
# MAGIC | **Key Assumptions Made** | `parcellab_system_created_date` + `created_time` are assumed to represent the same instant (same format, but not verified as guaranteed-atomic in the source system) - combined into `parcellab_created_ts` with this caveat documented. `refreshed_date` and 27/1000 `activity_monitor_last_update` values are corrupted/truncated timestamp fragments (e.g. `"38:26.6"`) - left unparsed/NULL rather than force-cast. `id` looked like a unique per-record key but is **not** (905 distinct values across 1000 rows - it identifies the shipment, and one shipment can have multiple rows, one per returned `article_number`); a surrogate `_row_uid` was introduced and used for all joins/grouping instead. Nested objects inside `articles`/`custom_fields` (`customFields`, `priceDetails`, `tracking`, `outboundCustomFields`) are flattened one level only, not recursively expanded. |
# MAGIC | **Output Format Chosen** | Delta (`output/final/returns_tracking_delta` and `output/exceptions/returns_tracking_exceptions_delta`) |
# MAGIC | **Any Limitations in the Submitted Solution** | The `custom_fields` schema was derived from this single sample day and validated against it (0 parse failures), but has not been seen against other days' data. This sample file has zero organic exception rows (every row parses and matches exactly once), so the exception-handling and repair logic (Part 3) is proven via a small synthetic bad-row harness rather than real failing rows. `refreshed_date` and the 27 corrupted `activity_monitor_last_update` values are left as NULL/raw rather than repaired.

# COMMAND ----------
# MAGIC ### Business Context
# MAGIC 
# MAGIC You are provided with a sample CSV file representing one day of **returns** data.
# MAGIC The sample file contains:
# MAGIC 
# MAGIC 1. Standard scalar columns
# MAGIC 2. an **articles** column containing a list of JSON objects
# MAGIC 3. a **custom_fields** column containing a nested JSON object
# MAGIC 
# MAGIC Your task is to transform this file into a stable, analytics-ready dataset that could realistically be used as part of a daily processing pipeline.
# MAGIC Structure the notebook in a way that would be reasonable for reuse in a production-style setting.
# MAGIC Please keep cluster efficiency in mind and make sensible optimisation choices where appropriate.
# MAGIC 
# MAGIC **Note:** 
# MAGIC The provided file is only a one-day sample. Your solution should be written as though it will be used on future daily files with similar structure and possible edge cases.
# MAGIC 

# COMMAND ----------
# MAGIC ***
# MAGIC ## Part 1: Load and Inspect the Source Data. Process Scalar columns
# MAGIC 

# COMMAND ----------
# MAGIC The dataset `daily_returns_trackings_2026-07-01.csv` contains raw data with 86 columns.

# COMMAND ----------
# MAGIC ### Task 1.1: Ingest the raw file & profile raw data
# MAGIC 
# MAGIC Read the source CSV file into Spark and inspect the schema. Perform an initial review of the data and understand if there are any transformation required for scalar columns to make them readable and ready for analytical database. 
# MAGIC 
# MAGIC _**Hint** : Focus on columns having correct datatype and formats that can be later converted into relational tables. (Date and time columns)_

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType

# Databricks provides the `spark` session and Delta support. Do not create a local
# SparkSession or install Delta packages in this notebook.
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
spark.sparkContext.setLogLevel("WARN")
print("spark version", spark.version)

# This workspace uses the legacy Hive metastore. Upload the CSV through the UI into
# the input table below, then run this notebook. All outputs are managed Delta tables.
dbutils.widgets.text(
    "input_table", "hive_metastore.default.daily_returns_trackings_raw", "Input table"
)
dbutils.widgets.text(
    "output_schema", "hive_metastore.default", "Output schema"
)
INPUT_TABLE = dbutils.widgets.get("input_table").strip()
OUTPUT_SCHEMA = dbutils.widgets.get("output_schema").strip()
assert INPUT_TABLE and OUTPUT_SCHEMA, "Set both the input_table and output_schema widgets."

df_raw = spark.table(INPUT_TABLE)
# NOTE: "id" looks like a per-record Mongo-style key but is NOT unique in this file
# (905 distinct values across 1000 rows - it identifies the shipment/parcel, and a
# shipment can have multiple rows, one per returned article_number). (id, article_number)
# IS unique (1000/1000), but to be robust for future files that may not guarantee even
# that, add a true surrogate row key up front and use it as the join/group key everywhere.
df_raw = df_raw.withColumn("_row_uid", F.monotonically_increasing_id())
n_raw = df_raw.count()
print("raw row count:", n_raw)
print("raw col count:", len(df_raw.columns))
assert n_raw == 1000

# ---- profile: literal "null" string prevalence ----
null_str_counts = {}
for c in df_raw.columns:
    cnt = df_raw.filter(F.trim(F.col(c)) == "null").count()
    if cnt > 0:
        null_str_counts[c] = cnt
print("\ncolumns with literal 'null' string (col: count):")
for k, v in sorted(null_str_counts.items(), key=lambda x: -x[1]):
    print(f"  {k}: {v}")

BOOL_COLS = [
    "is_returns_portal", "return_shipment", "is_cancelled", "is_complete",
    "is_branch_delivery", "cash_on_delivery", "is_transport", "is_doorstep_delivery",
    "has_pod_identifier", "has_pod_signature", "is_contacted", "is_contacted_and_bounce",
    "invalid", "forgotten", "dispatch_delayed", "network_delayed", "failed_attempt",
    "sla_exceeded", "customer_promise_exceeded", "returned_to_sender",
]

STRING_PRESERVE_COLS = ["id", "customer_no", "order_no", "tracking_number", "delivery_no", "xid"]

DATE_TIME_PAIRS = [
    ("order_date", "order_time", "order_ts"),
    ("pickup_scheduled_date", "pickup_scheduled_time", "pickup_scheduled_ts"),
    ("inbound_scan_date", "inbound_scan_time", "inbound_scan_ts"),
    ("qualified_delivery_attempt_date", "qualified_delivery_attempt_time", "qualified_delivery_attempt_ts"),
    ("delivery_date", "delivery_time", "delivery_ts"),
    ("announced_dispatch_date", "announced_dispatch_time", "announced_dispatch_ts"),
]

DATE_ONLY_COLS = {
    "cancelled_date": "cancelled_dt",
    "promise_date": "promise_dt",
    "return_to_sender_date": "return_to_sender_dt",
    "claimed_date": "claimed_dt",
}

SINGLE_TS_COLS = {
    "updated": "updated_ts",
    "activity_monitor_last_update": "activity_monitor_last_update_ts",
}

N_DAYS_COLS = [
    "delta_between_announced_and_actual_dispatch",
    "n_days_edi_transmission_until_final_delivery",
    "n_days_courier_inbound_until_first_attempt",
    "n_days_edi_transmission_until_first_attempt",
    "n_days_edi_transmission_until_courier_inbound",
    "n_days_order_until_first_attempt",
    "n_days_order_until_courier_inbound",
    "n_days_failed_until_delivered",
    "n_days_edi_transmission_customer_collected",
]

# check each n_days col for non-numeric, non-"null" garbage before blanket cast
print("\nn_days_* / delta_* distinct-shape audit:")
for c in N_DAYS_COLS:
    bad = df_raw.filter(
        F.col(c).isNotNull() & (F.trim(F.col(c)) != "null") & (~F.col(c).rlike(r"^-?\d+$"))
    )
    bad_cnt = bad.count()
    print(f"  {c}: non-numeric-non-null count = {bad_cnt}")
    if bad_cnt > 0:
        bad.select(c).show(5, truncate=False)

# ---- step 1: global "null"-string -> real NULL, over every string column ----
df_nulled = df_raw.select(
    [F.when(F.trim(F.col(c)) == "null", None).otherwise(F.col(c)).alias(c) for c in df_raw.columns]
)

# reconciliation check
total_null_str = sum(null_str_counts.values())
after_nulls = sum(
    df_nulled.filter(F.col(c).isNull()).count() - df_raw.filter(F.col(c).isNull()).count()
    for c in null_str_counts.keys()
)
print(f"\nreconciliation: total 'null'-string sentinels found = {total_null_str}, "
      f"new real-nulls introduced = {after_nulls}")
assert total_null_str == after_nulls, "mismatch between 'null' strings found and nulls introduced"

df = df_nulled

# ---- step 2: boolean casts ----
for c in BOOL_COLS:
    df = df.withColumn(c, F.when(F.col(c).isNull(), None).otherwise(F.col(c) == F.lit("t")))

# ---- step 3: date/time pair combination ----
# Spark 4 runs in ANSI mode by default and raises on empty/malformed timestamps.
# The pipeline intentionally represents those source values as NULL instead.
def parse_timestamp_or_null(value, fmt):
    cleaned = F.when(F.trim(value) == "", F.lit(None)).otherwise(value)
    return F.try_to_timestamp(cleaned, F.lit(fmt))

# NOTE: the paired *_time columns (order_time, pickup_scheduled_time, inbound_scan_time,
# qualified_delivery_attempt_time, delivery_time, created_time) all carry seconds
# (HH:mm:ss), verified live against the raw file - NOT HH:mm as originally assumed.
for date_col, time_col, out_col in DATE_TIME_PAIRS:
    df = df.withColumn(
        out_col,
        parse_timestamp_or_null(
            F.concat_ws(" ", F.col(date_col), F.col(time_col)), "dd/MM/yyyy HH:mm:ss"
        )
    )

# parcellab pairing - documented assumption; created_time also carries seconds
df = df.withColumn(
    "parcellab_created_ts",
    parse_timestamp_or_null(
        F.concat_ws(" ", F.col("parcellab_system_created_date"), F.col("created_time")),
        "dd/MM/yyyy HH:mm:ss",
    ),
)

# updated / activity_monitor_last_update are HH:mm (no seconds). activity_monitor_last_update
# additionally has 27/1000 corrupted values (same truncated-timestamp pattern as
# refreshed_date, e.g. "15:08.8") - with timeParserPolicy=CORRECTED these fail to
# match the pattern and become NULL rather than raising, which is the desired behavior
# (silently losing 27 unparseable values is acceptable/expected here, same judgement
# call as refreshed_date; not force-fixed).
for c, out_col in SINGLE_TS_COLS.items():
    df = df.withColumn(out_col, parse_timestamp_or_null(F.col(c), "dd/MM/yyyy HH:mm"))

for c, out_col in DATE_ONLY_COLS.items():
    df = df.withColumn(
        out_col,
        F.to_date(parse_timestamp_or_null(F.col(c), "dd/MM/yyyy")),
    )

# refreshed_date: leave raw, just show sample unparseable values
print("\nrefreshed_date sample values (left unparsed, corrupted/truncated):")
df.select("refreshed_date").filter(F.col("refreshed_date").isNotNull()).show(5, truncate=False)

activity_corrupt_cnt = df.filter(
    F.col("activity_monitor_last_update").isNotNull()
    & ~F.col("activity_monitor_last_update").rlike(r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$")
).count()
print(f"\nactivity_monitor_last_update: {activity_corrupt_cnt} corrupted/truncated values "
      f"(same pattern as refreshed_date) - these become NULL in activity_monitor_last_update_ts")

# ---- step 4: article_price decimal ----
df = df.withColumn("article_price", F.col("article_price").cast(DecimalType(12, 2)))

# ---- step 5: n_days_*/delta_* -> IntegerType (after null-cleanup, all are safe) ----
for c in N_DAYS_COLS:
    df = df.withColumn(c, F.col(c).cast(IntegerType()))

# ---- step 6: assert string-preserving columns still string ----
dtypes = dict(df.dtypes)
for c in STRING_PRESERVE_COLS:
    assert dtypes[c] == "string", f"{c} expected string, got {dtypes[c]}"
print("\nstring-preserving columns confirmed still StringType:", STRING_PRESERVE_COLS)

# sample values to confirm no leading-zero loss
df.select("customer_no", "id", "order_no").show(5, truncate=False)

df_stage1 = df.cache()
n_stage1 = df_stage1.count()
print("\ndf_stage1 row count:", n_stage1)
assert n_stage1 == 1000

print("\ndf_stage1 schema:")
df_stage1.printSchema()

# COMMAND ----------
# MAGIC **Narrative Question (1.1):** Briefly summarise the key transformation challenges you identified in the source file.
# MAGIC 
# MAGIC The raw file uses the literal string `"null"` in place of real NULLs across most optional columns (18,813 sentinel occurrences across 27 columns in this file alone, e.g. `cancelled_date`, `announced_dispatch_date/time`, `promise_date`, `courier_service_level` are 100% affected). These have to be cleaned to real NULLs *before* any type casting, otherwise casts either silently produce NULLs anyway (for numeric/date columns) or, worse, get treated as valid non-null string values downstream.
# MAGIC 
# MAGIC Dates are `dd/MM/yyyy` and mostly split across separate date and time columns that need combining into a single timestamp (`order_date`+`order_time`, `pickup_scheduled_date`+`_time`, `inbound_scan_date`+`_time`, `qualified_delivery_attempt_date`+`_time`, `delivery_date`+`_time`, `announced_dispatch_date`+`_time`). The time components carry seconds (`HH:mm:ss`) even though this isn't obvious from a first glance at the column names - verified empirically before picking a format string, since guessing wrong causes Spark to throw under the default `timeParserPolicy=EXCEPTION` rather than silently mis-parsing. `updated` and `activity_monitor_last_update` are single already-combined columns (`dd/MM/yyyy HH:mm`, no seconds) - but 27/1000 `activity_monitor_last_update` values are corrupted/truncated fragments (e.g. `"38:26.6"`) rather than valid timestamps, the same corruption pattern seen in the standalone `refreshed_date` column. Both are left as NULL/raw rather than force-repaired, since there's no reliable way to reconstruct the original value.
# MAGIC 
# MAGIC Boolean-looking columns (22 of them, e.g. `is_cancelled`, `has_pod_signature`) use Postgres-style `'t'`/`'f'` string values rather than `true`/`false`, which Spark's built-in boolean cast does not recognise - these need an explicit `col == 't'` comparison instead of a naive `.cast("boolean")`.
# MAGIC 
# MAGIC `article_price` has float-precision artifacts (e.g. `81.900000000000006`) from its source system and needs a `DecimalType` cast rather than `DoubleType` to avoid carrying that imprecision into the analytics-ready table.
# MAGIC 
# MAGIC Finally, several ID-shaped columns (`id`, `customer_no`, `order_no`, `tracking_number`, `delivery_no`) must be deliberately kept as `StringType` - a numeric cast would silently drop leading zeros (`customer_no`) or lose precision on the Mongo-style hex `id`.

# COMMAND ----------
# MAGIC ***
# MAGIC ## Part 2: Transform the _articles_ and _custom_fields_ Column

# COMMAND ----------
# MAGIC ### Task 2.1: Parse and match the correct article object
# MAGIC For each row:
# MAGIC 
# MAGIC 1. Parse the _articles_ column
# MAGIC 2. Identify the single JSON object where _articleNo_ matches the rowâ€™s _article_number_
# MAGIC 3. flatten only that matched JSON object into new columns
# MAGIC 4. Prefix the flattened columns with: articles_json_
# MAGIC 5. Retain the original raw articles column in the output for traceability and auditability.
# MAGIC 6. Add useful validation checks that help identify whether parsing and matching worked correctly.
# MAGIC 
# MAGIC **Note :** Your transformation must keep the record count unchanged throughout this process.
# MAGIC 

# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

articles_item_schema = StructType([
    StructField("articleNo", StringType()),
    StructField("productId", StringType()),
    StructField("articleName", StringType()),
    StructField("articleUrl", StringType()),
    StructField("articleImageUrl", StringType()),
    StructField("articleBrand", StringType()),
    StructField("price", DoubleType()),
    StructField("articlePrice", DoubleType()),
    StructField("tax", DoubleType()),
    StructField("priceDetails", StructType([
        StructField("pricePaid", DoubleType()),
        StructField("pricePaidNoTax", DoubleType()),
    ])),
    StructField("size", StringType()),
    StructField("sizeCode", StringType()),
    StructField("color", StringType()),
    StructField("quantity", IntegerType()),
    StructField("returnReason", StringType()),
    StructField("returnReasonPath", StringType()),
    StructField("prettyReturnReason", StringType()),
    StructField("problemDescription", StringType()),
    StructField("compensationMethod", StringType()),
    StructField("returnImages", ArrayType(StringType())),
    StructField("lineNumbers", StringType()),
    StructField("itemId", StringType()),
    StructField("sku", StringType()),
    StructField("barcode", StringType()),
    StructField("prettyProductId", StringType()),
    StructField("season", StringType()),
    StructField("consignmentId", StringType()),
    StructField("weightInGrams", IntegerType()),
    StructField("siteId", StringType()),
    StructField("category", StringType()),
    StructField("articleCategory", StringType()),
    StructField("productType", StringType()),
    StructField("keepArticle", StringType()),
    StructField("invoiceNo", StringType()),
    StructField("invoiceLineItemId", StringType()),
    StructField("lineItemId", StringType()),
    StructField("fulfillmentOriginLocationId", StringType()),
    StructField("itemFulfillmentLineUniqueKey", StringType()),
    StructField("condition", StringType()),
    StructField("prettyCondition", StringType()),
    StructField("countryCodeOfOrigin", StringType()),
    StructField("countryOfManufacture", StringType()),
    StructField("harmonizedSystemCode", StringType()),
    StructField("requiresExtraMaterial", StringType()),
    StructField("returnImagesUrlList", StringType()),
    StructField("shopifyFulfillmentLineItems", StringType()),
    StructField("shopifyLineItemId", StringType()),
    StructField("customFields", StructType([
        StructField("gross_weight", StringType()),
        StructField("net_weight", StringType()),
        StructField("height", StringType()),
        StructField("width", StringType()),
        StructField("length", StringType()),
    ])),
    StructField("tracking", StructType([
        StructField("client", StringType()),
        StructField("courier", StringType()),
        StructField("deliveryNo", StringType()),
        StructField("tracking_number", StringType()),
        StructField("warehouse", StringType()),
        StructField("consignmentNo", StringType()),
        StructField("market", StringType()),
        StructField("transportNo", StringType()),
    ])),
])
articles_schema = ArrayType(articles_item_schema)

# Backslash-escaped commas inside string values break naive from_json - verified this
# regex only touches "\," and doesn't corrupt the legitimate "\n"/"\"" escapes also
# present in the data.
df_repaired = df_stage1.withColumn("articles_repaired", F.regexp_replace(F.col("articles"), r"\\,", ","))

df_arr = df_repaired.withColumn("articles_arr", F.from_json(F.col("articles_repaired"), articles_schema))

df_exploded = df_arr.select("_row_uid", "id", "article_number", F.explode("articles_arr").alias("article_obj"))

df_matched_candidates = df_exploded.filter(F.col("article_obj.articleNo") == F.col("article_number"))

match_counts = df_matched_candidates.groupBy("_row_uid").count()
print("match count distribution (validation check, computed BEFORE collapsing):")
match_counts.groupBy("count").count().show()

item_fields = [f.name for f in articles_item_schema.fields]
matched_flat = df_matched_candidates.select(
    "_row_uid",
    *[F.col(f"article_obj.{f}").alias(f"articles_json_{f}") for f in item_fields],
)

# left join back onto df_stage1 by the surrogate _row_uid (NOT "id" - id is not
# unique, see note in cell 7) to preserve row count even w/ future 0-match rows.
# If a future file has duplicate matches for some row, this left join will fan that
# row out - dedupe defensively by keeping only the first match per _row_uid.
matched_flat_dedup = matched_flat.dropDuplicates(["_row_uid"])

match_count_by_uid = match_counts.withColumnRenamed("count", "articles_match_count")

articles_parse_failed_by_uid = df_arr.select(
    "_row_uid",
    (F.col("articles").isNotNull() & F.col("articles_arr").isNull()).alias("articles_parse_failed"),
)

df_stage2 = (
    df_stage1
    .join(matched_flat_dedup, on="_row_uid", how="left")
    .join(match_count_by_uid, on="_row_uid", how="left")
    .join(articles_parse_failed_by_uid, on="_row_uid", how="left")
    .withColumn("articles_match_count", F.coalesce(F.col("articles_match_count"), F.lit(0)))
)

df_stage2 = df_stage2.cache()
n_stage2 = df_stage2.count()
print("df_stage2 row count (must stay 1000):", n_stage2)
assert n_stage2 == 1000

match_dist_final = dict(df_stage2.groupBy("articles_match_count").count().collect())
print("final articles_match_count distribution:", match_dist_final)

df_stage2.select("_row_uid", "id", "article_number", "articles_json_articleNo",
                  "articles_json_price", "articles_match_count").show(5, truncate=False)

# COMMAND ----------
# MAGIC **Narrative Question (2.1):** Explain how you ensured that only the correct article was flattened while keeping the row count unchanged.
# MAGIC 
# MAGIC The first thing this required was noticing that `id` - which reads like a per-record unique key - is actually **not unique** (905 distinct values across the file's 1000 rows). Each `id` identifies a shipment/parcel, and a single shipment can appear as multiple rows in this file, one per returned `article_number`. Joining or grouping on `id` alone would silently merge unrelated rows together and corrupt the match logic. To fix this, a surrogate row key (`_row_uid`, via `monotonically_increasing_id()`) is added immediately after ingestion and used for every join/group-by from that point on - `(id, article_number)` also happens to be unique in this file, but a synthetic surrogate is more robust for future files that may not guarantee even that.
# MAGIC 
# MAGIC The `articles` array (after repairing the backslash-escaped-comma issue - see below) is exploded per row, filtered to the element(s) where `articleNo == article_number`, and the match count per `_row_uid` is computed **before** collapsing anything - `{1: 1000}` on this file, confirming every row matched exactly once. The matched element is then flattened and **left-joined back** onto the original (unexploded) dataframe by `_row_uid`, rather than filtering the exploded frame in place or using an inner join. A left join guarantees every original row survives even if a future file has a row with zero matches (`articles_match_count = 0`), and the match-count column is retained in the output specifically so 0-match and >1-match rows can be identified downstream instead of silently disappearing or being duplicated.
# MAGIC 
# MAGIC Nested objects inside the matched item (`customFields`, `priceDetails`, `tracking`) are flattened only one level - their top-level key becomes a single `articles_json_<key>` struct-typed column rather than being recursively exploded into individual scalar columns. This was a deliberate judgement call: the task asked to flatten "that matched JSON object into new columns," which is satisfied at the top level, while avoiding an explosion of narrow, deeply-nested column names for structures unlikely to be queried directly.

# COMMAND ----------
# MAGIC ### Task 2.2: Transform the custom_fields Column
# MAGIC 
# MAGIC For each row:
# MAGIC 
# MAGIC 1. Parse the custom_fields column and flatten the nested fields into new columns.
# MAGIC 2. Prefix the flattened columns with: custom_fields_json_
# MAGIC 3. Retain the original raw custom_fields column in the output for traceability and auditability.
# MAGIC 4. Add useful validation fields to help identify whether parsing worked correctly.
# MAGIC 
# MAGIC **Note** : For nested json objects inside custom_fields, avoid opening objects with lists
# MAGIC 

# COMMAND ----------
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, DoubleType, ArrayType, MapType
)

file_ref_schema = StructType([
    StructField("url", StringType()),
    StructField("bucket", StringType()),
    StructField("objectKey", StringType()),
    StructField("type", StringType()),
])

custom_fields_schema = StructType([
    StructField("isAdditionalLabel", BooleanType()),
    StructField("isManuallyAddedLabel", BooleanType()),
    StructField("originalCourier", StringType()),
    StructField("paymentMethod", StringType()),
    StructField("printLabel", file_ref_schema),
    StructField("backupPrintLabel", StringType()),
    StructField("barCode", file_ref_schema),
    StructField("customsDocument", StringType()),
    StructField("packingSlip", StringType()),
    StructField("claimsDocument", StringType()),
    StructField("commercialInvoice", StringType()),
    StructField("freeReturnLabel", BooleanType()),
    StructField("isWarranty", BooleanType()),
    StructField("isCourierPickup", BooleanType()),
    StructField("identityPin", StringType()),
    StructField("deliveryPointId", StringType()),
    StructField("groupCode", StringType()),
    StructField("pickupDateRange", StringType()),
    StructField("selectedLocation", StringType()),
    StructField("qualityCheckExpected", BooleanType()),
    StructField("shopifyReturnData", StringType()),
    StructField("shopifyCustomerTags", StringType()),
    StructField("shopifyOrderTags", ArrayType(StringType())),
    StructField("refundMethod", StringType()),
    StructField("labelCost", StringType()),
    StructField("changedAddress", BooleanType()),
    StructField("customerService", BooleanType()),
    StructField("secondHandCustomer", BooleanType()),
    StructField("outboundCustomFields", StructType([
        StructField("PaymentType", StringType()),
        StructField("ShipType", StringType()),
        StructField("ParcelShopId", StringType()),
        StructField("isNewCustomer", BooleanType()),
        StructField("ShippingAmount", DoubleType()),
        # InvoiceAddr / nameNoSurname are literal "<PII>" placeholder strings in the
        # sample data. In a real pipeline these should not be persisted downstream
        # un-redacted - flagged here, redaction itself is out of scope for this exercise.
        StructField("InvoiceAddr", StringType()),
        StructField("nameNoSurname", StringType()),
        StructField("carrier_id", StringType()),
        StructField("ExternOrderReference", StringType()),
        StructField("SalesChannel", StringType()),
        StructField("GuestFlag", StringType()),
        StructField("CustomerType", StringType()),
        StructField("CustomerSource", StringType()),
        StructField("Tags", StringType()),
        StructField("Cohort", StringType()),
        StructField("Segmentation", StringType()),
        StructField("OptIn", StringType()),
        StructField("InvoiceAmount", DoubleType()),
        StructField("PaymentMode", StringType()),
        StructField("OrderType", StringType()),
        StructField("LatestEvent", StringType()),
        StructField("LatestEventDate", StringType()),
        StructField("shortOrderNo", StringType()),
        StructField("totalPrice", DoubleType()),
        StructField("courierServiceLevel", StringType()),
        StructField("FirstPayment", StringType()),
        StructField("installmentPlanDisplay", ArrayType(StringType())),
        StructField("packslips", ArrayType(StringType())),
        # dynamically-numbered keys (primary_skn0, related_skn0, primary_skn1, ...)
        # with no fixed/bounded schema across rows - MapType avoids guessing a max index.
        StructField("productRecommendations", MapType(StringType(), StringType())),
    ])),
    StructField("currency", StringType()),
    StructField("currencyCode", StringType()),
    StructField("returnLabelsAdditional", ArrayType(StringType())),
    StructField("rmaStatus", StringType()),
    StructField("rmaStatusReason", StringType()),
    StructField("externalRMAId", StringType()),
    StructField("stateProvince", StringType()),
    StructField("returnPaymentInfo", StructType([
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("pspReference", StringType()),
        StructField("merchantReference", StringType()),
    ])),
    StructField("returnCourierFee", StructType([
        StructField("value", DoubleType()),
        StructField("currency", StringType()),
    ])),
    StructField("pickupWindow", StringType()),
    StructField("pickupConfirmationCode", StringType()),
    StructField("totalPrice", DoubleType()),
    StructField("shortOrderNo", StringType()),
    StructField("pickupWindowFormatted", StringType()),
    # NOTE: from_json's default PERMISSIVE mode on a StructType schema (unlike
    # ArrayType) almost never returns a NULL struct for malformed-but-non-empty JSON -
    # it silently nulls out unparseable fields instead (verified empirically). The
    # only reliable way to detect a genuinely corrupt custom_fields record is to
    # include a _corrupt_record column and pass columnNameOfCorruptRecord.
    StructField("_corrupt_record", StringType()),
])

df_cf_repaired = df_stage2.withColumn("custom_fields_repaired", F.regexp_replace(F.col("custom_fields"), r"\\,", ","))
df_cf_parsed = df_cf_repaired.withColumn(
    "custom_fields_json",
    F.from_json(
        F.col("custom_fields_repaired"), custom_fields_schema,
        {"columnNameOfCorruptRecord": "_corrupt_record"},
    ),
)

parse_failed_count = df_cf_parsed.filter(
    F.col("custom_fields_json._corrupt_record").isNotNull()
).count()
print("custom_fields parse failures (validation check, expect 0 - schema verified against this file):", parse_failed_count)

cf_fields = [f.name for f in custom_fields_schema.fields if f.name != "_corrupt_record"]

# ---- finalize df_stage3: flatten top-level keys, keep raw, add parse-failed flag ----
df_stage3 = df_cf_parsed.withColumn(
    "custom_fields_parse_failed",
    F.col("custom_fields_json._corrupt_record").isNotNull()
)
for f in cf_fields:
    df_stage3 = df_stage3.withColumn(f"custom_fields_json_{f}", F.col(f"custom_fields_json.{f}"))

df_stage3 = df_stage3.cache()
n_stage3 = df_stage3.count()
print("df_stage3 row count:", n_stage3)
assert n_stage3 == 1000
print("custom_fields_parse_failed count:", df_stage3.filter("custom_fields_parse_failed").count())

# COMMAND ----------
# MAGIC **Narrative Question (2.2):** Briefly describe your approach to flattening custom_fields and handling any malformed rows.
# MAGIC 
# MAGIC `custom_fields` is a single JSON object per row (not an array), repaired with the same backslash-escaped-comma fix used for `articles`, then parsed with an explicit `StructType` schema built from profiling the raw JSON keys in this sample file. That schema was **not** assumed correct - it was validated live against all 1000 rows before being trusted (0 parse failures, per-field null rates checked against a plain Python `json.loads` spot-check to rule out a silently-mismatched field name).
# MAGIC 
# MAGIC One non-obvious finding while validating: Spark's `from_json` in its default PERMISSIVE mode almost never returns a NULL struct for malformed-but-non-empty JSON against a `StructType` schema - it just nulls out whichever fields it can't parse and returns a mostly-empty struct. Detecting a genuinely corrupt record therefore requires adding a `_corrupt_record` field to the schema and passing `columnNameOfCorruptRecord` as an option to `from_json`; without that, a malformed `custom_fields` value would pass through completely undetected. (Note: this only applies to struct-shaped JSON - `from_json` against an `ArrayType` schema, as used for `articles`, does correctly return NULL on a genuinely malformed array, which was also verified empirically.)
# MAGIC 
# MAGIC Only top-level keys are flattened into `custom_fields_json_<key>` columns, consistent with the same one-level judgement call made for `articles`. Two specific nested shapes needed special handling rather than naive flattening, per the task's "avoid opening objects with lists" instruction: `outboundCustomFields.productRecommendations` has dynamically-numbered keys (`primary_skn0`, `related_skn0`, `primary_skn1`, ...) with no fixed/bounded schema across rows, so it's modelled as a `MapType(StringType, StringType)` rather than a struct with guessed field names. `installmentPlanDisplay`, `packslips`, `returnLabelsAdditional`, and `shopifyOrderTags` are all `array<string>` and are kept as single array-typed columns rather than exploded per element. Separately, `outboundCustomFields.InvoiceAddr` and `nameNoSurname` are literal `"<PII>"` placeholder strings in this sample data - flagged here as a real-pipeline redaction concern (these should not be persisted downstream un-redacted), though redaction itself is out of scope for this exercise.

# COMMAND ----------
# MAGIC ***
# MAGIC ## Part 3: Exception Handling
# MAGIC 

# COMMAND ----------
# MAGIC ### Task 3.1: Identify problematic rows
# MAGIC 
# MAGIC Create logic to identify rows where:
# MAGIC 
# MAGIC 1. Parsing failed
# MAGIC 2. The articles match could not be found
# MAGIC 3. Duplicate matches were found or other transformation issues occurred

# COMMAND ----------
df_stage4 = (
    df_stage3
    .withColumn("_articles_no_match", F.col("articles_match_count") == 0)
    .withColumn("_articles_duplicate_match", F.col("articles_match_count") > 1)
    .withColumn(
        "exception_type",
        F.when(F.col("articles_parse_failed"), F.lit("articles_parse_failed"))
         .when(F.col("articles_match_count") == 0, F.lit("articles_no_match"))
         .when(F.col("articles_match_count") > 1, F.lit("articles_duplicate_match"))
         .when(F.col("custom_fields_parse_failed"), F.lit("custom_fields_parse_failed"))
         .otherwise(F.lit(None)),
    )
    .withColumn("has_exception", F.col("exception_type").isNotNull())
)

n_exceptions_real = df_stage4.filter("has_exception").count()
print("real-data exception count (expected 0 - every row matched exactly once, both schemas parse clean):", n_exceptions_real)

# ---------------- reusable repair/parse functions (same logic as cells 11 & 14) ----------------
# Factored out here so the synthetic bad-row test below - and the repair logic in the
# next cell - literally re-invoke this logic rather than reimplement it.
def repair_json_col(col):
    return F.regexp_replace(col, r"\\,", ",")

def parse_and_match_articles(df, articles_col="articles", article_number_col="article_number", row_key="_row_uid"):
    df_r = df.withColumn("articles_repaired", repair_json_col(F.col(articles_col)))
    df_a = df_r.withColumn("articles_arr", F.from_json(F.col("articles_repaired"), articles_schema))
    parse_failed = df_a.select(row_key, (F.col(articles_col).isNotNull() & F.col("articles_arr").isNull()).alias("articles_parse_failed"))
    exploded = df_a.select(row_key, article_number_col, F.explode_outer("articles_arr").alias("article_obj"))
    matched = exploded.filter(F.col("article_obj.articleNo") == F.col(article_number_col))
    match_counts = matched.groupBy(row_key).count().withColumnRenamed("count", "articles_match_count")
    matched_flat = matched.select(
        row_key, *[F.col(f"article_obj.{f}").alias(f"articles_json_{f}") for f in item_fields]
    ).dropDuplicates([row_key])
    out = (
        df.join(matched_flat, on=row_key, how="left")
          .join(match_counts, on=row_key, how="left")
          .join(parse_failed, on=row_key, how="left")
          .withColumn("articles_match_count", F.coalesce(F.col("articles_match_count"), F.lit(0)))
    )
    return out

def parse_custom_fields(df, custom_fields_col="custom_fields"):
    df_r = df.withColumn("custom_fields_repaired", repair_json_col(F.col(custom_fields_col)))
    df_p = df_r.withColumn(
        "custom_fields_json",
        F.from_json(
            F.col("custom_fields_repaired"), custom_fields_schema,
            {"columnNameOfCorruptRecord": "_corrupt_record"},
        ),
    )
    df_p = df_p.withColumn(
        "custom_fields_parse_failed",
        F.col("custom_fields_json._corrupt_record").isNotNull()
    )
    return df_p

def flag_exceptions(df):
    return (
        df.withColumn(
            "exception_type",
            F.when(F.col("articles_parse_failed"), F.lit("articles_parse_failed"))
             .when(F.col("articles_match_count") == 0, F.lit("articles_no_match"))
             .when(F.col("articles_match_count") > 1, F.lit("articles_duplicate_match"))
             .when(F.col("custom_fields_parse_failed"), F.lit("custom_fields_parse_failed"))
             .otherwise(F.lit(None)),
        )
        .withColumn("has_exception", F.col("exception_type").isNotNull())
    )

# ---------------- synthetic bad-row test harness ----------------
# This sample file has zero organic exception rows, so the only way to prove this
# logic actually works is to exercise it against fabricated bad rows:
#   SYN-A: articles JSON that stays malformed even after the backslash-comma repair
#          (missing closing bracket) -> parse fails
#   SYN-B: valid articles array, but no element's articleNo matches article_number at all
#   SYN-C: valid articles (1 clean match) but custom_fields JSON is malformed
#   SYN-D: articleNo differs from article_number only by whitespace/case -> repairable
#   SYN-E: valid articles array, genuinely no match, no near-match either -> unrepairable
# Built via SQL VALUES (not spark.createDataFrame(list, ...)) - the latter round-trips
# through a Python worker for RDD-based parallelize/serialization, which crashes
# ("Python worker exited unexpectedly") on this pyspark 3.5.1 + Python 3.12 combo on
# Windows. SQL VALUES stays entirely on the JVM side and is also more portable to
# Databricks. All native SQL transforms used elsewhere in this notebook (regexp_replace,
# from_json, joins, explode) are unaffected by this - only Python-object parallelize is.
df_exception_demo_raw = spark.sql(r"""
    SELECT * FROM VALUES
        ('SYN-A', 1001L, '[{"articleNo":"X1","price":10.0', 'X1', '{"isAdditionalLabel":false}'),
        ('SYN-B', 1002L, '[{"articleNo":"X1","price":10.0}]', 'DOES-NOT-EXIST', '{"isAdditionalLabel":false}'),
        ('SYN-C', 1003L, '[{"articleNo":"X1","price":10.0}]', 'X1', '{"isAdditionalLabel":false'),
        ('SYN-D', 1004L, '[{"articleNo":" x1 ","price":10.0}]', 'X1', '{"isAdditionalLabel":false}'),
        ('SYN-E', 1005L, '[{"articleNo":"Q9","price":10.0}]', 'X1', '{"isAdditionalLabel":false}')
    AS t(id, _row_uid, articles, article_number, custom_fields)
""")

df_demo = parse_and_match_articles(df_exception_demo_raw)
df_demo = parse_custom_fields(df_demo)
df_demo = flag_exceptions(df_demo)

print("\nsynthetic exception-demo results:")
df_demo.select("id", "articles_parse_failed", "articles_match_count", "custom_fields_parse_failed", "exception_type").show(truncate=False)

demo_types = {r["id"]: r["exception_type"] for r in df_demo.select("id", "exception_type").collect()}
print("demo exception types:", demo_types)
assert demo_types["SYN-A"] == "articles_parse_failed", demo_types
assert demo_types["SYN-B"] == "articles_no_match", demo_types
assert demo_types["SYN-C"] == "custom_fields_parse_failed", demo_types

# COMMAND ----------
# MAGIC ### Task 3.2: Correct exception rows where possible
# MAGIC Where possible, attempt to correct and reprocess exception rows rather than only flagging them.

# COMMAND ----------
def attempt_articles_repair(df, row_key="_row_uid"):
    """For rows whose articles JSON failed to parse even after the backslash-comma
    repair, try a cheap best-effort fixup (append a likely-missing closing
    bracket/brace) and re-attempt the parse + match."""
    candidates = (
        df.filter(F.col("articles_parse_failed") == True)
          .withColumn("articles_repaired", repair_json_col(F.col("articles")))
          .select(row_key, "articles_repaired", "article_number")
    )
    if candidates.limit(1).count() == 0:
        return df.withColumn("_articles_repair_success", F.lit(False)).select(
            row_key, "_articles_repair_success",
            *[F.lit(None).alias(f"repaired_articles_json_{f}") for f in item_fields])

    fixed = candidates.withColumn("articles_fix_attempt", F.concat(F.col("articles_repaired"), F.lit("}]")))
    fixed = fixed.withColumn("articles_fix_arr", F.from_json(F.col("articles_fix_attempt"), articles_schema))
    exploded_fix = fixed.select(row_key, "article_number", F.explode_outer("articles_fix_arr").alias("obj"))
    matched_fix = exploded_fix.filter(F.col("obj.articleNo") == F.col("article_number"))
    match_ct = matched_fix.groupBy(row_key).count()
    unique_match = match_ct.filter("count = 1").select(row_key)
    matched_fix_unique = matched_fix.join(unique_match, on=row_key, how="inner")
    repaired_flat = matched_fix_unique.select(
        row_key,
        F.lit(True).alias("_articles_repair_success"),
        *[F.col(f"obj.{f}").alias(f"repaired_articles_json_{f}") for f in item_fields],
    )
    return repaired_flat


def attempt_no_match_repair(df, row_key="_row_uid"):
    """For rows with articles_match_count == 0 (parsed fine, no exact articleNo match),
    retry the match using a trimmed/case-insensitive comparison as a fallback."""
    candidates = (
        df.filter((F.col("articles_match_count") == 0) & (F.col("articles_parse_failed") == False))
          .withColumn("articles_repaired", repair_json_col(F.col("articles")))
          .select(row_key, "articles_repaired", "article_number")
    )
    if candidates.limit(1).count() == 0:
        return df.limit(0).select(row_key).withColumn("_no_match_repair_success", F.lit(False)) \
                  .select(row_key, "_no_match_repair_success",
                          *[F.lit(None).alias(f"repaired_articles_json_{f}") for f in item_fields])

    arr = candidates.withColumn("articles_arr", F.from_json(F.col("articles_repaired"), articles_schema))
    exploded = arr.select(row_key, "article_number", F.explode_outer("articles_arr").alias("obj"))
    fuzzy_matched = exploded.filter(
        F.upper(F.trim(F.col("obj.articleNo"))) == F.upper(F.trim(F.col("article_number")))
    )
    match_ct = fuzzy_matched.groupBy(row_key).count()
    unique_match = match_ct.filter("count = 1").select(row_key)
    fuzzy_unique = fuzzy_matched.join(unique_match, on=row_key, how="inner")
    repaired_flat = fuzzy_unique.select(
        row_key,
        F.lit(True).alias("_no_match_repair_success"),
        *[F.col(f"obj.{f}").alias(f"repaired_articles_json_{f}") for f in item_fields],
    )
    return repaired_flat


def apply_repairs(df, row_key="_row_uid"):
    parse_fix = attempt_articles_repair(df, row_key)
    no_match_fix = attempt_no_match_repair(df, row_key)

    df2 = df.join(
        parse_fix.select(row_key, "_articles_repair_success",
                          *[F.col(f"repaired_articles_json_{f}").alias(f"pf_{f}") for f in item_fields]),
        on=row_key, how="left",
    ).join(
        no_match_fix.select(row_key, "_no_match_repair_success",
                             *[F.col(f"repaired_articles_json_{f}").alias(f"nm_{f}") for f in item_fields]),
        on=row_key, how="left",
    )
    df2 = df2.withColumn("_articles_repair_success", F.coalesce(F.col("_articles_repair_success"), F.lit(False)))
    df2 = df2.withColumn("_no_match_repair_success", F.coalesce(F.col("_no_match_repair_success"), F.lit(False)))
    df2 = df2.withColumn("repair_attempted", F.col("has_exception"))
    df2 = df2.withColumn("repaired", F.col("_articles_repair_success") | F.col("_no_match_repair_success"))

    # overwrite the affected articles_json_* columns where a repair succeeded
    for f in item_fields:
        df2 = df2.withColumn(
            f"articles_json_{f}",
            F.when(F.col("_articles_repair_success"), F.col(f"pf_{f}"))
             .when(F.col("_no_match_repair_success"), F.col(f"nm_{f}"))
             .otherwise(F.col(f"articles_json_{f}")),
        )
    df2 = df2.withColumn(
        "exception_type",
        F.when(F.col("repaired"), F.lit(None)).otherwise(F.col("exception_type"))
    ).withColumn(
        "has_exception",
        F.when(F.col("repaired"), F.lit(False)).otherwise(F.col("has_exception"))
    )
    drop_cols = [f"pf_{f}" for f in item_fields] + [f"nm_{f}" for f in item_fields] \
        + ["_articles_repair_success", "_no_match_repair_success"]
    return df2.drop(*drop_cols)


# ---- apply to real data (no-op expected: 0 organic exceptions) ----
df_stage5 = apply_repairs(df_stage4)
df_stage5 = df_stage5.cache()
n_stage5 = df_stage5.count()
print("df_stage5 row count:", n_stage5)
assert n_stage5 == 1000
print("real-data repaired count (expect 0, nothing to repair):", df_stage5.filter("repaired").count())
print("real-data still-has_exception count (expect 0):", df_stage5.filter("has_exception").count())

# ---- demonstrate against the synthetic exception rows ----
df_demo_repaired = apply_repairs(df_demo, row_key="_row_uid")
print("\nsynthetic repair-demo results:")
df_demo_repaired.select("id", "exception_type", "has_exception", "repair_attempted", "repaired").orderBy("id").show(truncate=False)

demo_repair_state = {r["id"]: (r["repaired"], r["has_exception"]) for r in df_demo_repaired.select("id", "repaired", "has_exception").collect()}
print("demo repair state:", demo_repair_state)

# SYN-A: articles JSON was truncated but fixable by appending "}]" -> should repair
assert demo_repair_state["SYN-A"][0] is True, demo_repair_state
# SYN-D: case/whitespace mismatch -> fixable via fuzzy match -> should repair
assert demo_repair_state["SYN-D"][0] is True, demo_repair_state
# SYN-B, SYN-E: genuinely no matching article at all -> cannot be repaired
assert demo_repair_state["SYN-B"][0] is False and demo_repair_state["SYN-B"][1] is True, demo_repair_state
assert demo_repair_state["SYN-E"][0] is False and demo_repair_state["SYN-E"][1] is True, demo_repair_state
# SYN-C: custom_fields totally malformed, no repair attempted for that path -> stays unresolved
assert demo_repair_state["SYN-C"][0] is False and demo_repair_state["SYN-C"][1] is True, demo_repair_state

print("\nAt least one synthetic row repaired (SYN-A, SYN-D) and at least one stays genuinely unresolved (SYN-B, SYN-C, SYN-E) - repair logic proven end to end.")

# COMMAND ----------
# MAGIC ### Task 3.3: Route unresolved records
# MAGIC Any rows that still cannot be processed correctly should be written to a separate exceptions output with useful diagnostic fields.

# COMMAND ----------
diagnostic_cols = ["_row_uid", "id", "order_no", "exception_type", "articles", "custom_fields",
                    "order_ts", "updated_ts"]

df_unresolved = df_stage5.filter(F.col("has_exception") & ~F.col("repaired")).select(*diagnostic_cols)
df_success = df_stage5.filter(~(F.col("has_exception") & ~F.col("repaired")))

n_unresolved = df_unresolved.count()
n_success = df_success.count()
print("df_success count:", n_success, " df_unresolved count:", n_unresolved)
assert n_success + n_unresolved == 1000
print("real-data unresolved (expect 0):", n_unresolved)

# demonstrate routing on the synthetic set end-to-end
demo_unresolved = df_demo_repaired.filter(F.col("has_exception") & ~F.col("repaired"))
demo_success = df_demo_repaired.filter(~(F.col("has_exception") & ~F.col("repaired")))
print("\nsynthetic routing demo:")
print("  success:", [r["id"] for r in demo_success.select("id").orderBy("id").collect()])
print("  unresolved:", [r["id"] for r in demo_unresolved.select("id").orderBy("id").collect()])
assert demo_success.count() + demo_unresolved.count() == df_demo_repaired.count()
assert set(r["id"] for r in demo_unresolved.select("id").collect()) == {"SYN-B", "SYN-C", "SYN-E"}
assert set(r["id"] for r in demo_success.select("id").collect()) == {"SYN-A", "SYN-D"}

# COMMAND ----------
# MAGIC __Narrative Question 3:__ What kinds of exception cases did you encounter, and how did you decide whether to repair or isolate them?
# MAGIC 
# MAGIC This sample file has **zero organic exception rows** - every row's `articles` and `custom_fields` JSON parses cleanly, and every row matches exactly one article. That made it impossible to validate the exception-handling logic against real failures, so a small synthetic harness (5 fabricated rows, reusing the exact same repair/parse functions as the main pipeline - not a reimplementation) was built to exercise every exception path the task describes, since the notebook explicitly warns future daily files may exhibit edge cases this sample doesn't:
# MAGIC 
# MAGIC - **Articles JSON parse failure** (truncated/malformed JSON even after the backslash-comma repair) - a best-effort secondary repair is attempted (appending a plausibly-missing closing bracket/brace and re-parsing); if that recovers a valid, uniquely-matching article, the row is marked repaired. If not, it's routed to the exceptions output with the raw JSON retained for diagnosis.
# MAGIC - **No matching article** (`articles_match_count == 0`) - a fallback trimmed/case-insensitive comparison between `articleNo` and `article_number` is attempted first (catches whitespace/casing mismatches); if that still finds no unique match, the row is genuinely unresolvable and routed to exceptions.
# MAGIC - **Duplicate matches** (`articles_match_count > 1`) - flagged but not automatically resolved, since picking one of several equally-valid matches would be a data integrity risk rather than a real repair; these are isolated for manual review.
# MAGIC - **custom_fields parse failure** - detected via the `_corrupt_record` mechanism (see Narrative 2.2); no automatic repair is attempted here since a malformed nested JSON object doesn't have an obvious cheap fixup the way a missing trailing bracket does, so these are isolated directly.
# MAGIC 
# MAGIC The general principle: attempt a **cheap, low-risk, deterministic** repair only where one clearly exists (a likely-truncated bracket, a whitespace/case mismatch); anything requiring a judgement call about *which* value is correct (duplicate matches, genuinely malformed nested objects) is isolated to the exceptions output with full diagnostic context (raw `articles`/`custom_fields`, `exception_type`, key identifiers) rather than guessed at automatically.

# COMMAND ----------
# MAGIC ***
# MAGIC ## Part 4: Build the Final Dataset

# COMMAND ----------
# MAGIC ### Task 4.1: Produce the final transformed dataset
# MAGIC 
# MAGIC Create a final dataset that:
# MAGIC 
# MAGIC 1. Retains all original source columns unless you justify otherwise
# MAGIC 2. Retains raw complex columns
# MAGIC 3. Includes flattened articles_json_* columns
# MAGIC 4. Includes flattened custom_fields_json_* columns
# MAGIC 5. Includes useful validation / debug columns
# MAGIC 

# COMMAND ----------
articles_json_cols = [c for c in df_stage5.columns if c.startswith("articles_json_")]
custom_fields_json_cols = [c for c in df_stage5.columns if c.startswith("custom_fields_json_")]
validation_cols = [
    "articles_match_count", "articles_parse_failed", "custom_fields_parse_failed",
    "exception_type", "has_exception", "repair_attempted", "repaired",
]

# base_cols = every original scalar column from df_stage1 (the 86 raw source columns,
# retained unless justified otherwise) PLUS the additive combined date/time columns
# added in cell 7 - both are kept side by side (not a replacement) per the task's
# "retain all original source columns" instruction.
base_cols = df_stage1.columns

final_col_order = list(dict.fromkeys(base_cols + articles_json_cols + custom_fields_json_cols + validation_cols))
print("final column count:", len(final_col_order))

df_final = df_success.select(*final_col_order)

n_final = df_final.count()
print("df_final row count:", n_final)
assert n_final == 1000

# spot-check articles_json_* / custom_fields_json_* against the raw JSON for one row
sample = df_final.select("_row_uid", "id", "article_number", "articles_json_articleNo",
                          "articles_json_price", "custom_fields_json_isAdditionalLabel",
                          "custom_fields_json_originalCourier").limit(3)
sample.show(truncate=False)

# COMMAND ----------
# MAGIC ### Task 4.2: Optimise datatypes
# MAGIC Store the final dataset with sensible data types to support downstream performance and usability.

# COMMAND ----------
from pyspark.sql.types import DecimalType as _DecimalType

df_final_typed = (
    df_final
    .withColumn("articles_json_price", F.col("articles_json_price").cast(_DecimalType(12, 2)))
    .withColumn("articles_json_articlePrice", F.col("articles_json_articlePrice").cast(_DecimalType(12, 2)))
    .withColumn(
        "articles_json_priceDetails",
        F.struct(
            F.col("articles_json_priceDetails.pricePaid").cast(_DecimalType(12, 2)).alias("pricePaid"),
            F.col("articles_json_priceDetails.pricePaidNoTax").cast(_DecimalType(12, 2)).alias("pricePaidNoTax"),
        ),
    )
)

n_typed = df_final_typed.count()
print("df_final_typed row count:", n_typed)
assert n_typed == 1000

dtypes = dict(df_final_typed.dtypes)

# string-preserving columns must remain string
for c in STRING_PRESERVE_COLS:
    assert dtypes[c] == "string", f"{c} expected string, got {dtypes[c]}"

# boolean columns must be boolean
for c in BOOL_COLS:
    assert dtypes[c] == "boolean", f"{c} expected boolean, got {dtypes[c]}"

# n_days_*/delta_* must be int
for c in N_DAYS_COLS:
    assert dtypes[c] == "int", f"{c} expected int, got {dtypes[c]}"

assert dtypes["article_price"].startswith("decimal"), dtypes["article_price"]
assert dtypes["articles_json_price"].startswith("decimal"), dtypes["articles_json_price"]

# heuristic: flag any is_/has_-prefixed column still typed as plain string
# (raw source *_date/*_time columns are expected to stay string - they're kept
# alongside the combined _ts/_dt columns for traceability, not replaced)
suspect = [
    c for c, t in dtypes.items()
    if t == "string" and (c.startswith("is_") or c.startswith("has_"))
]
print("\nboolean-prefixed columns still string (should be empty):", suspect)
assert suspect == []

print("\nfinal schema (df_final_typed):")
df_final_typed.printSchema()

# COMMAND ----------
# MAGIC ### Task 4.3: Save Final Dataset in the proper format
# MAGIC Store the final dataset with proper format and file name.
# MAGIC 
# MAGIC **Note:** Send the final dataset and exception dataset (if applicable) along with your solution databricks notebook.

# COMMAND ----------
FINAL_TABLE = f"{OUTPUT_SCHEMA}.returns_tracking_final"
EXCEPTIONS_TABLE = f"{OUTPUT_SCHEMA}.returns_tracking_exceptions"

# 1000 rows/day - a single unpartitioned Delta table is the right call; partitioning
# by ingestion date only becomes worthwhile once multiple days accumulate (would
# otherwise fragment this into tiny single-partition files with no query benefit).
df_final_typed.write.format("delta").mode("overwrite").saveAsTable(FINAL_TABLE)
df_unresolved.write.format("delta").mode("overwrite").saveAsTable(EXCEPTIONS_TABLE)

# Read back to confirm the managed Delta table writes round-trip correctly.
n_final_rb = spark.table(FINAL_TABLE).count()
n_exc_rb = spark.table(EXCEPTIONS_TABLE).count()

print("read-back final count:", n_final_rb, " expected:", df_final_typed.count())
print("read-back exceptions count:", n_exc_rb, " expected:", df_unresolved.count())
assert n_final_rb == df_final_typed.count()
assert n_exc_rb == df_unresolved.count()

# COMMAND ----------
# MAGIC ***
# MAGIC ## Part 5: Quality Summary

# COMMAND ----------
# MAGIC 
# MAGIC ### Task 5.1: Produce transformation summary metrics
# MAGIC 
# MAGIC Create a summary showing at minimum:
# MAGIC 
# MAGIC 1. Total input rows
# MAGIC 2. Successfully processed rows
# MAGIC 3. Rows requiring exception handling
# MAGIC 4. Rows successfully repaired
# MAGIC 5. Rows still unresolved
# MAGIC 

# COMMAND ----------
total_input_rows = n_raw
requiring_exception_handling = n_exceptions_real  # from Part 3.1, pre-repair, on df_stage4
successfully_repaired = df_stage5.filter("repaired").count()
still_unresolved = n_unresolved  # from Part 3.3
successfully_processed = n_success  # from Part 3.3 (clean + repaired)

# built via SQL VALUES, not spark.createDataFrame(list, ...) - the latter needs a
# Python-worker round-trip that's broken in this local pyspark 3.5.1 / Python 3.12 /
# Windows combo (see Part 3.1's note); SQL VALUES stays entirely on the JVM side.
summary_df = spark.sql(f"""
    SELECT * FROM VALUES
        ('Total input rows', {total_input_rows}L),
        ('Successfully processed rows', {successfully_processed}L),
        ('Rows requiring exception handling', {requiring_exception_handling}L),
        ('Rows successfully repaired', {successfully_repaired}L),
        ('Rows still unresolved', {still_unresolved}L)
    AS t(metric, value)
""")
summary_df.show(truncate=False)

# reconciliation
check_a = total_input_rows == 1000
check_b = successfully_processed + still_unresolved == total_input_rows
check_c = requiring_exception_handling == successfully_repaired + still_unresolved

print(f"\ncheck: total_input_rows == 1000 -> {check_a}")
print(f"check: successfully_processed + still_unresolved == total_input_rows -> {check_b} "
      f"({successfully_processed} + {still_unresolved} == {total_input_rows})")
print(f"check: requiring_exception_handling == successfully_repaired + still_unresolved -> {check_c} "
      f"({requiring_exception_handling} == {successfully_repaired} + {still_unresolved})")

assert check_a and check_b and check_c

# COMMAND ----------
# MAGIC ---
# MAGIC **END OF CHALLENGE**
# MAGIC 
