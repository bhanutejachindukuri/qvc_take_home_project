# Databricks notebook source
# ---------------------------------------------------------------------------
# transform_ecommerce
#
# Triggered by an Azure Data Factory Notebook Activity (pipeline
# `pl_ecommerce_ingest`, running on a new/job cluster spun up per run).
#
# Reads:
#   - products.csv from an Azure Blob Storage container (CSV source)
#   - the `orders` table from Azure Database for PostgreSQL Flexible Server
#     (relational DB source - Postgres substituted for the brief's preferred
#     Oracle; see README "Assumptions / Trade-offs")
# Transforms:
#   - standardizes column names (defensive - source columns are already
#     snake_case, but this makes the pipeline robust to a future source that
#     isn't)
#   - handles simple nulls / type issues (explicit casts, sensible defaults)
#   - adds an ingestion_timestamp column
# Writes:
#   - dbo.products_loaded and dbo.orders_loaded in Azure SQL Database, via JDBC
#
# Connection details are passed in as notebook parameters (set as ADF Notebook
# Activity "Base parameters", non-secret) and secrets (Postgres/SQL passwords,
# storage account key), read from a Databricks secret scope - never hardcoded.
# For this exercise a Databricks-backed secret scope is used for simplicity;
# a production setup would back it with Azure Key Vault instead.
# ---------------------------------------------------------------------------

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, BooleanType, TimestampType

SECRET_SCOPE = "adf-pipeline-secrets"

dbutils.widgets.text("storage_account_name", "")
dbutils.widgets.text("blob_container", "raw-csv")
dbutils.widgets.text("products_csv_path", "products.csv")
dbutils.widgets.text("postgres_host", "")
dbutils.widgets.text("postgres_db", "postgres")
dbutils.widgets.text("postgres_user", "")
dbutils.widgets.text("sql_server_host", "")
dbutils.widgets.text("sql_database", "")
dbutils.widgets.text("sql_user", "")

storage_account_name = dbutils.widgets.get("storage_account_name")
blob_container = dbutils.widgets.get("blob_container")
products_csv_path = dbutils.widgets.get("products_csv_path")
postgres_host = dbutils.widgets.get("postgres_host")
postgres_db = dbutils.widgets.get("postgres_db")
postgres_user = dbutils.widgets.get("postgres_user")
sql_server_host = dbutils.widgets.get("sql_server_host")
sql_database = dbutils.widgets.get("sql_database")
sql_user = dbutils.widgets.get("sql_user")

storage_account_key = dbutils.secrets.get(SECRET_SCOPE, "storage-account-key")
postgres_password = dbutils.secrets.get(SECRET_SCOPE, "postgres-password")
sql_password = dbutils.secrets.get(SECRET_SCOPE, "sql-password")

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    storage_account_key,
)

blob_path = f"wasbs://{blob_container}@{storage_account_name}.blob.core.windows.net/{products_csv_path}"

postgres_jdbc_url = f"jdbc:postgresql://{postgres_host}:5432/{postgres_db}?sslmode=require"
sql_jdbc_url = (
    f"jdbc:sqlserver://{sql_server_host}:1433;database={sql_database}"
    ";encrypt=true;trustServerCertificate=false;loginTimeout=30;"
)


def standardize_columns(df):
    """Defensive column-name standardization: lowercase, trim, replace any
    whitespace with underscores. A no-op on this source (already snake_case)
    but keeps the pipeline robust against a future source that isn't."""
    for c in df.columns:
        clean = c.strip().lower().replace(" ", "_")
        if clean != c:
            df = df.withColumnRenamed(c, clean)
    return df


# COMMAND ----------

# ---- Source 1: products.csv (Blob Storage) ----

products_raw = (
    spark.read.option("header", True)
    .option("inferSchema", False)
    .csv(blob_path)
)
products_raw = standardize_columns(products_raw)

products_final = (
    products_raw
    .withColumn("product_id", F.col("product_id").cast(IntegerType()))
    .withColumn("product_name", F.coalesce(F.trim(F.col("product_name")), F.lit("Unknown Product")))
    .withColumn("category", F.trim(F.col("category")))
    .withColumn("price", F.col("price").cast(DecimalType(10, 2)))
    .withColumn(
        "is_active",
        F.when(F.lower(F.col("is_active")).isin("true", "1", "yes"), F.lit(True))
         .when(F.lower(F.col("is_active")).isin("false", "0", "no"), F.lit(False))
         .otherwise(F.lit(None).cast(BooleanType())),
    )
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .select("product_id", "product_name", "category", "price", "is_active", "ingestion_timestamp")
)

print(f"products_final row count: {products_final.count()}")
products_final.show(truncate=False)

# COMMAND ----------

# ---- Source 2: orders table (Postgres) ----

orders_raw = (
    spark.read.format("jdbc")
    .option("url", postgres_jdbc_url)
    .option("dbtable", "orders")
    .option("user", postgres_user)
    .option("password", postgres_password)
    .option("driver", "org.postgresql.Driver")
    .load()
)
orders_raw = standardize_columns(orders_raw)

orders_final = (
    orders_raw
    .withColumn("order_id", F.col("order_id").cast(IntegerType()))
    .withColumn("customer_id", F.col("customer_id").cast(IntegerType()))
    .withColumn("order_date", F.col("order_date").cast(TimestampType()))
    .withColumn("status", F.coalesce(F.lower(F.trim(F.col("status"))), F.lit("unknown")))
    .withColumn("order_total", F.coalesce(F.col("order_total").cast(DecimalType(10, 2)), F.lit(0.00)))
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .select("order_id", "customer_id", "order_date", "status", "order_total", "ingestion_timestamp")
)

print(f"orders_final row count: {orders_final.count()}")
orders_final.show(truncate=False)

# COMMAND ----------

# ---- Write both to Azure SQL Database (JDBC), overwrite semantics for this
#      one-off run - a real recurring pipeline would use an incremental/upsert
#      pattern instead (see README "Optimizing for a Recurring Run") ----

sql_write_opts = {
    "user": sql_user,
    "password": sql_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    # truncate=true makes Spark's "overwrite" mode issue a TRUNCATE TABLE instead
    # of DROP+CREATE, which would otherwise destroy the pre-created DDL (PK
    # constraint, exact column types) from sql/create_target_tables.sql and let
    # Spark infer/recreate its own schema instead.
    "truncate": "true",
}

(
    products_final.write.format("jdbc")
    .option("url", sql_jdbc_url)
    .option("dbtable", "dbo.products_loaded")
    .options(**sql_write_opts)
    .mode("overwrite")
    .save()
)

(
    orders_final.write.format("jdbc")
    .option("url", sql_jdbc_url)
    .option("dbtable", "dbo.orders_loaded")
    .options(**sql_write_opts)
    .mode("overwrite")
    .save()
)

print("Write complete: dbo.products_loaded, dbo.orders_loaded")
