-- =====================================================================
-- postgres_seed.sql — seeds the relational source (PostgreSQL 16)
--
-- The relational source is the SQL Exercise's local Docker container
-- (`qvc_sql_exercise`, postgres:16, host port 5432) — Azure Database for
-- PostgreSQL could not be provisioned in the available subscription, and
-- an on-prem-style database behind a Self-Hosted Integration Runtime
-- mirrors the real Oracle-behind-SHIR pattern anyway (substitution noted
-- per the brief). ADF reads the five src.* tables through the SHIR and
-- lands them as parquet in raw/<entity>/run_id=<id>/.
--
-- A verified fallback also exists: byte-equivalent typed parquet extracts
-- built by local_test/build_dbextract_parquet.py, uploaded to the
-- `dbextract` container and copied by ADF instead — one source-dataset
-- swap; everything downstream is identical. See README.
--
-- How to run (Docker Desktop started, container running):
--   docker start qvc_sql_exercise
--   docker exec qvc_sql_exercise psql -U qvc -d qvc_sql_exercise \
--          -c "CREATE DATABASE olist;"
--   docker cp postgres_seed.sql qvc_sql_exercise:/tmp/
--   for %f in (olist_orders_dataset.csv olist_customers_dataset.csv
--              olist_order_items_dataset.csv olist_order_payments_dataset.csv
--              olist_order_reviews_dataset.csv) do
--       docker cp "C:\Users\bhanu\Downloads\%f" qvc_sql_exercise:/tmp/
--   docker exec -it qvc_sql_exercise psql -U qvc -d olist -f /tmp/postgres_seed.sql
-- Then inside `docker exec -it qvc_sql_exercise psql -U qvc -d olist`:
--   \cd /tmp
--   ...and run the \copy commands below (client-side; relative filenames
--   resolve against the psql working directory, hence the \cd).
-- =====================================================================

-- The CSVs contain Portuguese accented text (city names, review comments).
-- Windows psql may default the client encoding to WIN1252, which corrupts
-- or aborts those loads — force UTF-8 before anything else.
SET client_encoding TO 'UTF8';

CREATE SCHEMA IF NOT EXISTS src;

DROP TABLE IF EXISTS src.orders;
CREATE TABLE src.orders (
    order_id                        VARCHAR(64) PRIMARY KEY,
    customer_id                     VARCHAR(64) NOT NULL,
    order_status                    VARCHAR(32),
    order_purchase_timestamp        TIMESTAMP,
    order_approved_at               TIMESTAMP,
    order_delivered_carrier_date    TIMESTAMP,
    order_delivered_customer_date   TIMESTAMP,
    order_estimated_delivery_date   TIMESTAMP
);

DROP TABLE IF EXISTS src.customers;
CREATE TABLE src.customers (
    customer_id              VARCHAR(64) PRIMARY KEY,
    customer_unique_id       VARCHAR(64) NOT NULL,
    customer_zip_code_prefix VARCHAR(10),
    customer_city            VARCHAR(100),
    customer_state           VARCHAR(5)
);

DROP TABLE IF EXISTS src.order_items;
CREATE TABLE src.order_items (
    order_id            VARCHAR(64) NOT NULL,
    order_item_id       INTEGER     NOT NULL,
    product_id          VARCHAR(64) NOT NULL,
    seller_id           VARCHAR(64),
    shipping_limit_date TIMESTAMP,
    price               NUMERIC(12,2),
    freight_value       NUMERIC(12,2),
    PRIMARY KEY (order_id, order_item_id)
);

DROP TABLE IF EXISTS src.order_payments;
CREATE TABLE src.order_payments (
    order_id             VARCHAR(64) NOT NULL,
    payment_sequential   INTEGER     NOT NULL,
    payment_type         VARCHAR(32),
    payment_installments INTEGER,
    payment_value        NUMERIC(12,2),
    PRIMARY KEY (order_id, payment_sequential)
);

-- review_id alone is NOT unique in the source (789 review_ids span multiple
-- orders) — a solo PRIMARY KEY (review_id) would abort the \copy below.
-- (review_id, order_id) pairs are unique; that is the real business key.
DROP TABLE IF EXISTS src.order_reviews;
CREATE TABLE src.order_reviews (
    review_id               VARCHAR(64) NOT NULL,
    order_id                VARCHAR(64) NOT NULL,
    review_score            INTEGER,
    review_comment_title    TEXT,
    review_comment_message  TEXT,
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    PRIMARY KEY (review_id, order_id)
);

-- ---------------------------------------------------------------------
-- Client-side loads (run inside psql, from the folder with the CSVs).
-- FORMAT csv is a full RFC-4180 parser: the reviews file's quoted fields
-- with embedded newlines and doubled "" quotes load correctly as-is.
-- ---------------------------------------------------------------------
-- \copy src.orders         FROM 'olist_orders_dataset.csv'         WITH (FORMAT csv, HEADER true)
-- \copy src.customers      FROM 'olist_customers_dataset.csv'      WITH (FORMAT csv, HEADER true)
-- \copy src.order_items    FROM 'olist_order_items_dataset.csv'    WITH (FORMAT csv, HEADER true)
-- \copy src.order_payments FROM 'olist_order_payments_dataset.csv' WITH (FORMAT csv, HEADER true)
-- \copy src.order_reviews  FROM 'olist_order_reviews_dataset.csv'  WITH (FORMAT csv, HEADER true)

-- ---------------------------------------------------------------------
-- Sanity checks — expected EXACT counts:
--   orders 99,441 | customers 99,441 | order_items 112,650
--   order_payments 103,886 | order_reviews 99,224
-- The reviews file has ~104.7k physical lines but 99,224 CSV records
-- (multiline comments). If order_reviews shows ~104k the file was loaded
-- line-wise, not CSV-wise — drop and reload with FORMAT csv.
-- ---------------------------------------------------------------------
-- SELECT 'orders' AS t, COUNT(*) FROM src.orders
-- UNION ALL SELECT 'customers',      COUNT(*) FROM src.customers
-- UNION ALL SELECT 'order_items',    COUNT(*) FROM src.order_items
-- UNION ALL SELECT 'order_payments', COUNT(*) FROM src.order_payments
-- UNION ALL SELECT 'order_reviews',  COUNT(*) FROM src.order_reviews;

-- Demonstration of the review_id duplication (expected: 789) — evidence
-- for why the composite key is required:
-- SELECT COUNT(*) AS duplicated_review_ids
-- FROM (SELECT review_id FROM src.order_reviews
--       GROUP BY review_id HAVING COUNT(*) > 1) d;
