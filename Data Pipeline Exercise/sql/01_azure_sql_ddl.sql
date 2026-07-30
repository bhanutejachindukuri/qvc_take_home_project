-- =====================================================================
-- 02_azure_sql_ddl.sql
-- Target DDL for Azure SQL Database (serverless).
-- Layers:
--   curated : typed, cleaned tables written by the Databricks notebook
--   etl     : operational metadata (process log)
--   mart    : consumption views for analysts
-- NOTE: the Spark JDBC write uses mode("overwrite") + option("truncate","true")
-- so these table definitions are PRESERVED across runs (truncate-load).
-- =====================================================================

IF SCHEMA_ID('curated') IS NULL EXEC('CREATE SCHEMA curated');
IF SCHEMA_ID('etl')     IS NULL EXEC('CREATE SCHEMA etl');
IF SCHEMA_ID('mart')    IS NULL EXEC('CREATE SCHEMA mart');
GO

-- ------------------------------------------------------------------ --
-- Operational process log (written by the notebook per entity/stage) --
-- ------------------------------------------------------------------ --
IF OBJECT_ID('etl.pipeline_process_log') IS NULL
CREATE TABLE etl.pipeline_process_log (
    log_id            INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_run_id   NVARCHAR(100)  NOT NULL,
    entity_name       NVARCHAR(100)  NOT NULL,
    stage             NVARCHAR(50)   NOT NULL,   -- e.g. raw_read / curated_write
    rows_read         BIGINT         NULL,
    rows_written      BIGINT         NULL,
    rows_flagged      BIGINT         NULL,       -- rows failing DQ checks (kept, flagged)
    status            NVARCHAR(20)   NOT NULL,   -- SUCCESS / FAILED
    message           NVARCHAR(1000) NULL,
    started_at_utc    DATETIME2      NULL,
    finished_at_utc   DATETIME2      NULL,
    logged_at_utc     DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- ------------------------------------------------------------------ --
-- Curated tables (typed contracts; Spark truncate-loads these)       --
-- ------------------------------------------------------------------ --
IF OBJECT_ID('curated.products') IS NULL
CREATE TABLE curated.products (
    product_id                  NVARCHAR(64)  NOT NULL,
    product_category_name       NVARCHAR(100) NOT NULL,  -- nulls coalesced to 'unknown'
    product_name_length         INT           NULL,      -- renamed from *_lenght (source typo)
    product_description_length  INT           NULL,
    product_photos_qty          INT           NULL,
    product_weight_g            INT           NULL,
    product_length_cm           INT           NULL,
    product_height_cm           INT           NULL,
    product_width_cm            INT           NULL,
    ingestion_timestamp         DATETIME2     NOT NULL,
    pipeline_run_id             NVARCHAR(100) NOT NULL
);
GO

IF OBJECT_ID('curated.customers') IS NULL
CREATE TABLE curated.customers (
    customer_id              NVARCHAR(64)  NOT NULL,
    customer_unique_id       NVARCHAR(64)  NOT NULL,  -- the actual person; customer_id is per-order
    customer_zip_code_prefix NVARCHAR(10)  NULL,
    customer_city            NVARCHAR(100) NULL,      -- casing standardised (initcap)
    customer_state           NVARCHAR(5)   NULL,
    ingestion_timestamp      DATETIME2     NOT NULL,
    pipeline_run_id          NVARCHAR(100) NOT NULL
);
GO

IF OBJECT_ID('curated.orders') IS NULL
CREATE TABLE curated.orders (
    order_id                      NVARCHAR(64) NOT NULL,
    customer_id                   NVARCHAR(64) NOT NULL,
    order_status                  NVARCHAR(32) NULL,   -- lower-cased
    order_purchase_timestamp      DATETIME2    NULL,
    order_approved_at             DATETIME2    NULL,
    order_delivered_carrier_date  DATETIME2    NULL,
    order_delivered_customer_date DATETIME2    NULL,   -- legitimately NULL for undelivered orders
    order_estimated_delivery_date DATETIME2    NULL,
    is_valid_delivery_date        BIT          NOT NULL, -- DQ flag: delivery >= purchase (or not yet delivered)
    ingestion_timestamp           DATETIME2    NOT NULL,
    pipeline_run_id               NVARCHAR(100) NOT NULL
);
GO

IF OBJECT_ID('curated.order_items') IS NULL
CREATE TABLE curated.order_items (
    order_id            NVARCHAR(64)  NOT NULL,
    order_item_id       INT           NOT NULL,
    product_id          NVARCHAR(64)  NOT NULL,
    seller_id           NVARCHAR(64)  NULL,
    shipping_limit_date DATETIME2     NULL,
    price               DECIMAL(12,2) NULL,
    freight_value       DECIMAL(12,2) NULL,
    ingestion_timestamp DATETIME2     NOT NULL,
    pipeline_run_id     NVARCHAR(100) NOT NULL
);
GO

IF OBJECT_ID('curated.order_payments') IS NULL
CREATE TABLE curated.order_payments (
    order_id             NVARCHAR(64)  NOT NULL,
    payment_sequential   INT           NOT NULL,
    payment_type         NVARCHAR(32)  NULL,      -- lower-cased; 'not_defined' rows kept + flagged
    payment_installments INT           NULL,
    payment_value        DECIMAL(12,2) NULL,
    is_valid_payment     BIT           NOT NULL,  -- DQ flag: value >= 0 AND known payment_type
    ingestion_timestamp  DATETIME2     NOT NULL,
    pipeline_run_id      NVARCHAR(100) NOT NULL
);
GO

IF OBJECT_ID('curated.order_reviews') IS NULL
CREATE TABLE curated.order_reviews (
    review_id               NVARCHAR(64)  NOT NULL,  -- NOT unique alone (789 dups); key is (review_id, order_id)
    order_id                NVARCHAR(64)  NOT NULL,
    review_score            INT           NULL,
    review_comment_title    NVARCHAR(100) NULL,      -- observed max 26 chars
    review_comment_message  NVARCHAR(500) NULL,      -- observed max 208 chars
    review_creation_date    DATETIME2     NULL,
    review_answer_timestamp DATETIME2     NULL,
    is_valid_review_score   BIT           NOT NULL,  -- DQ flag: score present and in 1..5
    ingestion_timestamp     DATETIME2     NOT NULL,
    pipeline_run_id         NVARCHAR(100) NOT NULL
);
GO

IF OBJECT_ID('curated.sellers') IS NULL
CREATE TABLE curated.sellers (
    seller_id              NVARCHAR(64)  NOT NULL,
    seller_zip_code_prefix NVARCHAR(10)  NULL,
    seller_city            NVARCHAR(100) NULL,      -- casing standardised (initcap)
    seller_state           NVARCHAR(5)   NULL,
    ingestion_timestamp    DATETIME2     NOT NULL,
    pipeline_run_id        NVARCHAR(100) NOT NULL
);
GO

-- Zip-prefix-grain dimension derived from the 1,000,163-row geolocation
-- point cloud (full fidelity stays in the raw zone). Centroids are averaged
-- over points inside a Brazil bounding box; out-of-bounds points are
-- excluded from the centroid but counted in invalid_point_count.
-- A zip whose points are ALL out of bounds is kept with NULL coordinates.
IF OBJECT_ID('curated.geolocation') IS NULL
CREATE TABLE curated.geolocation (
    geolocation_zip_code_prefix NVARCHAR(10)  NOT NULL,
    latitude                    DECIMAL(9,6)  NULL,
    longitude                   DECIMAL(9,6)  NULL,
    geolocation_city            NVARCHAR(100) NULL,  -- modal value across the zip's points
    geolocation_state           NVARCHAR(5)   NULL,  -- modal value across the zip's points
    point_count                 INT           NOT NULL,
    invalid_point_count         INT           NOT NULL,
    ingestion_timestamp         DATETIME2     NOT NULL,
    pipeline_run_id             NVARCHAR(100) NOT NULL
);
GO

IF OBJECT_ID('curated.product_category_translation') IS NULL
CREATE TABLE curated.product_category_translation (
    product_category_name         NVARCHAR(100) NOT NULL,
    product_category_name_english NVARCHAR(100) NULL,
    ingestion_timestamp           DATETIME2     NOT NULL,
    pipeline_run_id               NVARCHAR(100) NOT NULL
);
GO

-- ------------------------------------------------------------------ --
-- Consumption layer: views (cheap at this volume; would be           --
-- incrementally materialised tables in a production/daily setup)     --
-- ------------------------------------------------------------------ --

-- Sales by state x category. LEFT JOIN to the translation lookup because
-- it does not cover every category (2 Portuguese names have no English
-- entry, plus the coalesced 'unknown') — COALESCE falls back gracefully.
CREATE OR ALTER VIEW mart.v_sales_overview AS
SELECT
    c.customer_state,
    COALESCE(t.product_category_name_english, p.product_category_name) AS product_category,
    COUNT(DISTINCT o.order_id)                                   AS order_count,
    SUM(oi.price)                                                AS gross_merchandise_value,
    AVG(DATEDIFF(DAY, o.order_purchase_timestamp,
                       o.order_delivered_customer_date) * 1.0)   AS avg_delivery_days
FROM curated.orders        o
JOIN curated.customers     c  ON c.customer_id = o.customer_id
JOIN curated.order_items   oi ON oi.order_id   = o.order_id
JOIN curated.products      p  ON p.product_id  = oi.product_id
LEFT JOIN curated.product_category_translation t
       ON t.product_category_name = p.product_category_name
WHERE o.order_status = 'delivered'
  AND o.is_valid_delivery_date = 1
GROUP BY c.customer_state,
         COALESCE(t.product_category_name_english, p.product_category_name);
GO

-- How customers pay: order coverage, value and instalment behaviour per
-- payment type (DQ-valid payments only; the 3 'not_defined' rows are
-- excluded here but visible in the DQ evidence queries).
CREATE OR ALTER VIEW mart.v_payment_mix AS
SELECT
    pay.payment_type,
    COUNT(DISTINCT pay.order_id)                       AS order_count,
    COUNT(*)                                           AS payment_count,
    SUM(pay.payment_value)                             AS total_payment_value,
    AVG(pay.payment_installments * 1.0)                AS avg_installments,
    CAST(100.0 * SUM(pay.payment_value)
         / SUM(SUM(pay.payment_value)) OVER () AS DECIMAL(5,2)) AS pct_of_value
FROM curated.order_payments pay
WHERE pay.is_valid_payment = 1
GROUP BY pay.payment_type;
GO

-- Review score vs delivery performance: does late delivery show up as
-- low scores? (delivered + DQ-valid orders and valid scores only)
CREATE OR ALTER VIEW mart.v_review_delivery AS
SELECT
    r.review_score,
    COUNT(*)                                                    AS review_count,
    AVG(DATEDIFF(DAY, o.order_purchase_timestamp,
                       o.order_delivered_customer_date) * 1.0)  AS avg_delivery_days,
    CAST(100.0 * SUM(CASE WHEN o.order_delivered_customer_date
                               > o.order_estimated_delivery_date
                          THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)) AS pct_delivered_late
FROM curated.order_reviews r
JOIN curated.orders o ON o.order_id = r.order_id
WHERE o.order_status = 'delivered'
  AND o.is_valid_delivery_date = 1
  AND r.is_valid_review_score = 1
GROUP BY r.review_score;
GO
