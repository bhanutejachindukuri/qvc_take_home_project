-- Target tables for the Data Pipeline Exercise, created in Azure SQL Database
-- (serverless tier) BEFORE the pipeline runs. Pre-created deliberately, rather than
-- relying on ADF/Databricks auto-create, so column names/types are exact and
-- reviewable independent of the pipeline code.
--
-- Run this once against the target Azure SQL Database (portal Query Editor,
-- Azure Data Studio, or sqlcmd) before triggering the ADF pipeline.

DROP TABLE IF EXISTS dbo.products_loaded;
CREATE TABLE dbo.products_loaded (
    product_id           INT             NOT NULL,
    product_name         NVARCHAR(200)   NOT NULL,
    category             NVARCHAR(100)   NULL,
    price                DECIMAL(10, 2)  NULL,
    is_active             BIT             NULL,
    ingestion_timestamp   DATETIME2       NOT NULL,
    CONSTRAINT pk_products_loaded PRIMARY KEY (product_id)
);

DROP TABLE IF EXISTS dbo.orders_loaded;
CREATE TABLE dbo.orders_loaded (
    order_id              INT             NOT NULL,
    customer_id           INT             NULL,
    order_date            DATETIME2       NULL,   -- source carries a time component
    status                NVARCHAR(50)    NULL,
    order_total           DECIMAL(10, 2)  NULL,
    ingestion_timestamp   DATETIME2       NOT NULL,
    CONSTRAINT pk_orders_loaded PRIMARY KEY (order_id)
);
