-- =====================================================================
-- 03_evidence_queries.sql
-- Run against Azure SQL after a pipeline run; screenshot the results.
-- =====================================================================

-- 1. Row counts per curated table. Expected:
--    orders 99,441 | customers 99,441 | order_items 112,650
--    order_payments 103,886 | order_reviews 99,224 | products 32,951
--    sellers 3,095 | geolocation 19,015 (zip-grain dim) | translation 71
SELECT 'curated.orders'         AS table_name, COUNT(*) AS row_count FROM curated.orders
UNION ALL SELECT 'curated.customers',      COUNT(*) FROM curated.customers
UNION ALL SELECT 'curated.order_items',    COUNT(*) FROM curated.order_items
UNION ALL SELECT 'curated.order_payments', COUNT(*) FROM curated.order_payments
UNION ALL SELECT 'curated.order_reviews',  COUNT(*) FROM curated.order_reviews
UNION ALL SELECT 'curated.products',       COUNT(*) FROM curated.products
UNION ALL SELECT 'curated.sellers',        COUNT(*) FROM curated.sellers
UNION ALL SELECT 'curated.geolocation',    COUNT(*) FROM curated.geolocation
UNION ALL SELECT 'curated.product_category_translation',
                                           COUNT(*) FROM curated.product_category_translation;

-- 2. Reconciliation: rows read at source vs rows written to target for the
--    latest run (9 entities). rows_read = rows_written everywhere EXCEPT
--    geolocation, where 1,000,163 raw points intentionally aggregate to
--    19,015 zip-prefix rows (see README).
SELECT entity_name, stage, rows_read, rows_written, rows_flagged, status,
       started_at_utc, finished_at_utc
FROM etl.pipeline_process_log
WHERE pipeline_run_id = (SELECT TOP 1 pipeline_run_id
                         FROM etl.pipeline_process_log
                         ORDER BY logged_at_utc DESC)
ORDER BY logged_at_utc;

-- 3. Transformations visible in the data
--    (renamed column, coalesced category, ingestion metadata)
SELECT TOP 10 product_id, product_category_name, product_name_length,
       ingestion_timestamp, pipeline_run_id
FROM curated.products
WHERE product_category_name = 'unknown';

-- 4. DQ flag distributions (rows kept and flagged, never dropped)
-- 4a. Orders: impossible delivery dates
SELECT is_valid_delivery_date, COUNT(*) AS n
FROM curated.orders
GROUP BY is_valid_delivery_date;

-- 4b. Payments: exactly 3 'not_defined' rows expected with the flag off
SELECT payment_type, is_valid_payment, COUNT(*) AS n
FROM curated.order_payments
GROUP BY payment_type, is_valid_payment
ORDER BY n DESC;

-- 4c. Reviews: score distribution (all expected valid, 1..5)
SELECT review_score, is_valid_review_score, COUNT(*) AS n
FROM curated.order_reviews
GROUP BY review_score, is_valid_review_score
ORDER BY review_score;

-- 5. review_id duplication proof (expected 789) — why the curated key is
--    (review_id, order_id), documented rather than "fixed"
SELECT COUNT(*) AS duplicated_review_ids
FROM (SELECT review_id FROM curated.order_reviews
      GROUP BY review_id HAVING COUNT(*) > 1) d;

-- 6. Geolocation compression proof: 1,000,163 raw points -> 19,015 zips;
--    out-of-bounds points excluded from centroids but counted
SELECT COUNT(*)                 AS zip_prefixes,
       SUM(point_count)         AS raw_points,
       SUM(invalid_point_count) AS excluded_points
FROM curated.geolocation;

-- 7. Cross-source join: sellers enriched with zip centroids (file-sourced
--    sellers x file-sourced-but-aggregated geolocation)
SELECT TOP 10 s.seller_state,
       COUNT(DISTINCT s.seller_id) AS sellers,
       AVG(g.latitude)             AS avg_lat,
       AVG(g.longitude)            AS avg_lng
FROM curated.sellers s
LEFT JOIN curated.geolocation g
       ON g.geolocation_zip_code_prefix = s.seller_zip_code_prefix
GROUP BY s.seller_state
ORDER BY sellers DESC;

-- 8. The mart: both source systems integrated and analytically usable
-- 8a. Sales overview (now with English category names via the translation
--     lookup; 'unknown' and untranslated categories fall back to Portuguese)
SELECT TOP 10 customer_state, product_category,
       order_count, gross_merchandise_value, avg_delivery_days
FROM mart.v_sales_overview
ORDER BY order_count DESC;

-- 8b. Payment mix (full output — 4 rows)
SELECT * FROM mart.v_payment_mix ORDER BY total_payment_value DESC;

-- 8c. Review score vs delivery performance (full output — 5 rows;
--     expect avg_delivery_days and pct_delivered_late to fall as score rises)
SELECT * FROM mart.v_review_delivery ORDER BY review_score;
