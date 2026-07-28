-- QVC SQL Take-Home — Solutions
-- Dialect: PostgreSQL 16
-- Schema/seed: see setup/init.sql (run via `docker compose up` in this folder)
--
-- Data model recap: customers, products, orders, order_items 
-- See README.md for the
-- data quality assumptions referenced below (file mislabeling, orphaned
-- rows, order_total mismatches).

-- =====================================================================
-- Question 1: Top Products by Revenue
-- Top 5 products by total revenue from completed orders.
-- =====================================================================
SELECT
    p.product_id,
    p.product_name,
    p.category,
    SUM(oi.quantity) AS total_quantity_sold,
    SUM(oi.quantity * oi.unit_price) AS total_revenue
FROM order_items oi
JOIN orders o    ON o.order_id = oi.order_id
JOIN products p  ON p.product_id = oi.product_id
WHERE o.status = 'completed'
GROUP BY p.product_id, p.product_name, p.category
ORDER BY total_revenue DESC
LIMIT 5;

-- =====================================================================
-- Question 2: Monthly Revenue Trend
-- Monthly revenue for completed orders, using the order-level
-- order_total as the revenue figure (see README for why this is used
-- instead of a re-derived line-item total, and its known limitations).
-- =====================================================================
SELECT
    to_char(date_trunc('month', o.order_date), 'YYYY-MM') AS year_month,
    COUNT(*)                        AS total_orders,
    SUM(o.order_total)              AS total_revenue,
    ROUND(AVG(o.order_total), 2)    AS average_order_value
FROM orders o
WHERE o.status = 'completed'
GROUP BY 1
ORDER BY 1;

-- =====================================================================
-- Question 3: Detect Data Quality Issues
-- =====================================================================

-- 3a. Orders where order_total does not match the sum of its order_items.
--     (LEFT JOIN so orders with zero matching items -- e.g. order 1009 --
--     surface as a mismatch against an implied total of 0, rather than
--     being silently dropped.)
WITH item_sums AS (
    SELECT order_id, SUM(quantity * unit_price) AS computed_total
    FROM order_items
    GROUP BY order_id
)
SELECT
    o.order_id,
    o.order_total  AS recorded_order_total,
    COALESCE(i.computed_total, 0)  AS computed_order_total,
    o.order_total - COALESCE(i.computed_total, 0) AS diff
FROM orders o
LEFT JOIN item_sums i ON i.order_id = o.order_id
WHERE o.order_total <> COALESCE(i.computed_total, 0)
ORDER BY o.order_id;

-- 3b. Orphaned order_items rows with no matching order.
SELECT oi.*
FROM order_items oi
LEFT JOIN orders o ON o.order_id = oi.order_id
WHERE o.order_id IS NULL;

-- 3c. Duplicate customer emails.
SELECT
    email,
    COUNT(*) AS customer_count,
    array_agg(customer_id ORDER BY customer_id) AS customer_ids
FROM customers
GROUP BY email
HAVING COUNT(*) > 1;

-- =====================================================================
-- Question 4: Customer Ranking
-- Rank customers within each country by completed-order revenue.
-- (INNER JOIN to customers naturally excludes order 1009, which
-- references a non-existent customer_id 99 -- another data quality
-- issue, flagged in README rather than silently masked.)
-- =====================================================================
WITH customer_revenue AS (
    SELECT
        c.customer_id,
        c.customer_name,
        c.country,
        SUM(o.order_total) AS total_revenue
    FROM customers c
    JOIN orders o ON o.customer_id = c.customer_id
    WHERE o.status = 'completed'
    GROUP BY c.customer_id, c.customer_name, c.country
)
SELECT
    country,
    customer_id,
    customer_name,
    total_revenue,
    RANK() OVER (PARTITION BY country ORDER BY total_revenue DESC) AS revenue_rank
FROM customer_revenue
ORDER BY country, revenue_rank;

-- =====================================================================
-- Question 5: Build a Reusable SQL Model
-- Order-fact view at the order grain. This generalises the mismatch
-- logic from 3a into a reusable model -- 3a is effectively
-- =====================================================================
CREATE OR REPLACE VIEW order_fact AS
WITH item_agg AS (
    SELECT
        order_id,
        COUNT(*)                          AS item_count,
        COUNT(DISTINCT product_id)        AS distinct_product_count,
        SUM(quantity * unit_price)        AS computed_order_total
    FROM order_items
    GROUP BY order_id
)
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status                                        AS order_status,
    COALESCE(ia.item_count, 0)                       AS item_count,
    COALESCE(ia.distinct_product_count, 0)           AS distinct_product_count,
    COALESCE(ia.computed_order_total, 0)             AS computed_order_total,
    o.order_total                                    AS recorded_order_total,
    o.order_total - COALESCE(ia.computed_order_total, 0) AS order_total_diff
FROM orders o
LEFT JOIN item_agg ia ON ia.order_id = o.order_id;

SELECT * FROM order_fact ORDER BY order_id;
