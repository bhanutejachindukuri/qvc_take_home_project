-- Seeds the "relational database" source for the pipeline: an `orders` table in
-- Azure Database for PostgreSQL Flexible Server. Data is the SQL Exercise's own
-- corrected orders.csv (SQL Exercise/setup/orders.csv), reused here as the take-home's
-- second source. Run once against the Postgres Flexible Server (portal query tool,
-- Azure Data Studio, or psql) after it's provisioned.
--
-- Note: order_id 1009 has customer_id 99, which does not exist among the SQL
-- Exercise's customers (max customer_id there is 8) - this is a genuine orphaned-FK
-- data quality issue already identified in the SQL Exercise, deliberately preserved
-- here rather than "fixed", since it's useful to demonstrate the pipeline's
-- null/type-issue handling doesn't silently drop or corrupt a row it can't fully
-- resolve - it still ingests it as-is (no FK enforced at this layer).

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    order_id     INT PRIMARY KEY,
    customer_id  INT,
    order_date   TIMESTAMP,
    status       VARCHAR(50),
    order_total  NUMERIC(10, 2)
);

INSERT INTO orders (order_id, customer_id, order_date, status, order_total) VALUES
    (1001, 1, '2024-01-10 09:15:00', 'completed', 95.00),
    (1002, 2, '2024-01-12 14:20:00', 'completed', 25.00),
    (1003, 1, '2024-02-01 11:05:00', 'cancelled', 75.00),
    (1004, 3, '2024-02-03 16:40:00', 'completed', 230.00),
    (1005, 4, '2024-02-15 10:10:00', 'completed', 80.00),
    (1006, 5, '2024-03-01 08:00:00', 'pending', 40.00),
    (1007, 3, '2024-03-08 12:30:00', 'completed', 50.00),
    (1008, 7, '2024-03-11 13:00:00', 'completed', 220.00),
    (1009, 99, '2024-03-12 15:45:00', 'completed', 35.00),
    (1010, 2, '2024-03-20 17:10:00', 'completed', 20.00);
