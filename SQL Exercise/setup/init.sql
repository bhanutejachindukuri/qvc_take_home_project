-- QVC SQL Exercise: schema + seed data
-- Dialect: PostgreSQL 16
--
-- NOTE: customer_id / order_id / product_id references on orders and
-- order_items are intentionally left WITHOUT foreign key constraints.
-- The sample data contains a dangling order_item (order_id 9999, which
-- does not exist in orders) and an order referencing a non-existent
-- customer (customer_id 99). These are exactly the kind of data quality
-- issues Question 3 asks us to detect via SQL, so the schema models a
-- raw/staging layer that accepts the data as-is rather than rejecting it
-- at load time. See README.md for the full write-up.

DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    customer_id   INTEGER PRIMARY KEY,
    customer_name TEXT NOT NULL,
    email         TEXT NOT NULL,
    country       TEXT NOT NULL,
    signup_date   DATE NOT NULL
);

CREATE TABLE products (
    product_id   INTEGER PRIMARY KEY,
    product_name TEXT NOT NULL,
    category     TEXT NOT NULL,
    price        NUMERIC(10, 2) NOT NULL,
    is_active    BOOLEAN NOT NULL
);

CREATE TABLE orders (
    order_id     INTEGER PRIMARY KEY,
    customer_id  INTEGER NOT NULL,      -- no FK: see note above (order 1009 -> customer 99)
    order_date   TIMESTAMP NOT NULL,
    status       TEXT NOT NULL,
    order_total  NUMERIC(10, 2) NOT NULL
);

CREATE TABLE order_items (
    order_item_id INTEGER PRIMARY KEY,
    order_id      INTEGER NOT NULL,     -- no FK: see note above (item 14 -> order 9999)
    product_id    INTEGER NOT NULL,
    quantity      INTEGER NOT NULL,
    unit_price    NUMERIC(10, 2) NOT NULL
);

COPY customers   FROM '/setup/customers.csv'   WITH (FORMAT csv, HEADER true);
COPY products    FROM '/setup/products.csv'    WITH (FORMAT csv, HEADER true);
COPY orders      FROM '/setup/orders.csv'       WITH (FORMAT csv, HEADER true);
COPY order_items FROM '/setup/order_items.csv'  WITH (FORMAT csv, HEADER true);
