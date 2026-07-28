
# SQL Exercise — Solution

## SQL dialect

PostgreSQL 16, run locally via Docker.

## Setup and run steps

```bash
cd "SQL Exercise"
docker compose up -d          # starts postgres:16, auto-loads schema + data
                               # via setup/init.sql (docker-entrypoint-initdb.d)

docker compose exec -T db psql -U qvc -d qvc_sql_exercise < solution.sql
```

Connection details: `localhost:5432`, db `qvc_sql_exercise`, user/password `qvc`/`qvc` (throwaway local-only credentials).

To reset and reload from scratch: `docker compose down -v && docker compose up -d`.

## Assumptions made

- **The four provided CSVs are mislabeled / content-shifted from their filenames**, and no `customers.csv` was actually present despite being referenced in the brief. By inspecting each file's header row against the documented data model:

  | File as provided                                          | Actually contains    |
  | --------------------------------------------------------- | -------------------- |
  | `orders.csv`                                            | `order_items` data |
  | `order_items.csv`                                       | `customers` data   |
  | `products.csv`                                          | `orders` data      |
  | `setup-sql.docx` (not actually a Word doc — plain CSV) | `products` data    |

  I remapped these to their correct roles under `setup/` (`customers.csv`, `products.csv`, `orders.csv`, `order_items.csv`, each with the content matching their name) before building the schema. This is treated as the primary data quality finding for this exercise, since it would silently corrupt every downstream query if unnoticed.
- `orders` / `order_items` are loaded **without foreign key constraints** on `customer_id` / `order_id` / `product_id`. The sample data contains genuine referential issues (see below) that Question 3 asks us to detect via SQL — enforcing FKs at load time would just reject those rows instead of letting them be queried, so the schema models a raw/staging layer rather than a fully constrained warehouse table.
- Question 2 and Question 4 use the **recorded `order_total`** on the `orders` table as "revenue," not a re-derived sum of `order_items`. This is simpler and matches how most order systems report top-line revenue, but per the Question 3a findings, `order_total` and the line-item sum disagree for 2 of 10 orders — in a production model I'd likely prefer the recomputed total (`order_fact.computed_order_total` from Question 5) as the source of truth for revenue reporting, and reconcile discrepancies as a data quality alert rather than silently trusting either number. Noted as a trade-off, not fixed silently.
- "Completed orders" is interpreted as `status = 'completed'` exactly (the sample data also has `cancelled` and `pending` statuses, both excluded).

## Data quality issues identified

Beyond the file mislabeling above, running the Question 3 queries against the corrected data surfaces:

1. **`order_total` mismatches** (order-level total ≠ sum of its order_items):
   - Order `1001`: recorded `95.00` vs. computed `120.00`
   - Order `1005`: recorded `80.00` vs. computed `85.00`
   - Order `1009`: recorded `35.00` but has **zero** matching order_items (computed `0`) — see #3.
2. **Orphaned order_item**: `order_item_id 14` references `order_id 9999`, which does not exist in `orders`.
3. **Order with no line items**: order `1009` has no rows in `order_items` at all (distinct from #2 — this is an order missing its items, not an item missing its order).
4. **Duplicate customer email**: `alice@example.com` is used by both `customer_id 1` (Alice Johnson) and `customer_id 8` (Hannah Scott).
5. **Orphaned customer reference** (not explicitly asked for, but caught by the schema design): order `1009`'s `customer_id` is `99`, which does not exist in `customers`. Because Questions 1 and 4 use inner joins to `orders`/`customers`, this order is naturally excluded from product-revenue and customer-ranking results rather than causing an error — worth flagging explicitly rather than leaving as a silent gap.

## Optional ideas for improving the pipeline / model

- Add a nightly data-quality check job running the Question 3 queries (and the `order_fact.order_total_diff <> 0` check from Question 5) with alerting, so `order_total` drift is caught same-day instead of at analysis time.
- Materialize `order_fact` (Question 5) as a proper dbt model / materialized view refreshed on each load, and have Questions 1, 2, and 4 read from it instead of re-deriving revenue independently — this would make the "which total do we trust" decision (see assumptions above) a single, auditable choice instead of three separate ones.
- Add a `customers.email` uniqueness constraint (or an explicit "merge/survivorship" process) once the duplicate-email root cause is understood, rather than tolerating duplicates indefinitely.
- Backfill or quarantine order `1009` (bad customer_id, no line items) into an exceptions table rather than leaving it silently excluded from every aggregate query that inner-joins.
