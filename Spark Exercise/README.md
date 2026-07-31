# Spark Exercise — Solution

## Objective

Transform a raw one-day returns dataset (scalar columns + a nested `articles`
JSON array + a nested `custom_fields` JSON object) into a clean,
analytics-ready dataset in PySpark on Azure Databricks, written as though it
will run daily against future files with similar structure.

## Notebook

`QVC_Data_Engineer_Challenge_databricks_solution.ipynb` — structured as
Part 0 (reusable transform/validation/repair functions) followed by numbered
tasks: 1.1 (ingest & profile), 2.1 (article match/flatten), 2.2
(`custom_fields` transform), 3.1–3.3 (identify/repair/route exception rows),
4.1–4.3 (build final dataset, optimise datatypes, save). Each task cell
prints and asserts its own evidence rather than deferring validation to the
end.

## Assumptions made

- `parcellab_system_created_date` + `created_time` are assumed to represent
  the same instant (same format, but not verified as guaranteed-atomic in
  the source system) — combined into `parcellab_created_ts` with this
  caveat documented.
- `refreshed_date` and 27/1000 `activity_monitor_last_update` values are
  corrupted/truncated timestamp fragments (e.g. `"38:26.6"`) — left
  unparsed/NULL rather than force-cast.
- `id` looked like a unique per-record key but is **not** (905 distinct
  values across 1000 rows — it identifies the shipment, and one shipment
  can have multiple rows, one per returned `article_number`); a surrogate
  `_row_uid` was introduced and used for all joins/grouping instead.
- Nested objects inside `articles`/`custom_fields` (`customFields`,
  `priceDetails`, `tracking`, `outboundCustomFields`) are flattened one
  level only, not recursively expanded.
- Output format: Delta (`output/final/returns_tracking_delta` and
  `output/exceptions/returns_tracking_exceptions_delta`).

## Limitations

- The `custom_fields` schema was derived from this single sample day and
  validated against it (0 parse failures), but has not been seen against
  other days' data.
- This sample file has zero organic exception rows (every row parses and
  matches exactly once), so the exception-handling and repair logic
  (Part 3) is proven via a small synthetic bad-row harness (`SYN-A`…`SYN-F`)
  rather than real failing rows.
- `refreshed_date` and the 27 corrupted `activity_monitor_last_update`
  values are left as NULL/raw rather than repaired.

## Evidence of execution

See `Spark_Exercise_Evidence.docx` — notebook run screenshots for every
numbered task, including the synthetic exception/repair/routing demo and
the final Databricks catalog showing all output tables written.

Results summary: `Results_spark.xlsx`.
