# warehouse/ — the dbt warehouse and quality checks

## What it does (plain English)
Raw Amazon India and Zomato delivery files land in Postgres `raw.*` as untouched text. dbt cleans them in
`staging` (trim text, turn "NaN" into NULL, parse numbers and timestamps), combines both sources in
`staging.int_deliveries` (one row per order, with a boolean flag per data-quality problem), and publishes a star
schema in `analytics`: `fct_deliveries` plus `dim_courier`, `dim_pincode_zone`, `dim_date`, `dim_time`,
`dim_city`, `dim_weather_daily`. Names and columns follow `docs/CONTRACTS.md`.

## Data quality
- dbt generic tests: `not_null`, `unique`, `accepted_values`, `relationships` (see `_staging.yml`, `_marts.yml`).
- Custom singular tests (`warehouse/tests/dq_*.sql`, rules in `macros/dq.sql`): coordinates outside India's
  bounding box, negative or absurd delivery time, duplicate order ids, orphan pincodes, flag consistency. They are
  configured `warn` so that bad rows are *measured*, not allowed to stop the build.
- `python -m warehouse.quality` counts the flags per source and writes one row per
  (run_id, check, dataset) to `quality.check_results` with `rows_failed`, `rows_checked` and a timestamp.
  Re-running a run_id is idempotent. These are the numbers the README quotes.
- Bad rows are kept in the fact table but marked invalid (flags) rather than deleted, so you can show both
  "rows caught" and "rows used".

## Incremental model
`fct_deliveries` is incremental (`unique_key` = order key): a normal run only processes new/changed rows;
`--full-refresh` rebuilds it.

## Run on the tiny fixture (no full dataset)
```
export PGPORT=5434          # your own Postgres (scripts/pg_native.sh start)
make warehouse-sample       # loads warehouse/sample/*.csv into DB warehouse_sample, dbt build, quality
pytest -q tests/warehouse
```
Fixture result: dbt build PASS=48 WARN=4 ERROR=0; quality runner flags e.g. 9 of 27 fixture rows as invalid
(the fixture deliberately contains bad rows). Real numbers come from the full run by the integration owner.

## Interview questions
1. **Why a star schema?** One narrow fact (one row per delivery) with descriptive dimensions makes the
   Spark/SQL questions (per pincode, hour, city) simple joins and keeps definitions in one place.
2. **Why staging / intermediate / marts layers?** Staging only cleans types; intermediate holds business logic
   and DQ flags; marts are the published model. Each layer is testable on its own.
3. **Why incremental, and what is its risk?** Avoids reprocessing millions of rows each run; the risk is late or
   changed rows, handled by `unique_key` (merge/delete+insert) and a periodic `--full-refresh`.
4. **Why are DQ tests `warn` and not `error`?** Real public data is dirty; the goal is to count and report
   problems, and let clean rows flow. Structural tests (not_null keys, relationships) still fail the build.
5. **How do you detect bad coordinates?** A bounding box for India (lat ~6–37.5, lon ~68–98) in one macro,
   `in_india_bbox`; also NULL coordinates count as not valid.
6. **How do you make the quality numbers trustworthy?** They are counted by SQL over the actual run, stored
   with run_id and timestamp, and the same macro defines the rule for the dbt test and the runner.
