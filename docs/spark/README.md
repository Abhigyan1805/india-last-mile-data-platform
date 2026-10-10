# `spark/` — PySpark batch marts

This folder answers four Porter-style last-mile questions on top of the dbt
warehouse and writes the results back to PostgreSQL. It is the `spark` step of
the pipeline (`make spark` / `python -m spark.run_all`).

## What it does

It reads `analytics.fct_deliveries` (valid rows only) plus the small
`analytics.dim_weather_daily` dimension over **JDBC**, computes four aggregates
and overwrites four tables in the `spark_marts` schema:

| Mart | Question | Grain |
|---|---|---|
| `deliveries_per_pincode_hour` | Where and when is demand concentrated? | source × drop pincode × city × hour |
| `courier_utilisation` | How busy are couriers; how much of a shift is spent delivering? | source × courier |
| `delay_impact` | How much do weather, traffic, festivals and rain slow deliveries? | source × factor × factor value |
| `city_comparison` | Which cities are hardest to deliver in? | source × city |

Every table's columns are fixed by [`ingest/sql/ddl.sql`](../ingest/sql/ddl.sql)
and `docs/CONTRACTS.md` §6; the code writes exactly those columns.

## Running it

```bash
source scripts/env.sh                      # sets PG* / SPARK_MASTER / JAVA_HOME
python -m spark.run_all                    # all four jobs -> Postgres
python -m spark.run_all --source zomato    # one source only
python -m spark.run_all --jobs city_comparison,pincode_hour
python -m spark.run_all --sample --no-write # fixture, no Postgres
python -m spark.run_all --report           # also print 95% CIs for delay impact
```

Each job is also its own CLI, e.g. `python -m spark.jobs.delay_impact --sample`.
`make spark` is the pipeline target; `make spark-sample` and `make spark-test`
are conveniences.

The Postgres JDBC driver is picked up from `.tools/jars/postgresql-*.jar` if
present (set `POSTGRES_JDBC_JAR` to override); otherwise Spark resolves
`org.postgresql:postgresql:42.7.3` through Ivy.

## Design choices (and why)

- **Read once, share across jobs.** `run_all` pulls the fact table a single time
  and `cache()`s it, so four jobs share one JDBC read. The weather dimension is
  broadcast into the delay job instead of being shuffled.
- **`local[*]` / bounded resources.** `build_session` uses `$SPARK_MASTER`
  (default `local[*]`, pinned to `local[2]` by `scripts/env.sh` on the shared
  machine), driver heap `1g`, executor heap `512m`, 8 shuffle partitions and the
  UI off. Python workers are pinned to the driver interpreter — the system
  Python (3.14) differs from the venv Python (3.12) and PySpark refuses to mix
  them.
- **p90, not just the mean.** Delivery times are right-skewed; the 90th
  percentile (`percentile_approx`, Greenwald–Khanna) shows the tail the mean
  hides and scales to the full dataset.
- **Courier utilisation (Zomato only).** Amazon rows carry no courier id.
  `busy_minutes = Σ delivery_minutes`; `on_shift_minutes` is measured per
  courier-*day* (`last order_ts + delivery − first order_ts`) and summed over the
  courier's active days; `utilisation = busy / on-shift`, capped at 1 because a
  courier can be assigned the next order before the previous one is "complete".
  A courier working in several cities is reported under their dominant city.
- **Delay baseline.** For each (source, factor) the baseline is the value with
  the smallest mean delivery time, so `delay_vs_baseline_min` is 0 for the
  best-performing value and positive elsewhere — directly quotable.
- **Confidence intervals.** The stored mart keeps exactly the DDL columns; the
  `--report` flag additionally prints `mean ± 1.96·sd/√n` per group so a number
  can be quoted with a margin of error.
- **Idempotent writes.** Marts are written `mode=overwrite` with `truncate=true`
  and `isolationLevel=NONE` (Postgres cannot run `TRUNCATE` inside the
  transaction Spark otherwise opens), so re-running never duplicates rows.
- **Unmatched drops.** The pincode/hour mart drops rows whose drop point did not
  resolve to a pincode (they cannot be placed on the map); the city mart instead
  reports `pincode_match_rate` so coverage gaps stay visible.

## Tests

```bash
make spark-test                 # 19 unit tests, local SparkSession, tiny fixture
ILM_SPARK_IT=1 PGPORT=5435 PGDATABASE=spark_it pytest tests/spark/test_jdbc_integration.py
```

Unit tests build DataFrames from `spark/sample.py` — a hand-built fixture whose
aggregates are known — and assert the transform outputs. The integration test is
skipped by default; with `ILM_SPARK_IT=1` it applies the DDL, loads the fixture,
runs the real JDBC path and checks the rows landed in Postgres. Point it at a
throwaway database (it truncates what it touches).

## Likely interview questions

**1. Why Spark for this, when the data fits in one machine?**
The jobs are aggregate-heavy with a right-skewed tail and the pipeline is meant
to scale past one node. Spark gives the same code a local and a clustered
execution mode, and the marts are the natural place to demonstrate partitioning,
broadcast joins and approximate percentiles.

**2. How is a Spark job made idempotent here?**
`overwrite` + `truncate=true` rewrites each mart in place, so re-running a DAG
task replaces rather than appends. Combined with the warehouse's `is_valid`
filter the same input always yields the same output.

**3. Why is courier utilisation capped at 1, and how is on-shift time defined?**
Utilisation is busy delivery minutes over on-shift minutes. `Time_taken` in the
Zomato data is the total order-to-delivery time, so a courier can overlap orders
and busy can exceed a strict shift window; capping at 1 keeps the metric a
fraction. On-shift is per courier-day from the first order to the last order plus
its delivery time, summed across active days — a lower bound on real shift time.

**4. What is the p90 for and why `percentile_approx`?**
The mean hides the slow tail that hurts customer experience; p90 exposes it.
Exact percentiles require a full sort and don't parallelise cleanly, so
`percentile_approx` gives a bounded-error estimate that works at scale.

**5. Where would you broadcast a join, and why here?**
The weather dimension is tiny (city × day), so broadcasting it to every executor
avoids shuffling the much larger fact table. Spark's adaptive engine may do this
automatically, but making it explicit documents the intent.

**6. How do you know a bad row didn't skew these numbers?**
The jobs read only `is_valid` rows from `fct_deliveries`; the failing rules
(coordinates outside India, absurd delivery times, duplicate orders, unparseable
timestamps) are recorded by the quality layer in `quality.check_results` with
row counts, so the exclusions are auditable rather than silent.

## Files

```
spark/config.py        session factory, JDBC URL/props, table names
spark/io.py            JDBC read (projection + is_valid pushdown) and write helpers
spark/jobs/*.py        one module per mart, each with transform() + CLI main()
spark/run_all.py       entry point: read once, run jobs, write marts
spark/sample.py        deterministic in-memory fixture for tests and --sample
```
