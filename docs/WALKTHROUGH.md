# Walkthrough: every component, every design choice (plain English)

Read this to explain the project without notes.

## The one-minute story
Delivery companies like Porter collect messy data: orders, GPS pings, drivers, weather. This project is a
small version of that plumbing. It pulls real Indian delivery datasets, cleans them, places every coordinate
on the Indian postal map (pincode -> district -> city), stores them in a warehouse, checks quality
automatically, and answers business questions (which pincodes are busy at which hour, how rain slows
deliveries, how busy couriers are). A simulated Bengaluru order stream shows the live (Kafka) side.

## Data flow
`Kaggle CSVs + India Post pincodes` -> **raw** schema (as-is text) -> **geo enrichment** (pincode/city) +
**weather** -> **dbt** staging and star schema -> **quality checks** -> **PySpark** aggregates -> `spark_marts`.
Airflow runs these in order, one date partition per run.

## ingest/ (acquisition, raw load, geo, weather)
* **acquire.py** — downloads three Kaggle datasets with the `kaggle` CLI. The token is read from
  `~/.kaggle/access_token` at runtime and handed only to the child process; never stored. Skips files that
  already exist (idempotent). The pincode file is a Kaggle mirror of India Post's directory (data.gov.in) because
  data.gov.in itself needs an API key and the mirror has office latitude/longitude.
* **load.py** — ELT, not ETL: loads everything as TEXT into `raw.*` first so nothing is lost or silently
  coerced; typing/cleaning happens in dbt where it is tested and visible. Re-runs `TRUNCATE`+`COPY` (or
  delete-and-reload one `order_date` when `PARTITION_DATE` is set, which is what Airflow backfills use).
  `COPY` is used because it is 10-100x faster than row inserts.
* **geo.py** — for each pickup/drop coordinate finds the nearest post office. A KD-tree
  (scipy `cKDTree`) over points on a unit sphere gives nearest-neighbour in O(log n) instead of comparing with
  all 19k pincodes (178k x 19k = 3.4 billion comparisons naively). Unit-sphere (x,y,z) coordinates avoid the
  distortions of treating lat/long as flat; chord length is converted back to km. A per-pincode **median** of
  its offices' coordinates damps bad geocodes in the directory (we saw a Koramangala office placed 100 km away).
  A point is `matched` only if it is inside India's bounding box *and* within 10 km of a pincode;
  the rest (e.g. (0,0) coordinates) are counted, not hidden. District -> city via `city_map.py` because India Post
  has districts, and metros span several (Thane/Mumbai Suburban -> Mumbai).
* **weather.py** — Open-Meteo archive API, one call per city for the whole date range, cached to disk so
  re-runs make zero network calls. Only cities with >= 200 matched pickups are fetched.

## orchestration/ (Airflow)
`ilm_batch_pipeline`: acquire -> load_raw -> geo_enrich -> dbt_build -> quality_checks -> spark_aggregates.
Daily schedule with `catchup=True` over the dates present in the data, `max_active_runs=1`. Each run passes
its logical date (`{{ ds }}`) as `PARTITION_DATE`, so the run for 2022-03-01 reloads only that day's raw rows
(idempotent: delete that day, insert that day). Tasks shell out to the same Python modules as `make`, so the DAG
is a thin scheduler and the logic is unit-testable without Airflow.
**Why LocalExecutor?** Single machine, no Celery/Kubernetes overhead; the DAG code is identical for other executors.

## Runtime decision
Docker was not available on the build machine, so the recorded run is native (Postgres from the `pgserver`
wheel, Kafka KRaft + JDK from tarballs, PySpark local). `docker-compose.yml` describes the same stack for
reviewers and was **not executed** by the author's machine. Say this honestly if asked.

## Likely interview questions (ingest / orchestration)
**Why load raw as text?** Raw is the audit trail. If I cast on load, a bad value either fails the load or
becomes NULL silently; as text I can count and inspect bad values and let dbt cast with tests.
**Why a KD-tree rather than SQL / PostGIS?** Fastest to make reliable: one in-memory index, vectorised queries,
runs in ~15 s for 178k points; PostGIS would need an extension and GiST index, and gives the same answer.
**How do you know the match is right?** Report distance distribution and the match rate; unit test with known
coordinates; 10 km cutoff is a parameter. Limits: nearest *post office*, not polygon containment.
**What does idempotent mean here?** Running a task twice gives the same end state: raw loads replace their
partition, geo/weather replace their tables, dbt incremental models merge on a key, Spark overwrites marts.
**How would you backfill?** `airflow dags backfill -s 2022-02-11 -e 2022-04-06 ilm_batch_pipeline`; each run owns a date partition.
**What if the Kaggle file schema changes?** Raw load maps columns by name, so a missing/renamed column fails loudly in `copy_expert` instead of shifting data.
**What breaks at 100x data?** Pandas in-memory geo -> chunk or Spark; single Postgres -> partitioned tables/columnar warehouse; Airflow LocalExecutor -> Celery/K8s.

## warehouse/ (dbt + quality) - see also [`warehouse.md`](warehouse.md)
Raw text -> `staging` (trim, "NaN" -> NULL, parse numbers/timestamps) -> `staging.int_deliveries` (both sources,
one row per order, one boolean flag per data problem) -> star schema in `analytics`: `fct_deliveries` (incremental,
merge on `delivery_key`) with `dim_courier`, `dim_pincode_zone`, `dim_city`, `dim_date`, `dim_time`, `dim_weather_daily`.
Tests: dbt `not_null/unique/accepted_values/relationships` plus custom SQL tests (coords outside India's box,
negative/absurd times, duplicates). They are `warn`, not `error`, so bad rows are *measured* instead of stopping the
build; `python -m warehouse.quality` writes the per-check failing-row counts to `quality.check_results` each run.
Bad rows stay in the fact table flagged `is_valid=false` (14.4% of rows) so we can show "caught" and "used".
**Perf bug found in integration:** a correlated "latest load" subquery in `stg_delivery_geo` re-scanned 178k rows per
row (O(n^2)): the build ran 10+ min; precomputing the latest load once cut it to 13 s. Worth telling as a debugging story.

## spark/ - see [`spark/README.md`](spark/README.md)
Reads valid facts over JDBC, computes four marts, overwrites `spark_marts.*` (deliveries per pincode-hour, courier
utilisation, delay impact by weather/traffic/festival/rain, city comparison). Local mode: honest about data size
(89k rows fit in memory; Spark is used to show the distributed-compute pattern, and it would scale out unchanged).
Utilisation = busy minutes / on-shift minutes per courier-day, Zomato only (Amazon has no courier id).

## streaming/ - see [`streaming.md`](streaming.md)
A simulator generates Bengaluru order lifecycles + GPS pings (two-wheeler, truck) with demand-by-hour and
delivery-time-by-traffic learned from the Spark marts, and **deliberately injects** duplicates, late pings,
out-of-bounds GPS and malformed events. A consumer validates, dedups on `event_id` (idempotent insert), keeps late
events but flags them (per-topic watermark, 300 s allowed lateness), and records counts in `quality.check_results`.
**All simulated, labelled `"simulated": true`.**

## Likely interview questions (warehouse / spark / streaming)
**Why a star schema?** One fact table at delivery grain with small dimensions makes analyst queries simple joins and
keeps definitions (city, pincode, date) in one place.
**Incremental model - how does it avoid duplicates?** `unique_key=delivery_key` with merge: re-processed rows update, not append.
**Why warn instead of fail on quality tests?** Source data is known-dirty; the goal is to quantify and flag it, and
fail only on structural problems (null keys, broken relationships). Thresholds could be added to fail on regressions.
**Why 0 duplicates in batch but thousands in streaming?** The real files have unique ids; duplicates are a streaming
reality (at-least-once delivery, retries), so they are injected and caught there. Say so plainly - don't oversell.
**Exactly-once?** No: at-least-once delivery + idempotent sink (primary key on `event_id`, `ON CONFLICT DO NOTHING`).
**What is a watermark?** The newest event time seen; an event older than watermark minus allowed lateness is "late".
**Why one Kafka partition?** Ordering is only guaranteed within a partition; more partitions need per-partition watermarks.
**Is Spark overkill here?** For 89k rows yes; it is chosen to demonstrate the pattern, and the same job runs on a cluster by changing the master.
**What would you do with real Porter data?** Replace the simulator with the real order topic, keep the same consumer
contract, add schema registry, partition tables by day, and alert on quality-check regressions.
**Biggest weakness?** Datasets are 2022 Kaggle snapshots that look partly synthetic; the pincode match is nearest-office, not polygon containment; Docker compose was written but not executed on the build machine.
