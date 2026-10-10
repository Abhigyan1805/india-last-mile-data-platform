# Walkthrough: every component, every design choice (plain English)

Read this to explain the project without notes. Sections marked **(draft)** are completed after the final run.

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
