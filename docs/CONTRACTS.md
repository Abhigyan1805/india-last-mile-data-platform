# Contracts between components

Single source of truth for the interfaces between `ingest/`, `warehouse/`, `spark/`, `streaming/` and
`orchestration/`. **Exact DDL lives in [`ingest/sql/ddl.sql`](../ingest/sql/ddl.sql)** (raw tables, quality
table, streaming landing tables, Spark mart tables). Change it only by editing that file and this doc together.

## 1. Runtime decision

Docker is **not installed** on the build machine (WSL2, 5 GB RAM, no sudo), so the recorded end-to-end run is
**native**: PostgreSQL 16 (binaries from the `pgserver` wheel, `scripts/pg_native.sh`), Kafka 4.x KRaft + JDK 17
(`scripts/fetch_tools.sh`, `scripts/kafka_native.sh`), PySpark local mode, dbt-postgres, Airflow in `standalone`
/ `dags test` mode. `docker-compose.yml` expresses the same stack for reviewers with Docker (`make up`) but was
not executed on the build machine; the README says so plainly.

## 2. Environment variables (see `scripts/env.sh`, `.env.example`)

| Var | Native default | Meaning |
|---|---|---|
| `PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE` | `127.0.0.1/5433/ilm/ilm/warehouse` | warehouse (compose: port 5432 inside network) |
| `DATA_DIR` | `./data` | gitignored raw files: `raw/amazon/`, `raw/zomato/`, `raw/pincodes/`, `raw/weather/` |
| `KAFKA_BOOTSTRAP` | `127.0.0.1:9092` | broker |
| `SPARK_MASTER` | `local[2]` | Spark master; keep driver memory <= 1.5g |
| `SAMPLE_MODE` | `0` | `1` = demo slice: first `DEMO_ROWS` (default 2000) rows per source, weather for the cities present |
| `GEO_MATCH_MAX_KM` | `10` | max distance for a nearest-pincode match to count as matched |
| `RUN_DATE` / `LOAD_ID` | today / `YYYYmmddTHHMMSS` | partition + lineage id; every raw row carries `_load_id` |
| `KAGGLE_API_TOKEN` | read from `~/.kaggle/access_token` at runtime | **never written into repo/logs/images** |

Python entry points are modules run from repo root with `scripts/env.sh` sourced (the Make targets do this).

## 3. Schemas and ownership

| Schema | Owner | Content |
|---|---|---|
| `raw` | ingest | TEXT-typed source landing + `delivery_geo`, `city_ref`, `weather_daily`, `ingest_runs`; streaming tables `stream_*` are written by streaming |
| `staging` | warehouse | dbt views `stg_*` (typed, cleaned, snake_case) |
| `analytics` | warehouse | star schema (below) |
| `quality` | warehouse + streaming | `check_results` |
| `spark_marts` | spark | four aggregate tables |

### Raw loading rules (ingest)
* Idempotent: each source's rows are replaced per `_load_id` run (delete-by-source then insert, in one transaction); re-running never doubles rows. Raw files are not mutated.
* Dedup is **not** done in raw (duplicates are real data and must be caught by quality checks).
* `raw.delivery_geo` has two rows per order (pickup, drop). Latest `_load_id` is authoritative (ingest deletes older rows for the same source).
* **City mapping:** `raw.delivery_geo.city` = canonical city from the nearest pincode's district via `ingest/city_map.py` (e.g. Bengaluru Urban/Rural -> Bengaluru, Mumbai/Thane suburbs -> Mumbai, New Delhi districts -> Delhi, etc.); otherwise the district name title-cased. `raw.zomato_deliveries.city` is only the *area type* (Metropolitian/Urban/Semi-Urban) in this dataset, **not** a city. The Zomato `delivery_person_id` prefix (e.g. `DEHRES`) is a restaurant-city code, kept as `courier_id` source.
* Known data quirks to expect (quality checks exist for them): `NaN` strings, trailing spaces in categoricals (`'High '`), `(0,0)` coordinates, negative coordinates, duplicate IDs, `Time_taken` possibly formatted `(min) 24`, dates `dd-mm-yyyy` (Zomato) vs `yyyy-mm-dd` (Amazon).

## 4. `analytics` star schema (warehouse/ must produce exactly these models and columns)

* `fct_deliveries` — grain: one delivery (`source`,`order_id`); incremental on `order_date` (merge on `delivery_key`).
  `delivery_key` (PK = `source||'-'||order_id`), `source`, `order_id`, `courier_id` (Zomato `delivery_person_id`; NULL for Amazon), `city` (FK dim_city.city, from pickup geo), `pickup_pincode`, `drop_pincode` (FK dim_pincode_zone.pincode, nullable if unmatched),
  `order_ts`, `pickup_ts` (timestamps, nullable), `order_date` (date), `order_hour` (smallint), `delivery_minutes` (numeric), `distance_km` (haversine pickup->drop),
  `weather`, `traffic`, `vehicle_type`, `area_type`, `festival` (boolean, NULL for Amazon), `multiple_deliveries` (int, NULL for Amazon), `courier_rating` (numeric), `rain_mm` (joined from weather by city+date, nullable),
  `is_valid` (boolean: false if any hard DQ rule fails — coords outside India bbox, delivery_minutes <= 0 or > 300, duplicate order beyond the first, unparseable timestamp), `dq_flags` (text[] of failed rule names).
* `dim_courier` (`courier_id` PK, `home_city_code`, `avg_rating`, `first_seen`, `last_seen`, `deliveries`)
* `dim_pincode_zone` (`pincode` PK, `office_name`, `district`, `state`, `city`, `latitude`, `longitude`; one row per pincode, centroid of its post offices)
* `dim_city` (`city` PK, `state`, `latitude`, `longitude`, `deliveries`)
* `dim_date` (`date_key` date PK, `year`, `month`, `day`, `dow`, `is_weekend`, `is_festival_season` boolean — Diwali/Holi windows), `dim_time` (`hour_of_day` PK 0-23, `daypart`)
* `dim_weather_daily` (`city`,`date` PK, temps, `rain_mm`, `rain_bucket` in none/light/moderate/heavy)
* India bounding box used everywhere: lat 6.0–37.5, lon 68.0–97.5 (`warehouse` macro `in_india_bbox`; Python `ingest.geo.INDIA_BBOX`).
* Required dbt tests: not_null/unique on keys, accepted_values on `source`, relationships fct->dims, plus singular tests for bbox, delivery-time range and duplicates.

## 5. `quality.check_results`

One row per (`run_id`, `check_name`, `dataset`) with `rows_failed` and `rows_checked`. `run_id` = the pipeline `LOAD_ID`. Written by `python -m warehouse.quality` (layer `batch`) and the streaming consumer (layer `stream`). Minimum batch checks: `coords_outside_india`, `negative_or_absurd_delivery_time`, `duplicate_order_id`, `null_required_field`, `pincode_unmatched`; stream: `out_of_bounds_gps`, `duplicate_event`, `late_arriving_ping`.

## 6. Spark (`spark/`)

Reads `analytics.fct_deliveries` (`is_valid`) and dims via JDBC (driver jar auto-fetched or in `.tools/jars/`), writes the four `spark_marts.*` tables exactly as in `ddl.sql` (overwrite with `truncate=true`). Entry: `python -m spark.run_all`. Courier utilisation is Zomato-only (Amazon has no courier id); busy minutes = sum of `delivery_minutes`, on-shift = per courier-day (last order_ts+delivery - first order_ts), utilisation = busy/on-shift capped at 1.

## 7. Streaming (`streaming/`)

Topics: `orders.events` (lifecycle), `gps.pings`; key = `order_id`/`driver_id`, JSON value with `event_id`, `event_type`, `order_id`, `driver_id`, `vehicle_type`, `city='Bengaluru'`, `event_ts` (ISO8601 UTC), `lat`, `lon`, `speed_kmh`. Everything is **simulated** and labelled so. Distributions (hour-of-day demand, delivery-time by traffic/weather) are read from `analytics.*` or `spark_marts.*` if present, else from a checked-in JSON fallback `streaming/params_default.json`. Consumer lands to `raw.stream_events` (PK dedup), `raw.stream_rejects`, `raw.stream_runs`, and writes stream rows to `quality.check_results`. Entry: `python -m streaming.run --events N`.

## 8. Orchestration (`orchestration/`)

Airflow DAGs in `orchestration/dags/`: `ilm_batch_pipeline` (acquire -> load -> geo -> dbt build -> quality -> spark), `@daily`, `catchup` capable; each task calls the Make-target module with `RUN_DATE={{ ds }}`; tasks are idempotent. `ilm_streaming_demo` is manually triggered.

## 9. Make targets

`make up|down` (Docker), `native-up|native-down`, `setup`, `acquire`, `load`, `geo`, `dbt`, `quality`, `spark`, `stream`, `pipeline`, `demo`, `lint`, `test`. Helpers add code behind the same module entry points; do not rename targets.

## 10. Branches / merge protocol

`fm/ilm-platform-01` (ingest, orchestration, integration), `fm/ilm-warehouse-01` (warehouse/), `fm/ilm-spark-01` (spark/), `fm/ilm-stream-01` (streaming/). Each helper touches only its own directory (+ its tests under `tests/<component>/`) and may append to Makefile only inside its own target. Conflicts in `ingest/sql/ddl.sql` must be reported to the integration owner, not resolved unilaterally.
