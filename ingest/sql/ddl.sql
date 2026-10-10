-- Authoritative DDL for schemas owned by ingest. Applied (idempotently) by `python -m ingest.load`.
-- See docs/CONTRACTS.md. Source columns land as TEXT (ELT: dbt staging casts/cleans).
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS quality;
CREATE SCHEMA IF NOT EXISTS spark_marts;

CREATE TABLE IF NOT EXISTS raw.amazon_deliveries (
  order_id text, agent_age text, agent_rating text,
  store_latitude text, store_longitude text, drop_latitude text, drop_longitude text,
  order_date text, order_time text, pickup_time text,
  weather text, traffic text, vehicle text, area text, delivery_time text, category text,
  _load_id text NOT NULL, _loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.zomato_deliveries (
  id text, delivery_person_id text, delivery_person_age text, delivery_person_ratings text,
  restaurant_latitude text, restaurant_longitude text,
  delivery_location_latitude text, delivery_location_longitude text,
  order_date text, time_orderd text, time_order_picked text,
  weather_conditions text, road_traffic_density text, vehicle_condition text,
  type_of_order text, type_of_vehicle text, multiple_deliveries text, festival text,
  city text, time_taken_min text,
  _load_id text NOT NULL, _loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.pincodes (
  circlename text, regionname text, divisionname text, officename text, pincode text,
  officetype text, delivery text, district text, statename text, latitude text, longitude text,
  _load_id text NOT NULL, _loaded_at timestamptz NOT NULL DEFAULT now()
);

-- Output of nearest-pincode enrichment. One row per (source, order, pickup|drop point).
CREATE TABLE IF NOT EXISTS raw.delivery_geo (
  source text NOT NULL CHECK (source IN ('amazon','zomato')),
  order_id text NOT NULL,
  point_kind text NOT NULL CHECK (point_kind IN ('pickup','drop')),
  latitude double precision, longitude double precision,
  in_india_bbox boolean NOT NULL,
  pincode text, office_name text, district text, state text,
  city text,                 -- canonical city label (see CONTRACTS: city mapping)
  distance_km double precision,
  matched boolean NOT NULL,  -- in_india_bbox AND distance_km <= GEO_MATCH_MAX_KM (default 10)
  _load_id text NOT NULL, _loaded_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source, order_id, point_kind, _load_id)
);

CREATE TABLE IF NOT EXISTS raw.city_ref (
  city text PRIMARY KEY, state text, latitude double precision, longitude double precision
);

CREATE TABLE IF NOT EXISTS raw.weather_daily (
  city text NOT NULL, date date NOT NULL,
  temp_max_c double precision, temp_min_c double precision,
  precipitation_mm double precision, rain_mm double precision,
  wind_max_kmh double precision, weather_code integer,
  _load_id text NOT NULL, _loaded_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (city, date)
);

CREATE TABLE IF NOT EXISTS raw.ingest_runs (
  load_id text PRIMARY KEY, started_at timestamptz, finished_at timestamptz,
  sample_mode boolean, rows_amazon bigint, rows_zomato bigint, rows_pincodes bigint,
  rows_weather bigint, geo_points bigint, geo_matched bigint, geo_match_rate double precision
);

-- Written by dbt/quality checks (warehouse/) and streaming consumer. One row per check per run.
CREATE TABLE IF NOT EXISTS quality.check_results (
  run_id text NOT NULL, run_ts timestamptz NOT NULL DEFAULT now(),
  layer text NOT NULL,          -- 'batch' | 'stream'
  check_name text NOT NULL, dataset text NOT NULL,
  rows_checked bigint, rows_failed bigint NOT NULL, details jsonb,
  PRIMARY KEY (run_id, check_name, dataset)
);

-- Streaming landing (owned by streaming/, DDL here so every owner sees one definition).
CREATE TABLE IF NOT EXISTS raw.stream_events (
  event_id text PRIMARY KEY, event_type text NOT NULL,   -- order_created|driver_assigned|picked_up|delivered|gps_ping
  order_id text, driver_id text, vehicle_type text,       -- two_wheeler|truck
  city text, event_ts timestamptz NOT NULL, received_ts timestamptz NOT NULL,
  latitude double precision, longitude double precision, speed_kmh double precision,
  is_late boolean NOT NULL DEFAULT false, payload jsonb, _load_id text NOT NULL
);
CREATE TABLE IF NOT EXISTS raw.stream_rejects (
  event_id text, reason text NOT NULL,                    -- duplicate|out_of_bounds|malformed
  payload jsonb, received_ts timestamptz NOT NULL DEFAULT now(), _load_id text NOT NULL
);
CREATE TABLE IF NOT EXISTS raw.stream_runs (
  load_id text PRIMARY KEY, produced bigint, consumed bigint, landed bigint,
  duplicates_dropped bigint, late_events bigint, out_of_bounds bigint, malformed bigint,
  duration_s double precision, events_per_s double precision
);

-- Written by spark/ (schema spark_marts). `source` = amazon|zomato.
CREATE TABLE IF NOT EXISTS spark_marts.deliveries_per_pincode_hour (
  source text, pincode text, city text, hour_of_day smallint,
  deliveries bigint, avg_delivery_min double precision, p90_delivery_min double precision
);
CREATE TABLE IF NOT EXISTS spark_marts.courier_utilisation (
  source text, courier_id text, city text, active_days integer, deliveries bigint,
  busy_minutes double precision, on_shift_minutes double precision, utilisation double precision
);
CREATE TABLE IF NOT EXISTS spark_marts.delay_impact (
  source text, factor text,                -- weather|traffic|festival|rain_bucket
  factor_value text, deliveries bigint, avg_delivery_min double precision,
  delay_vs_baseline_min double precision   -- vs. the lowest-impact baseline value of that factor
);
CREATE TABLE IF NOT EXISTS spark_marts.city_comparison (
  source text, city text, deliveries bigint, avg_delivery_min double precision,
  p90_delivery_min double precision, pct_over_45_min double precision,
  avg_distance_km double precision, pincode_match_rate double precision
);
