-- Streaming landing tables. Authoritative copy lives in ingest/sql/ddl.sql
-- (docs/CONTRACTS.md section 7); repeated here so a standalone streaming run can
-- create its own tables idempotently without depending on the batch loader.
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS quality;

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

-- Owned by warehouse/ingest (see CONTRACTS section 5); created here so a
-- standalone streaming run can record its stream checks.
CREATE TABLE IF NOT EXISTS quality.check_results (
  run_id text NOT NULL, run_ts timestamptz NOT NULL DEFAULT now(),
  layer text NOT NULL,          -- 'batch' | 'stream'
  check_name text NOT NULL, dataset text NOT NULL,
  rows_checked bigint, rows_failed bigint NOT NULL, details jsonb,
  PRIMARY KEY (run_id, check_name, dataset)
);
