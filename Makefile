SHELL := /bin/bash
export LOAD_ID ?= $(shell date +%Y%m%dT%H%M%S)
ENV := source scripts/env.sh &&
.PHONY: e2e warehouse-sample help up down native-up native-down setup acquire load geo dbt quality spark stream pipeline demo lint test

help:  ## list targets
	@grep -E '^[a-z-]+:.*##' Makefile | sed 's/:.*##/ -/'

up:  ## Docker stack: Postgres, Airflow, Kafka, Spark runner (reviewer path)
	docker compose up -d --build
down:  ## stop Docker stack
	docker compose down
native-up:  ## no-Docker runtime: native Postgres (+Kafka if .tools/kafka exists)
	scripts/pg_native.sh start
	@[ -x .tools/kafka/bin/kafka-server-start.sh ] && scripts/kafka_native.sh start || true
native-down:
	-scripts/kafka_native.sh stop
	-scripts/pg_native.sh stop

setup:  ## create venv + install pinned deps + JDK/Kafka for native runs
	uv venv --python 3.12 .venv && uv pip install -p .venv/bin/python -e '.[dev]'
	scripts/fetch_tools.sh

acquire:  ## download Kaggle/pincode/weather raw files (idempotent)
	$(ENV) python -m ingest.acquire
load:  ## create schemas + load raw files into Postgres raw.*
	$(ENV) python -m ingest.load
geo:  ## nearest-pincode enrichment -> raw.delivery_geo (reports match rate)
	$(ENV) python -m ingest.geo && python -m ingest.weather
dbt:  ## dbt deps/run/test
	$(ENV) cd warehouse && dbt build --profiles-dir .
quality:  ## custom quality checks -> quality.check_results
	$(ENV) python -m warehouse.quality
spark:  ## PySpark aggregates -> spark_marts.*
	$(ENV) python -m spark.run_all
spark-sample:  ## Spark marts from the in-memory sample fixture (no Postgres needed)
	$(ENV) python -m spark.run_all --sample --no-write
spark-test:  ## Spark job tests on tiny fixtures (local SparkSession)
	$(ENV) pytest -q tests/spark
.PHONY: spark-sample spark-test
stream:  ## Kafka simulator + consumer (see streaming/README)
	$(ENV) python -m streaming.run
e2e:  ## one command: services + full pipeline + streaming + docs/RESULTS.md
	scripts/e2e_run.sh
pipeline: acquire load geo dbt quality spark  ## full batch pipeline, natively
demo:  ## small-slice end-to-end run (minutes)
	SAMPLE_MODE=1 $(MAKE) pipeline
lint:
	$(ENV) ruff check .
test:
	$(ENV) pytest -q

warehouse-sample:  ## warehouse on the tiny fixture (own DB warehouse_sample): load -> dbt build -> quality; set PGPORT if not 5433
	$(ENV) export PGDATABASE=warehouse_sample && python -m warehouse.load_sample && (cd warehouse && dbt build --full-refresh --profiles-dir .) && python -m warehouse.quality

# --- streaming/ (own native runtimes on non-default ports; see docs/streaming.md) ---
.PHONY: stream-up stream-down stream-test stream-million
stream-up:  ## start streaming's isolated Kafka (9094/9095) + Postgres (5436)
	streaming/kafka_stream.sh start
	PGPORT=5436 streaming/native_pg.sh start
stream-down:  ## stop streaming's isolated Kafka + Postgres
	-streaming/kafka_stream.sh stop
	-PGPORT=5436 streaming/native_pg.sh stop
stream-test:  ## streaming unit tests (validation/dedup; no Kafka required)
	$(ENV) pytest -q tests/streaming
stream-million:  ## stream ~1,000,000 simulated events end-to-end on the native stack
	export KAFKA_BOOTSTRAP=127.0.0.1:9094 PGPORT=5436; $(ENV) python -m streaming.run --events 1000000
