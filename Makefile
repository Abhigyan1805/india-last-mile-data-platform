SHELL := /bin/bash
ENV := source scripts/env.sh &&
.PHONY: warehouse-sample help up down native-up native-down setup acquire load geo dbt quality spark stream pipeline demo lint test

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
	$(ENV) python -m ingest.geo
dbt:  ## dbt deps/run/test
	$(ENV) cd warehouse && dbt build --profiles-dir .
quality:  ## custom quality checks -> quality.check_results
	$(ENV) python -m warehouse.quality
spark:  ## PySpark aggregates -> spark_marts.*
	$(ENV) python -m spark.run_all
stream:  ## Kafka simulator + consumer (see streaming/README)
	$(ENV) python -m streaming.run
pipeline: acquire load geo dbt quality spark  ## full batch pipeline, natively
demo:  ## small-slice end-to-end run (minutes)
	SAMPLE_MODE=1 $(MAKE) pipeline
lint:
	$(ENV) ruff check . && ruff format --check .
test:
	$(ENV) pytest -q

warehouse-sample:  ## warehouse on the tiny fixture (own DB warehouse_sample): load -> dbt build -> quality; set PGPORT if not 5433
	$(ENV) export PGDATABASE=warehouse_sample && python -m warehouse.load_sample && (cd warehouse && dbt build --full-refresh --profiles-dir .) && python -m warehouse.quality
