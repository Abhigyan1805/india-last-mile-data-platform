"""Daily batch pipeline: ingest -> load -> geo -> dbt -> quality -> spark.

Backfill-capable and idempotent: each run processes the `order_date` partition equal to the logical
date (`{{ ds }}`) for the delivery sources; geo/weather/dbt/spark steps are full-refresh safe.
Task commands call the same module entry points as the Makefile, so a DAG run == `make pipeline`.
Set ILM_FULL_REFRESH=1 (Variable or env) to ignore partitions and process everything in one run.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

ROOT = os.environ.get("ILM_ROOT", "/app")
ENV = f"cd {ROOT} && source scripts/env.sh && export LOAD_ID='{{{{ ts_nodash }}}}' "
PART = "export PARTITION_DATE='{{ ds }}' && "

default_args = {"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="ilm_batch_pipeline",
    description="India last-mile delivery platform: raw -> geo -> dbt -> quality -> Spark",
    start_date=datetime(2022, 2, 11),
    end_date=datetime(2022, 4, 6),  # order dates present in the source data
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["ilm"],
) as dag:
    acquire = BashOperator(task_id="acquire", bash_command=f"{ENV}&& python -m ingest.acquire")
    load_raw = BashOperator(task_id="load_raw", bash_command=f"{ENV}&& {PART}python -m ingest.load")
    geo = BashOperator(
        task_id="geo_enrich",
        bash_command=f"{ENV}&& python -m ingest.geo && python -m ingest.weather",
    )
    dbt_build = BashOperator(
        task_id="dbt_build", bash_command=f"{ENV}&& cd warehouse && dbt build --profiles-dir ."
    )
    quality = BashOperator(task_id="quality_checks", bash_command=f"{ENV}&& python -m warehouse.quality")
    spark = BashOperator(task_id="spark_aggregates", bash_command=f"{ENV}&& python -m spark.run_all")

    acquire >> load_raw >> geo >> dbt_build >> quality >> spark
