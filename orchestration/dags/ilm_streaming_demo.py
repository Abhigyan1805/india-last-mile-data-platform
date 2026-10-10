"""Manually triggered: run the Bengaluru order-stream simulator + consumer (SIMULATED data)."""
from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

ROOT = os.environ.get("ILM_ROOT", "/app")

with DAG(
    dag_id="ilm_streaming_demo",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={"events": 200000},
    tags=["ilm", "simulated"],
) as dag:
    BashOperator(
        task_id="simulate_and_consume",
        bash_command=(
            f"cd {ROOT} && source scripts/env.sh && "
            "python -m streaming.run --events {{ params.events }}"
        ),
    )
