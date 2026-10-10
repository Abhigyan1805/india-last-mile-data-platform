# source scripts/env.sh  -- native runtime environment (no Docker)
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PGHOST=${PGHOST:-127.0.0.1} PGPORT=${PGPORT:-5433} PGUSER=${PGUSER:-ilm} PGPASSWORD=${PGPASSWORD:-ilm} PGDATABASE=${PGDATABASE:-warehouse}
export DATA_DIR=${DATA_DIR:-$ROOT/data} KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-127.0.0.1:9092}
export SPARK_MASTER=${SPARK_MASTER:-local[2]} SAMPLE_MODE=${SAMPLE_MODE:-0}
export JAVA_HOME=${JAVA_HOME:-$ROOT/.tools/jdk}; export PATH="$ROOT/.venv/bin:$JAVA_HOME/bin:$PATH"
export DBT_PROFILES_DIR="$ROOT/warehouse" PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
