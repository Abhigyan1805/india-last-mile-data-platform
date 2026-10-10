#!/usr/bin/env bash
# Streaming lane's isolated native PostgreSQL (no Docker), on its own data dir
# and port so it never touches the batch lane's cluster (.pgdata / 5433).
# Binaries come from the pgserver wheel. Usage: start|stop|status
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PGBIN="$ROOT/.venv/lib/python3.12/site-packages/pgserver/pginstall/bin"
PGDATA="$ROOT/data/pg-stream"
PORT="${PGPORT:-5436}"
SOCK=/tmp

case "${1:-status}" in
  start)
    if [ ! -d "$PGDATA" ]; then
      "$PGBIN/initdb" -D "$PGDATA" -U ilm --auth=trust -E UTF8 >/dev/null
    fi
    "$PGBIN/pg_ctl" -D "$PGDATA" status >/dev/null 2>&1 || \
      "$PGBIN/pg_ctl" -D "$PGDATA" -o "-p $PORT -c listen_addresses=127.0.0.1 \
        -c unix_socket_directories=$SOCK -c shared_buffers=256MB -c max_connections=50" \
        -l "$ROOT/logs/pg-stream.log" -w start >/dev/null
    "$PGBIN/psql" -h 127.0.0.1 -p "$PORT" -U ilm -d postgres -tc \
      "select 1 from pg_database where datname='warehouse'" | grep -q 1 || \
      "$PGBIN/createdb" -h 127.0.0.1 -p "$PORT" -U ilm warehouse
    echo "postgres up on 127.0.0.1:$PORT db=warehouse user=ilm" ;;
  stop)
    "$PGBIN/pg_ctl" -D "$PGDATA" -m fast stop ;;
  status)
    "$PGBIN/pg_ctl" -D "$PGDATA" status ;;
esac
