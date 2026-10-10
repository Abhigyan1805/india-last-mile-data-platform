#!/usr/bin/env bash
# Native single-node Kafka (KRaft) for the streaming piece, no Docker.
# Uses .tools/kafka + .tools/jdk (fetched by scripts/fetch_tools.sh).
# Runs on its own ports so it never clashes with the batch lane's broker:
#   PLAINTEXT 127.0.0.1:9094   CONTROLLER 127.0.0.1:9095
# Usage: streaming/kafka_stream.sh start|stop|status
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export JAVA_HOME="$ROOT/.tools/jdk" PATH="$ROOT/.tools/jdk/bin:$PATH"
K="$ROOT/.tools/kafka"
D="$ROOT/data/kafka-stream"
PORT="${KAFKA_PORT:-9094}"
CTRL="${KAFKA_CONTROLLER_PORT:-9095}"
PROPS="$D/kafka.properties"
PIDFILE="$D/kafka.pid"

case "${1:-status}" in
  start)
    mkdir -p "$D/logs"
    cat > "$PROPS" <<P
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:$CTRL
listeners=PLAINTEXT://127.0.0.1:$PORT,CONTROLLER://127.0.0.1:$CTRL
advertised.listeners=PLAINTEXT://127.0.0.1:$PORT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=$D/logs
num.partitions=3
auto.create.topics.enable=false
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
P
    [ -f "$D/logs/meta.properties" ] || \
      "$K/bin/kafka-storage.sh" format -t "$("$K/bin/kafka-storage.sh" random-uuid)" -c "$PROPS" >/dev/null
    KAFKA_HEAP_OPTS="-Xmx512m -Xms256m" nohup "$K/bin/kafka-server-start.sh" "$PROPS" \
      > "$D/kafka.log" 2>&1 &
    echo $! > "$PIDFILE"
    for _ in $(seq 1 60); do
      if "$K/bin/kafka-broker-api-versions.sh" --bootstrap-server "127.0.0.1:$PORT" >/dev/null 2>&1; then
        echo "kafka up on 127.0.0.1:$PORT (controller $CTRL), pid $(cat "$PIDFILE")"
        exit 0
      fi
      sleep 1
    done
    echo "kafka failed to start; see $D/kafka.log" >&2
    exit 1 ;;
  stop)
    if [ -f "$PIDFILE" ]; then
      kill "$(cat "$PIDFILE")" 2>/dev/null || true
      rm -f "$PIDFILE"
    fi
    "$K/bin/kafka-server-stop.sh" >/dev/null 2>&1 || true
    echo "kafka stopped" ;;
  status)
    if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
      echo "kafka running (pid $(cat "$PIDFILE")) on 127.0.0.1:$PORT"
    else
      echo "kafka not running"
    fi ;;
esac
