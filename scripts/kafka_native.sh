#!/usr/bin/env bash
# Native single-node Kafka (KRaft) using .tools/kafka + .tools/jdk. Usage: start|stop
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export JAVA_HOME="$ROOT/.tools/jdk" PATH="$ROOT/.tools/jdk/bin:$PATH"
K="$ROOT/.tools/kafka"; D="$ROOT/.kafkadata"
case "${1:-start}" in
  start)
    cat > "$ROOT/.kafka.properties" <<P
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=$D
num.partitions=3
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
P
    [ -d "$D" ] || "$K/bin/kafka-storage.sh" format -t "$("$K/bin/kafka-storage.sh" random-uuid)" -c "$ROOT/.kafka.properties" >/dev/null
    KAFKA_HEAP_OPTS="-Xmx512m -Xms256m" nohup "$K/bin/kafka-server-start.sh" "$ROOT/.kafka.properties" > "$ROOT/.kafka.log" 2>&1 &
    echo "kafka starting on 127.0.0.1:9092 (log .kafka.log)" ;;
  stop) "$K/bin/kafka-server-stop.sh" || true ;;
esac
