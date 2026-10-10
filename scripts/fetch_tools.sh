#!/usr/bin/env bash
# Fetch JDK 17 and Kafka into .tools/ (gitignored) for native (no-Docker) runs.
set -euo pipefail
cd "$(dirname "$0")/.." && mkdir -p .tools && cd .tools
[ -d jdk ] || { curl -sL -o jdk.tgz "https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jdk/hotspot/normal/eclipse" && tar xzf jdk.tgz && rm jdk.tgz && mv jdk-17* jdk; }
[ -d kafka ] || { curl -sL -o k.tgz https://downloads.apache.org/kafka/4.2.2/kafka_2.13-4.2.2.tgz && tar xzf k.tgz && rm k.tgz && mv kafka_2.13-4.2.2 kafka; }
