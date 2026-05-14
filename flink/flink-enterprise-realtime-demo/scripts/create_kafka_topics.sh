#!/usr/bin/env bash

set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-/opt/module/kafka_2.12-3.9.2}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"

for topic in rt_order_events rt_payment_events rt_page_view_events; do
  "${KAFKA_HOME}/bin/kafka-topics.sh" \
    --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions 1 \
    --replication-factor 1
done

"${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --list
