#!/bin/sh
set -eu

bootstrap_server="${KAFKA_BOOTSTRAP_SERVER:-kafka:29092}"

for topic in \
  activation.requested \
  billing.events \
  provisioning.events \
  activation.events
 do
  kafka-topics \
    --bootstrap-server "$bootstrap_server" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000

  kafka-topics \
    --bootstrap-server "$bootstrap_server" \
    --create \
    --if-not-exists \
    --topic "$topic.dlq" \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000
done
