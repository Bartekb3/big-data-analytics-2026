#!/bin/sh
# Creates the Kafka topics with the settings from architecture/contracts/kafka-topics.md#topic-settings.
# Safe to run again: it keeps existing topics and their data, and applies the settings to them.
set -eu

BIN=/opt/kafka/bin
SERVER=kafka:9092
SETTINGS=retention.ms=259200000,segment.ms=86400000,message.timestamp.type=LogAppendTime

topic() {
    "$BIN/kafka-topics.sh" --bootstrap-server "$SERVER" --create --if-not-exists \
        --topic "$1" --partitions "$2" --replication-factor 1
    "$BIN/kafka-configs.sh" --bootstrap-server "$SERVER" --alter \
        --entity-type topics --entity-name "$1" --add-config "$SETTINGS"
    # --if-not-exists ignores a different partition count, and Kafka cannot lower it on an existing topic.
    description=$("$BIN/kafka-topics.sh" --bootstrap-server "$SERVER" --describe --topic "$1")
    echo "$description"
    echo "$description" | grep -q "PartitionCount: $2[^0-9]" || {
        echo "ERROR: topic $1 exists with a partition count other than $2" >&2
        exit 1
    }
}

topic roblox.snapshots 3
topic steam.players 1
