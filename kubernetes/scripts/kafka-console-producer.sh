#!/usr/bin/env bash

kubectl \
    run \
    --generator=run-pod/v1 kafka-producer \
    -i \
    --tty \
    --rm \
    --image=wurstmeister/kafka:2.12-2.5.0 \
    --command \
    -- \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka-bootstrap:9092 \
    "$@"
