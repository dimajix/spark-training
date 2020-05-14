#!/usr/bin/env bash

kubectl \
    run \
    --generator=run-pod/v1 kafka-consumer \
    -i \
    --tty \
    --rm \
    --image=wurstmeister/kafka:2.12-2.5.0 \
    --command \
    -- \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka-bootstrap:9092 \
    "$@"
