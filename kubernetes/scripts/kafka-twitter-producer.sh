#!/usr/bin/env bash

basedir=$(readlink -f $(dirname $0))
rootdir=$(readlink -f $basedir/../..)

$rootdir/utils/s3cat.py s3://dimajix-training/data/twitter-sample/ \
    | kubectl \
          run \
          --generator=run-pod/v1 kafka-producer-tweets \
          -i \
          --rm \
          --image=wurstmeister/kafka:2.12-2.5.0 \
          --command \
          -- \
          /opt/kafka/bin/kafka-console-producer.sh \
          --bootstrap-server kafka-bootstrap:9092 \
          --topic tweets
