#!/usr/bin/env bash
SPARK_OPTS="--executor-cores 2
    --executor-memory 2G
    --driver-memory 1G
    --conf spark.shuffle.memoryFraction=0.8
    --conf spark.storage.memoryFraction=0.1
    --conf spark.yarn.executor.memoryOverhead=512"

SPARK_MASTER="yarn"
