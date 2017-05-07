#!/usr/bin/env bash
SPARK_OPTS="--executor-cores 2
    --executor-memory 1G
    --driver-memory 512M
    --conf spark.shuffle.memoryFraction=0.8
    --conf spark.storage.memoryFraction=0.1
    --conf spark.yarn.max.executor.failures=2
    --conf spark.task.maxFailures=2
    --conf spark.yarn.executor.memoryOverhead=512"

SPARK_MASTER="yarn"
