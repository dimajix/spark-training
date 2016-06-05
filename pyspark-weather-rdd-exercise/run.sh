#!/usr/bin/env bash

DRIVER_NAME="driver.py"
PYTHON_FILES="weather.py"

SPARK_OPTS="--executor-cores 2
    --executor-memory 2G
    --driver-memory 512M
    --conf spark.yarn.max.executor.failures=2
    --conf spark.task.maxFailures=2"

SPARK_MASTER="yarn"


spark-submit $SPARK_OPTS --py-files $PYTHON_FILES --master $SPARK_MASTER $DRIVER_NAME $@
