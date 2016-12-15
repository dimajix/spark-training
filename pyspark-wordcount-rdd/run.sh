#!/usr/bin/env bash

DRIVER_NAME="wordcount.py"

SPARK_OPTS="--executor-cores 2
    --executor-memory 2G
    --driver-memory 512M
    --conf spark.yarn.max.executor.failures=2
    --conf spark.task.maxFailures=2"

SPARK_MASTER="yarn"

PYTHON_ROOT=/usr

export PYSPARK_DRIVER_PYTHON=$PYTHON_ROOT/bin/python3
export PYSPARK_PYTHON=$PYTHON_ROOT/bin/python3

export PYTHONHASHSEED=0
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0
export SPARK_MAJOR_VERSION=2

spark-submit $SPARK_OPTS --master $SPARK_MASTER $DRIVER_NAME $@
