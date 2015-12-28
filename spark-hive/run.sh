#!/bin/bash

APP_NAME="spark-hive"
APP_MAIN="de.dimajix.training.spark.hive.Driver"
APP_VERSION="0.1.0"
CDH_VERSION="cdh5.5.0"

JAR_NAME="target/$APP_NAME-$APP_VERSION-$CDH_VERSION-jar-with-dependencies.jar"
EXTRA_CLASSPATH="/usr/lib/hive/lib/hive-common.jar:/usr/lib/hive/lib/hive-serde.jar:/usr/lib/hive/lib/hive-exec.jar:/usr/lib/hive/lib/hive-metastore.jar:/usr/lib/hive/lib/libfb303-0.9.2.jar"

SPARK_OPTS="--executor-cores 2
    --executor-memory 1G
    --driver-memory 512M
    --driver-class-path $EXTRA_CLASSPATH
    --conf spark.executor.extraClassPath=$EXTRA_CLASSPATH
    --conf spark.shuffle.memoryFraction=0.8
    --conf spark.storage.memoryFraction=0.1
    --conf spark.yarn.max.executor.failures=2
    --conf spark.task.maxFailures=2
    --conf spark.yarn.executor.memoryOverhead=512"

SPARK_MASTER="yarn"


spark-submit $SPARK_OPTS --master $SPARK_MASTER --class $APP_MAIN $JAR_NAME $@
