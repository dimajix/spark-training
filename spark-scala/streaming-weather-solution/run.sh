#!/bin/bash

APP_NAME="spark-weather-streaming-solution"
APP_MAIN="de.dimajix.training.spark.weather.Driver"
APP_VERSION="1.0.0"

JAR_NAME="target/$APP_NAME-$APP_VERSION-jar-with-dependencies.jar"

SPARK_OPTS="--executor-cores 2
    --num-executors 3
    --executor-memory 1G
    --driver-memory 512M
    --conf spark.sql.shuffle.partitions=10
    --conf spark.ui.retainedJobs=25
    --conf spark.worker.ui.retainedExecutors=100
    --conf spark.sql.ui.retainedExecutions=10
    --conf spark.ui.retainedStages=100
    --conf spark.eventLog.enabled=false
    --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j-executor.properties
    --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j-driver.properties
    --conf spark.yarn.max.executor.failures=2
    --conf spark.task.maxFailures=2
    --conf spark.yarn.executor.memoryOverhead=512"

SPARK_MASTER="yarn"


spark-submit $SPARK_OPTS --master $SPARK_MASTER --class $APP_MAIN $JAR_NAME $@
