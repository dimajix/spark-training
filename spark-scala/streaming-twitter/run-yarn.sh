#!/usr/bin/env bash
APP_NAME="spark-streaming-twitter"
APP_MAIN="com.dimajix.training.spark.twitter.Driver"
APP_VERSION="1.0.0"

basedir=$(readlink -f $(dirname $0))

: ${SPARK_MASTER:="yarn"}
: ${SPARK_EXECUTOR_CORES:="8"}
: ${SPARK_EXECUTOR_MEMORY:="16G"}
: ${SPARK_DRIVER_MEMORY:="1G"}

APP_JAR="$basedir/target/$APP_NAME-$APP_VERSION-jar-with-dependencies.jar"


spark-submit \
      --executor-cores $SPARK_EXECUTOR_CORES \
      --executor-memory $SPARK_EXECUTOR_MEMORY \
      --driver-memory $SPARK_DRIVER_MEMORY \
      --driver-java-options "$SPARK_DRIVER_JAVA_OPTS" \
      --conf spark.executor.extraJavaOptions="$SPARK_EXECUTOR_JAVA_OPTS" \
      --master $SPARK_MASTER \
      --class $APP_MAIN \
      $SPARK_OPTS \
      $APP_JAR "$@"
