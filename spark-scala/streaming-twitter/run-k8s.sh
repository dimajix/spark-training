#!/usr/bin/env bash

APP_NAME="spark-streaming-twitter"
APP_MAIN="com.dimajix.training.spark.twitter.Driver"
APP_VERSION="1.0.0"

basedir=$(readlink -f $(dirname $0))

: ${SPARK_MASTER:="k8s://https://192.168.99.109:8443"}
: ${SPARK_EXECUTOR_INSTANCES:="2"}
: ${SPARK_EXECUTOR_CORES:="2"}
: ${SPARK_EXECUTOR_MEMORY:="2G"}
: ${SPARK_DRIVER_MEMORY:="1G"}

APP_JAR="local:///opt/dimajix/jars/$APP_NAME-$APP_VERSION-jar-with-dependencies.jar"

spark-submit \
    --master $SPARK_MASTER \
    --deploy-mode cluster \
    --name Streaming-Twitter \
    --class $APP_MAIN \
    --conf spark.kubernetes.namespace=default \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.executor.instances=$SPARK_EXECUTOR_INSTANCES \
    --conf spark.kubernetes.container.image=874361956431.dkr.ecr.eu-central-1.amazonaws.com/dimajix-training/streaming-twitter \
    $APP_JAR "$@"
