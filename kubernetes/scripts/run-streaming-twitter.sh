#!/usr/bin/env bash

APP_NAME="spark-streaming-twitter"
APP_MAIN="com.dimajix.training.spark.twitter.Driver"
APP_VERSION="1.0.0"

basedir=$(readlink -f $(dirname $0))

K8S_CONTEXT=$(kubectl config view -o jsonpath="{.current-context}")
K8S_MASTER=$(kubectl config view -o jsonpath="{.clusters[?(@.name==\"$K8S_CONTEXT\")].cluster.server}")
K8S_NAMESPACE=$(whoami)

: ${SPARK_MASTER:="k8s://$K8S_MASTER"}
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
    --conf spark.kubernetes.namespace=$K8S_NAMESPACE \
    --conf spark.sql.shuffle.partitions=8 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.executor.instances=$SPARK_EXECUTOR_INSTANCES \
    --conf spark.kubernetes.container.image=874361956431.dkr.ecr.eu-central-1.amazonaws.com/dimajix-training/twitter-streaming \
    $APP_JAR "$@"
