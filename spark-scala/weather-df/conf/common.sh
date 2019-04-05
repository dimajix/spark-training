#!/usr/bin/env bash
basedir=$(readlink -f $(dirname $0))

: ${SPARK_MASTER:="yarn"}
: ${SPARK_EXECUTOR_CORES:="8"}
: ${SPARK_EXECUTOR_MEMORY:="16G"}
: ${SPARK_DRIVER_MEMORY:="1G"}

APP_JAR="$basedir/target/$APP_NAME-$APP_VERSION-jar-with-dependencies.jar"

# LIB_JARS=$(ls $basedir/lib/*.jar | awk -vORS=, '{ print $1 }' | sed 's/,$/\n/')

# Load configuration
source $basedir/conf/config.sh

# Apply any proxy settings
if [[ $PROXY_HOST != "" ]]; then
    SPARK_DRIVER_JAVA_OPTS="
        -Dhttp.proxyHost=${PROXY_HOST}
        -Dhttp.proxyPort=${PROXY_PORT}
        -Dhttps.proxyHost=${PROXY_HOST}
        -Dhttps.proxyPort=${PROXY_PORT}
        $SPARK_DRIVER_JAVA_OPTS"

    SPARK_EXECUTOR_JAVA_OPTS="
        -Dhttp.proxyHost=${PROXY_HOST}
        -Dhttp.proxyPort=${PROXY_PORT}
        -Dhttps.proxyHost=${PROXY_HOST}
        -Dhttps.proxyPort=${PROXY_PORT}
        $SPARK_EXECUTOR_JAVA_OPTS"

    SPARK_OPTS="
        --conf spark.hadoop.fs.s3a.proxy.host=${PROXY_HOST}
        --conf spark.hadoop.fs.s3a.proxy.port=${PROXY_PORT}
        $SPARK_OPTS"
fi

# Set AWS credentials if required
if [[ $AWS_ACCESS_KEY_ID != "" ]]; then
    SPARK_OPTS="
        --conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}
        --conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}
        $SPARK_OPTS"
fi


run() {
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
}
