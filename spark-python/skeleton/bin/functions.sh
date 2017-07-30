#!/usr/bin/env bash

# Fundamental Variables
BASEDIR=$(readlink -f $(dirname $0)/..)
PYTHON_ROOT=/emr/anaconda3

# Derived common properties
APP_NAME=${APP_NAME:-$(basename -s .sh $0)}
LIBDIR=${BASEDIR}/lib
CONFDIR=${BASEDIR}/conf

COMMON_CONFIG_FILE=${CONFDIR}/common.conf
APP_CONFIG_FILE="${CONFDIR}/${APP_NAME}.conf"

DRIVER_FILE="${LIBDIR}/${APP_NAME}.py"
PYTHON_FILES="${LIBDIR}/${APP_NAME}.zip"

# Collect all Python files in lib directory, if zip does not exist
if [ ! -f $PYTHON_FILES ]; then
    PYTHON_FILES=$(find ${LIBDIR} -name "*.py" | paste -d, -s)
fi

# Define default values for more options, these can be overriden in config files
: ${SPARK_MASTER:="yarn"}
: ${SPARK_EXECUTOR_CORES:="1"}
: ${SPARK_EXECUTOR_MEMORY:="1G"}
: ${SPARK_DRIVER_MEMORY:="1G"}
SPARK_OPTS="
    --conf spark.sql.shuffle.partitions=48
    --conf spark.memory.fraction=0.2
    --conf spark.memory.storageFraction=0.1
    --conf spark.yarn.max.executor.failures=2
    --conf spark.task.maxFailures=2"

# Load config file, if it exists
if [ -f $COMMON_CONFIG_FILE ]; then
    source ${COMMON_CONFIG_FILE}
fi
if [ -f $APP_CONFIG_FILE ]; then
    source ${APP_CONFIG_FILE}
fi

export PYSPARK_DRIVER_PYTHON=$PYTHON_ROOT/bin/python
export PYSPARK_PYTHON=$PYTHON_ROOT/bin/python

export PYTHONHASHSEED=0
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0


function run
{
    spark-submit \
        --py-files $PYTHON_FILES \
        --master $SPARK_MASTER \
        --executor-cores $SPARK_EXECUTOR_CORES \
        --executor-memory $SPARK_EXECUTOR_MEMORY \
        --driver-memory $SPARK_DRIVER_MEMORY \
        --driver-java-options "$SPARK_DRIVER_JAVA_OPTS" \
        --conf spark.executor.extraJavaOptions="$SPARK_EXECUTOR_JAVA_OPTS" \
        $SPARK_OPTS \
        $DRIVER_FILE "$@"
}
