#!/bin/bash

ROOT_DIR=$(readlink -e $(dirname $0))/pyspark
echo "ROOT_DIR:" $ROOT_DIR

source $ROOT_DIR/bin/activate

export PYSPARK_DRIVER_PYTHON=$ROOT_DIR/bin/jupyter
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --NotebookApp.open_browser=False --NotebookApp.ip=0.0.0.0 --Notebook.port=8888"
export PYSPARK_PYTHON=$ROOT_DIR/bin/python

export TZ=UTC
export PYTHONHASHSEED=0
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0
export SPARK_MAJOR_VERSION=2

SPARK_OPTS="--master=yarn --driver-memory=4G --executor-cores=4 --executor-memory=4G --num-executors=2"

pyspark $SPARK_OPTS

