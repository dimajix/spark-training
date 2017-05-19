#!/bin/bash

BASEDIR=$(dirname $0)

source $BASEDIR/pyspark/bin/activate

export ANACONDA_ROOT=$BASEDIR/pyspark
export PYSPARK_DRIVER_PYTHON=$ANACONDA_ROOT/bin/jupyter
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --NotebookApp.open_browser=False --NotebookApp.ip=0.0.0.0 --Notebook.port=8888"
export PYSPARK_PYTHON=$ANACONDA_ROOT/bin/python

export TZ=UTC
export PYTHONHASHSEED=0
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0

PACKAGES="com.databricks:spark-csv_2.10:1.4.0"

export SPARK_OPTS="--master=yarn --executor-cores=4 --driver-memory=4G --packages=\"$PACKAGES\""

pyspark --master=local[12] --driver-memory=8G --jars=$JARS --packages=$PACKAGES --executor-cores=4 --executor-memory=4G --num-executors=2
