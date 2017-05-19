#!/bin/bash

export ANACONDA_ROOT=/opt/anaconda3
export PYSPARK_DRIVER_PYTHON=$ANACONDA_ROOT/bin/jupyter
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --NotebookApp.open_browser=True --NotebookApp.ip=0.0.0.0 --NotebookApp.port=8880"
export PYSPARK_PYTHON=$ANACONDA_ROOT/bin/python 

export TZ=UTC
export PYTHONHASHSEED=0
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0

pyspark --master=yarn-client

