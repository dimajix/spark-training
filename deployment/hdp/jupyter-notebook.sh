#!/bin/bash

BASEDIR=$(readlink -f $(dirname $0))

source $BASEDIR/pyspark/bin/activate

export ANACONDA_ROOT=$BASEDIR/pyspark
export JUPYTER_OPTS="notebook --NotebookApp.open_browser=True --NotebookApp.ip=0.0.0.0"

jupyter $JUPYTER_OPTS

