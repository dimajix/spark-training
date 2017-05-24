#!/bin/bash
basedir=$(readlink -f $(dirname $0))

APP_NAME="spark-wordcount-filtered"
APP_MAIN="de.dimajix.training.spark.wordcount.Driver"
APP_VERSION="1.0.0"

source $basedir/conf/common.sh

run "$@"
