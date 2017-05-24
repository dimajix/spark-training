#!/bin/bash
basedir=$(readlink -f $(dirname $0))

APP_NAME="spark-skeleton"
APP_MAIN="de.dimajix.training.spark.skeleton.Driver"
APP_VERSION="1.0.0"

source $basedir/conf/common.sh

run "$@"
