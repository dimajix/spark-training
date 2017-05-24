#!/bin/bash
basedir=$(readlink -f $(dirname $0))

APP_NAME="spark-streaming-recoverable-wordcount"
APP_MAIN="de.dimajix.training.spark.wordcount.NetworkDriver"
APP_VERSION="1.0.0"

source $basedir/conf/common.sh

run "$@"
