#!/bin/bash
basedir=$(readlink -f $(dirname $0))

APP_NAME="spark-wordcount-mini"
APP_MAIN="de.dimajix.training.spark.wordcount.WordCountDriver"
APP_VERSION="1.0.0"

source $basedir/conf/common.sh

run "$@"
