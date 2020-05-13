#!/bin/bash
basedir=$(readlink -f $(dirname $0))

APP_NAME="spark-weather-df"
APP_MAIN="com.dimajix.training.spark.weather.Driver"
APP_VERSION="1.0.0"

source $basedir/conf/common.sh

run "$@"
