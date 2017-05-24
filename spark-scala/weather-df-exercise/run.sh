#!/bin/bash
basedir=$(readlink -f $(dirname $0))

APP_NAME="spark-weather-df-exercise"
APP_MAIN="de.dimajix.training.spark.weather.Driver"
APP_VERSION="1.0.0"

Ssource $basedir/conf/common.sh

run "$@"
