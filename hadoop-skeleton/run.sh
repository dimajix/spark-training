#!/bin/bash

APP_NAME="hadoop-skeleton"
APP_MAIN="de.dimajix.training.hadoop.skeleton.Driver"
APP_VERSION="1.0.0"

JAR_NAME="target/$APP_NAME-$APP_VERSION-jar-with-dependencies.jar"

hadoop jar $JAR_NAME $APP_MAIN $@

