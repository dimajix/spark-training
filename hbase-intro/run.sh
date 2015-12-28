#!/bin/bash

APP_NAME="hbase-intro"
APP_MAIN="de.dimajix.training.hbase.Example"
APP_VERSION="0.1.0"
CDH_VERSION="cdh5.5.0"

JAR_NAME="target/$APP_NAME-$APP_VERSION-$CDH_VERSION-jar-with-dependencies.jar"

hadoop jar $JAR_NAME $APP_MAIN $@
