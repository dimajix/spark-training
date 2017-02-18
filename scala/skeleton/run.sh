#!/bin/bash

APP_NAME="scala-skeleton"
APP_MAIN="de.dimajix.training.scala.skeleton.SkeletonApp"
APP_VERSION="1.0.0"

JAR_NAME="target/$APP_NAME-$APP_VERSION-jar-with-dependencies.jar"

java -cp $JAR_NAME $APP_MAIN $@
