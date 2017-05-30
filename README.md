# Spark Training Repository

This repository contains many different examples, exercises and tutorials for Spark and Hadoop trainings performed
by dimajix. You can always find the latest version on GitHub at

    https://github.com/dimajix/spark-training


## Contents

The repository contains different types of documents
* Source Code for Spark/Scala
* Jupyter Notebooks for PySpark
* Zeppelin Notebooks for Spark/Scala
* Hive SQL scripts
* Pig scripts
* ...and much more


## External Dependencies

Some notebooks require some test data provided by dimajix on S3 at s3://dimajix-training/data/.


## Building Executables

The source code can be built using Maven, simply by running

    mvn install

from the root directory.


## Running Examples

Most code is either provided as interactive Notebooks (Jupyter and/or Zeppelin) or as compilable programs. Programs
which create jar files always contain start scripts, which take care of setting any environment variables and Spark
configuration properties.