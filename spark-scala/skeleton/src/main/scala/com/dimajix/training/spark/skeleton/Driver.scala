package com.dimajix.training.spark.skeleton

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
  * Created by kaya on 03.12.15.
  */
object Driver {
    def main(args: Array[String]) : Unit = {
        // First create driver, so can already process arguments
        val options = new Options(args)
        val driver = new Driver(options)

        // Now create SparkContext (possibly flooding the console with logging information)
        val conf = new SparkConf()
        val spark = SparkSession.builder()
            .config(conf)
            .appName("Spark Example")
            .getOrCreate()

        // ... and run!
        driver.run(spark)
    }
}


class Driver(options:Options) {
    private val logger: Logger = LoggerFactory.getLogger(classOf[Driver])

    def run(spark: SparkSession) = {
        // Your application logic here
    }
}
