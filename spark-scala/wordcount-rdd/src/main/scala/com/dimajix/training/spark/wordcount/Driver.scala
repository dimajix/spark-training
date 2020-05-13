package com.dimajix.training.spark.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
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
      .setAppName("Spark WordCount")
    val sc = new SparkContext(conf)

    // ... and run!
    driver.run(sc)
  }
}


class Driver(options:Options) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[Driver])

  def run(sc: SparkContext) = {
    val input = sc.textFile(options.inputPath)
    val words = input.flatMap(_.split(" "))
    val wc = words.filter(_ != "")
      .map(x => (x,1))
      .reduceByKey(_ + _)
      .sortBy(_._2,ascending=false)
    wc.saveAsTextFile(options.outputPath)
  }
}
