package de.dimajix.training.spark.wordcount

import scala.collection.JavaConversions._

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
  * Created by kaya on 03.12.15.
  */
object NetworkDriver {
  def main(args: Array[String]) : Unit = {
    // First create driver, so can already process arguments
    val options = new Options(args)
    val driver = new NetworkDriver(options)

    // Now create SparkContext (possibly flooding the console with logging information)
    val conf = new SparkConf()
      .setAppName("Spark Streaming Word Count")
    val ssc = new StreamingContext(conf, Seconds(1))

    // ... and run!
    driver.run(ssc)
  }
}


class NetworkDriver(options: Options) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[NetworkDriver])

  def run(ssc: StreamingContext) = {
    val input = ssc.socketTextStream(options.streamHostname, options.streamPort)
    val words = input.flatMap(_.split(" "))
    words.filter(_ != "")
      .map(x => (x,1))
      .reduceByKey(_ + _)
      .print()
    ssc.start()
    ssc.awaitTermination()
  }
}
