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
    val driver = new NetworkDriver(args)

    // Now create SparkContext (possibly flooding the console with logging information)
    val conf = new SparkConf()
      .setAppName("Spark Streaming Word Count")
    val ssc = new StreamingContext(conf, Seconds(1))

    // ... and run!
    driver.run(ssc)
  }
}


class NetworkDriver(args: Array[String]) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[NetworkDriver])

  @Option(name = "--hostname", usage = "hostname of stream server", metaVar = "<hostname>")
  private var streamHostname: String = "quickstart"
  @Option(name = "--port", usage = "port of stream server", metaVar = "<port>")
  private var streamPort: Int = 9977

  parseArgs(args)

  private def parseArgs(args: Array[String]) {
    val parser: CmdLineParser = new CmdLineParser(this)
    parser.setUsageWidth(80)
    try {
      parser.parseArgument(args.toList)
    }
    catch {
      case e: CmdLineException => {
        System.err.println(e.getMessage)
        parser.printUsage(System.err)
        System.err.println
        System.exit(1)
      }
    }
  }

  def run(ssc: StreamingContext) = {
    val input = ssc.socketTextStream(streamHostname, streamPort)
    input.window(Seconds(10), Seconds(3))
      .flatMap(_.split(" "))
      .filter(_ != "")
      .map(x => (x,1))
      .reduceByKey(_ + _)
      .transform(_.sortBy(_._2, ascending = false))
      .print(20)
    ssc.start()
    ssc.awaitTermination()
  }
}
