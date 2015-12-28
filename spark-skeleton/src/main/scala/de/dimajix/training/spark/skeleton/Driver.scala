package de.dimajix.training.spark.skeleton

import scala.collection.JavaConversions._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
  * Created by kaya on 03.12.15.
  */
object Driver {
  def main(args: Array[String]) : Unit = {
    // First create driver, so can already process arguments
    val driver = new Driver(args)

    // Now create SparkContext (possibly flooding the console with logging information)
    val conf = new SparkConf()
      .setAppName("Spark Attribution")
    val sc = new SparkContext(conf)

    // ... and run!
    driver.run(sc)
  }
}


class Driver(args: Array[String]) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[Driver])

  @Option(name = "--input", usage = "input table", metaVar = "<inputTable>")
  private var inputPath: String = "wordcount.text"
  @Option(name = "--output", usage = "output table", metaVar = "<outputTable>")
  private var outputPath: String = "wordcount.word"

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

  def run(sc: SparkContext) = {
  }
}
