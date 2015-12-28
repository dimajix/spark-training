package de.dimajix.training.spark.weather

import scala.collection.JavaConversions._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType
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
    val sql = new SQLContext(sc)

    // ... and run!
    driver.run(sql)
  }
}


class Driver(args: Array[String]) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[Driver])

  @Option(name = "--input", usage = "input dirs", metaVar = "<inputDirectory>")
  private var inputPath: String = "weather/2005,weather/2006,weather/2007,weather/2008,weather/2009,weather/2010,weather/2011"
  @Option(name = "--output", usage = "output dir", metaVar = "<outputDirectory>")
  private var outputPath: String = "weather/minmax"
  @Option(name = "--stations", usage = "stations definitioons", metaVar = "<stationsPath>")
  private var stationsPath: String = "weather/ish"

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

  def run(sql: SQLContext) = {
    // 1. Load raw weather data from text file

    // 2. Transform raw data into meaningful DataFrame. See WeatherData for helper functions

    // 3. Load raw station data from text file

    // 4. Transform raw data into meaningful DataFrame. See StationData for helper functions

    // 5. Join both DataFrames and station code (wban and usaf)

    // 6. Extract year from date

    // 7. Group by country and year

    // 8. Aggregate minimum/maximum values, pay attention to quality!

    // 9. Save as ParquetFile
  }
}
