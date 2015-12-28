package de.dimajix.training.spark.weather

import scala.collection.JavaConversions._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
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

  def run(sc: SparkContext) = {
    // 1. Load raw weather data from text file

    // 2. Transform raw data into meaningful RDD with WeatherData as object type

    // 3. Load raw station data from text file

    // 4. Transform raw data into meaningful RDD with StationData as object type

    // 5. Create new RDD from weather RDD with station code as key

    // 6. Create new RDD from station RDD with station code as key

    // 7. Join both RDDs

    // 8. Extract country, year as key and weather as value from joined RDD

    // 9. Aggregate results for getting min and max information. Have a look at aggregateByKey in PairRDD

    // 10. Save results as TextFile
  }
}
