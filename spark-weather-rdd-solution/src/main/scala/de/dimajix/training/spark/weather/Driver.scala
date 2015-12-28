package de.dimajix.training.spark.weather

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
      .setAppName("Spark Weather Analysis")
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
  private var stationsPath: String = "weather/isd"

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
    val raw_weather = sc.textFile(inputPath)
    val weather = raw_weather.map(WeatherData.extract)

    val ish_raw = sc.textFile(stationsPath)
    val ish_head = ish_raw.first
    val ish = ish_raw
      .filter(_ != ish_head)
      .map(StationData.extract)

    val weather_idx = weather.keyBy(x => x.usaf + x.wban)
    val ish_idx = ish.keyBy(x => x.usaf + x.wban)

    val weather_per_country_and_year = weather_idx
      .join(ish_idx)
      .map(x =>
        (
          (x._2._2.country,x._2._1.date.substring(0,4)),
          x._2._1
          )
      )

    val weather_minmax = weather_per_country_and_year
      .aggregateByKey(WeatherMinMax())((x:WeatherMinMax,y:WeatherData)=>x.reduce(y),(x:WeatherMinMax,y:WeatherMinMax)=>x.reduce(y))

    weather_minmax.saveAsTextFile(outputPath)
  }
}
