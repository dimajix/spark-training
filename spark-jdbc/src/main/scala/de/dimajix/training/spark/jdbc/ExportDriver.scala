package de.dimajix.training.spark.jdbc

import scala.collection.JavaConversions._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
  * Created by kaya on 03.12.15.
  */
object ExportDriver {
  def main(args: Array[String]) : Unit = {
    // First create driver, so can already process arguments
    val driver = new ExportDriver(args)

    // Now create SparkContext (possibly flooding the console with logging information)
    val conf = new SparkConf()
      .setAppName("Spark Attribution")
    val sc = new SparkContext(conf)
    val sql = new  SQLContext(sc)

    // ... and run!
    driver.run(sql)
  }
}


class ExportDriver(args: Array[String]) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[ExportDriver])

  @Option(name = "--weather", usage = "weather dirs", metaVar = "<weatherDirectory>")
  private var inputPath: String = "data/weather/2005,data/weather/2006,data/weather/2007,data/weather/2008,data/weather/2009,data/weather/2010,data/weather/2011"
  @Option(name = "--stations", usage = "stations definitioons", metaVar = "<stationsPath>")
  private var stationsPath: String = "data/weather/ish-history.csv"
  @Option(name = "--dburi", usage = "JDBC connection", metaVar = "<connection>")
  private var dburi: String = "jdbc:mysql://localhost/training"
  @Option(name = "--dbuser", usage = "JDBC username", metaVar = "<db_user>")
  private var dbuser: String = "cloudera"
  @Option(name = "--dbpass", usage = "JDBC password", metaVar = "<db_password>")
  private var dbpassword: String = "cloudera"

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
    val connection = dburi + "?user=" + dbuser + "&password=" + dbpassword

    // Load Weather data
    val raw_weather = sql.sparkContext.textFile(inputPath)
    val weather_rdd = raw_weather.map(WeatherData.extract)
    val weather = sql.createDataFrame(weather_rdd, WeatherData.schema)
    weather.createJDBCTable(connection, "weather", true)

    // Load station data
    val ish_raw = sql.sparkContext.textFile(stationsPath)
    val ish_head = ish_raw.first
    val ish_rdd = ish_raw
      .filter(_ != ish_head)
      .map(StationData.extract)
    val ish = sql.createDataFrame(ish_rdd, StationData.schema)
    ish.createJDBCTable(connection, "ish", true)
  }
}
