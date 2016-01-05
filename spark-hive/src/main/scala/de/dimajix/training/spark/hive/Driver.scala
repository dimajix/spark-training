package de.dimajix.training.spark.hive

import scala.collection.JavaConversions._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
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
      .setAppName("Spark Hive Weather Analysis")
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)

    // ... and run!
    driver.run(sql)
  }
}


class Driver(args: Array[String]) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[Driver])

  @Option(name = "--weather", usage = "input table", metaVar = "<weather_table>")
  private var weatherTable: String = "training.weather"
  @Option(name = "--stations", usage = "stations definitioons", metaVar = "<stations_table>")
  private var stationsTable: String = "training.stations"
  @Option(name = "--output", usage = "output table", metaVar = "<output_table>")
  private var outputPath: String = "training.weather_minmax"

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
    // Load Weather data
    val weather = sql.table(weatherTable)

    // Load station data
    val ish = sql.table(stationsTable)

    weather.join(ish, weather("usaf") === ish("usaf") && weather("wban") === ish("wban"))
        .withColumn("year", weather("date").substr(0,4))
        .groupBy("country", "year")
        .agg(
              col("year"),
              col("country"),
              min(when(col("air_temperature_quality") === lit(1), col("air_temperature")).otherwise(9999)).as("temp_min"),
              max(when(col("air_temperature_quality") === lit(1), col("air_temperature")).otherwise(-9999)).as("temp_max"),
              min(when(col("wind_speed_quality") === lit(1), col("wind_speed")).otherwise(9999)).as("wind_min"),
              max(when(col("wind_speed_quality") === lit(1), col("wind_speed")).otherwise(-9999)).as("wind_max")
        )
        .write.parquet(outputPath)
  }
}
