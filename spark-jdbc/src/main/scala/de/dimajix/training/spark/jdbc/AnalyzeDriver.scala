package de.dimajix.training.spark.jdbc

import scala.collection.JavaConversions._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
  * Created by kaya on 03.12.15.
  */
object AnalyzeDriver {
  def main(args: Array[String]) : Unit = {
    // First create driver, so can already process arguments
    val driver = new AnalyzeDriver(args)

    // Now create SparkContext (possibly flooding the console with logging information)
    val conf = new SparkConf()
      .setAppName("Spark Attribution")
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)

    // ... and run!
    driver.run(sql)
  }
}


class AnalyzeDriver(args: Array[String]) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[AnalyzeDriver])

  @Option(name = "--dburi", usage = "JDBC connection", metaVar = "<connection>")
  private var dburi: String = "jdbc:mysql://localhost/training"
  @Option(name = "--dbuser", usage = "JDBC username", metaVar = "<db_user>")
  private var dbuser: String = "cloudera"
  @Option(name = "--dbpass", usage = "JDBC password", metaVar = "<db_password>")
  private var dbpassword: String = "cloudera"
  @Option(name = "--output", usage = "output dir", metaVar = "<outputDirectory>")
  private var outputPath: String = "weather/minmax"

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
    val weather = sql.jdbc(connection, "weather")

    // Load station data
    val ish = sql.jdbc(connection, "ish")

    weather.join(ish, weather("usaf") === ish("usaf") && weather("wban") === ish("wban"))
      .withColumn("year", weather("date").substr(0,4))
      .groupBy("country", "year")
      .agg(
        col("year"),
        col("country"),
        // The following is not supported in Spark 1.3
        // min(when(col("air_temperature_quality") === lit(1), col("air_temperature")).otherwise(9999)).as("temp_min"),
        // max(when(col("air_temperature_quality") === lit(1), col("air_temperature")).otherwise(-9999)).as("temp_max"),
        // min(when(col("wind_speed_quality") === lit(1), col("wind_speed")).otherwise(9999)).as("wind_min"),
        // max(when(col("wind_speed_quality") === lit(1), col("wind_speed")).otherwise(-9999)).as("wind_max")
        min(callUDF((q:Int,t:Double) => if (q == 1) t else 9999.toDouble, DoubleType, col("air_temperature_quality"), col("air_temperature"))).as("temp_min"),
        max(callUDF((q:Int,t:Double) => if (q == 1) t else -9999.toDouble, DoubleType, col("air_temperature_quality"), col("air_temperature"))).as("temp_max"),
        min(callUDF((q:Int,t:Double) => if (q == 1) t else 9999.toDouble, DoubleType, col("wind_speed_quality"), col("wind_speed"))).as("wind_min"),
        max(callUDF((q:Int,t:Double) => if (q == 1) t else -9999.toDouble, DoubleType, col("wind_speed_quality"), col("wind_speed"))).as("wind_max")
      )
      .saveAsParquetFile(outputPath)
  }
}
