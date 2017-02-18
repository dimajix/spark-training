package de.dimajix.training.spark.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
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
    val sql = SparkSession
        .builder()
        .appName("Spark Hive Weather Analyzer")
        .getOrCreate()

    // ... and run!
    driver.run(sql)
  }
}


class Driver(options: Options) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[Driver])

  def run(sql: SparkSession) = {
    // Load Weather data
    val weather = sql.table(options.weatherTable)

    // Load station data
    val ish = sql.table(options.stationsTable)

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
        .write.parquet(options.outputPath)
  }
}
