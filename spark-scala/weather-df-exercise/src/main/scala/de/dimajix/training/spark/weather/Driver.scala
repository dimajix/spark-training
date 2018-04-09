package de.dimajix.training.spark.weather

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.types.FloatType
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
        .appName("Spark Weather Analysis")
        .getOrCreate()

    // ... and run!
    driver.run(sql)
  }
}


class Driver(options:Options) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[Driver])

  def run(sql: SparkSession) = {
    // 1. Load raw weather data from text file, as specified in options.inputPath
    val rawWeather = ...

    // 2. Extract relevant columns from weather data
    val weather = rawWeather.select(
      substring(col("value"),5,6) as "usaf",
      substring(col("value"),11,5) as "wban",
      substring(col("value"),16,8) as "date",
      substring(col("value"),24,4) as "time",
      substring(col("value"),42,5) as "report_type",
      substring(col("value"),61,3) as "wind_direction",
      substring(col("value"),64,1) as "wind_direction_qual",
      substring(col("value"),65,1) as "wind_observation",
      substring(col("value"),66,4).cast(FloatType) / lit(10.0) as "wind_speed",
      substring(col("value"),70,1) as "wind_speed_qual",
      substring(col("value"),88,5).cast(FloatType) / lit(10.0) as "air_temperature",
      substring(col("value"),93,1) as "air_temperature_qual"
    )

    // 3. Load station data as CSV file as specified in options.stationsPath

    // 4. Join both DataFrames and station code (wban and usaf)

    // 5. Extract year from date

    // 6. Rename ctry column to country

    // 7. Group by country and year

    // 8. Aggregate minimum/maximum values, pay attention to quality!

    // 9. Coalesca all partitions into a single one

    // 10. Save as CSV file as specified in options.outputPath
  }
}
