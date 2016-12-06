package de.dimajix.training.spark.weather

import org.apache.spark.sql.SparkSession
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
