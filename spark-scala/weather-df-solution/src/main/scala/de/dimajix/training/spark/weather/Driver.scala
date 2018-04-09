package de.dimajix.training.spark.weather

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
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
    val spark = SparkSession
        .builder()
        .appName("Spark Weather Analysis")
        .getOrCreate()

    // ... and run!
    driver.run(spark)
  }
}


class Driver(options:Options) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[Driver])

  def run(spark: SparkSession) = {
    // Load Weather data as a text file
    val rawWeather = spark.read.text(options.inputPath)

    // Extract relevant columns from weather data
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

    // Read weather stations as CSV and use existing header as schema information
    val stations = spark.read
        .option("header","true")
        .csv(options.stationsPath)

    // Perform simple analysis
    weather.join(stations, weather("usaf") === stations("usaf") && weather("wban") === stations("wban"))
        .withColumn("year", weather("date").substr(0,4))
        .withColumnRenamed("ctry", "country")
        .groupBy("country", "year")
        .agg(
              min(when(col("air_temperature_qual") === lit(1), col("air_temperature"))).as("temp_min"),
              max(when(col("air_temperature_qual") === lit(1), col("air_temperature"))).as("temp_max"),
              min(when(col("wind_speed_qual") === lit(1), col("wind_speed"))).as("wind_min"),
              max(when(col("wind_speed_qual") === lit(1), col("wind_speed"))).as("wind_max")
        )
        .coalesce(1)
        .write.csv(options.outputPath)
  }
}
