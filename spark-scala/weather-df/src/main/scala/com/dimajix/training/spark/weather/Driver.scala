package com.dimajix.training.spark.weather

import org.apache.spark.SparkConf
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
        val conf = new SparkConf()
        val spark = SparkSession.builder()
            .config(conf)
            .appName("Spark Example")
            .getOrCreate()

        // ... and run!
        driver.run(spark)
    }
}


class Driver(options:Options) {
    private val logger: Logger = LoggerFactory.getLogger(classOf[Driver])

    def run(spark: SparkSession) = {
        import org.apache.spark.sql.functions._
        import org.apache.spark.sql.types._

        import spark.implicits._

        val storageLocation = "s3://dimajix-training/data/weather"
        val rawWeatherData = (2003 to 2014) map {i => spark.read.text(s"${storageLocation}/${i}").withColumn("year", lit(i)) } reduce((l,r) => l.union(r))

        val weatherData = rawWeatherData.select(
            col("year"),
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

        val stationData = spark.read
            .option("header","true")
            .csv(s"${storageLocation}/isd-history")

        val joined_weather = (weatherData.join(stationData, weatherData("usaf") === stationData("usaf") && weatherData("wban") === stationData("wban"))
            .select(
                $"year",
                $"wind_speed_qual",
                $"wind_speed",
                $"air_temperature_qual",
                $"air_temperature",
                $"ctry" as "country"
            ))

        val result = joined_weather.groupBy("country", "year")
            .agg(
                min(when(col("wind_speed_qual") === lit(1), col("wind_speed"))) as "min_wind_speed",
                max(when(col("wind_speed_qual") === lit(1), col("wind_speed"))) as "max_wind_speed",
                min(when(col("air_temperature_qual") === lit(1), col("air_temperature"))) as "min_temperature",
                max(when(col("air_temperature_qual") === lit(1), col("air_temperature"))) as "max_temperature"
            )

        result.write.csv("/user/hadoop/weather_results")
    }
}
