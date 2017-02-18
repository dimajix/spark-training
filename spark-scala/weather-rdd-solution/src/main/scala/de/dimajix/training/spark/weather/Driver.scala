package de.dimajix.training.spark.weather

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
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
      .setAppName("Spark Weather Analysis")
    val sc = new SparkContext(conf)

    // ... and run!
    driver.run(sc)
  }
}


class Driver(options:Options) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[Driver])

  def run(sc: SparkContext) = {
    val raw_weather = sc.textFile(options.inputPath)
    val weather = raw_weather.map(WeatherData.extract)

    val ish_raw = sc.textFile(options.stationsPath)
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

    weather_minmax.saveAsTextFile(options.outputPath)
  }
}
