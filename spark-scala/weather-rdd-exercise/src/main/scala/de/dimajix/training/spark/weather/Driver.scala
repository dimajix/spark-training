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
    // 1. Load raw weather data from text file

    // 2. Transform raw data into meaningful RDD with WeatherData as object type

    // 3. Load raw station data from text file

    // 4. Transform raw data into meaningful RDD with StationData as object type

    // 5. Create new RDD from weather RDD with station code as key

    // 6. Create new RDD from station RDD with station code as key

    // 7. Join both RDDs

    // 8. Extract country, year as key and weather as value from joined RDD

    // 9. Aggregate results for getting min and max information. Have a look at aggregateByKey in PairRDD

    // 10. Save results as TextFile
  }
}
