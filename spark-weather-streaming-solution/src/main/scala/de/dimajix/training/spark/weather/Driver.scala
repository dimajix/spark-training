package de.dimajix.training.spark.weather

import scala.collection.JavaConversions._

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
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

    // ... and run!
    driver.run()
  }
}


class Driver(args: Array[String]) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[Driver])

  @Option(name = "--hostname", usage = "hostname of stream server", metaVar = "<hostname>")
  private var streamHostname: String = "quickstart"
  @Option(name = "--port", usage = "port of stream server", metaVar = "<port>")
  private var streamPort: Int = 9977
  @Option(name = "--stations", usage = "stations definitioons", metaVar = "<stationsPath>")
  private var stationsPath: String = "data/weather/isd"

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

  private def createContext() : StreamingContext = {
    // If you do not see this printed, that means the StreamingContext has been loaded
    // from the new checkpoint
    println("Creating new context")

    // Now create SparkContext (possibly flooding the console with logging information)
    val conf = new SparkConf()
      .setAppName("Spark Streaming Weather Analysis")
      .set("spark.default.parallelism", "4")
      .set("spark.streaming.blockInterval", "1000")
    val ssc = new StreamingContext(conf, Seconds(1))
    val sc = ssc.sparkContext

    // #1 Load Station data into an appropriate map. Key could be usaf+wban
    val isd_raw = sc.textFile(stationsPath).collect()
    val isd_map = isd_raw
      .drop(1)
      .map(StationData.extract)
      .map(data => (data.usaf + data.wban, data))
      .toMap

    // #2 Put station data map into broadcast variable
    val isd = sc.broadcast(isd_map)

    // #3 Create text stream from Socket
    val stream = ssc.socketTextStream(streamHostname, streamPort, StorageLevel.MEMORY_ONLY)

    // User defined function for looking up the country from usaf and wban
    val country = udf((usaf:String,wban:String) => isd.value.get(usaf + wban).map(_.country).getOrElse("none"))

    // #4 Extract weather data from stream
    val weatherData = stream.map[Row](WeatherData.extract)
      .window(Seconds(10), Seconds(1))
      .foreachRDD(rdd => {
        val sql = SQLContext.getOrCreate(rdd.sparkContext)
        val weather = sql.createDataFrame(rdd, WeatherData.schema)
        // #5 Lookup country from embedded station data.
        // #6 Perform min/max aggregations of temperature and wind speed grouped by year and country
        weather.withColumn("country", country(weather("usaf"), weather("wban")))
          .withColumn("year", weather("date").substr(0,4))
          .groupBy("country", "year")
          .agg(
            min(when(col("air_temperature_quality") === lit(1), col("air_temperature")).otherwise(9999)).as("temp_min"),
            max(when(col("air_temperature_quality") === lit(1), col("air_temperature")).otherwise(-9999)).as("temp_max"),
            min(when(col("wind_speed_quality") === lit(1), col("wind_speed")).otherwise(9999)).as("wind_min"),
            max(when(col("wind_speed_quality") === lit(1), col("wind_speed")).otherwise(-9999)).as("wind_max")
          )
          // #7 Print results
          .collect()
          .foreach(println)
      })

    ssc
  }

  def run() = {
    // ... and run!
    val ssc = createContext()
    ssc.start()
    ssc.awaitTermination()
  }
}
