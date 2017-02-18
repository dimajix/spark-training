package de.dimajix.training.spark.weather

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
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

        // ... and run!
        driver.run()
    }
}


class Driver(options:Options) {
    private val logger: Logger = LoggerFactory.getLogger(classOf[Driver])

    private def createContext() : StreamingContext = {
        // If you do not see this printed, that means the StreamingContext has been loaded
        // from the new checkpoint
        println("Creating new context")

        // Now create SparkContext (possibly flooding the console with logging information)
        val session = SparkSession.builder()
            .appName("Spark Streaming Weather Analysis")
            .config("spark.default.parallelism", "4")
            .config("spark.streaming.blockInterval", "1000")
            .getOrCreate()
        val sc = session.sparkContext
        val ssc = new StreamingContext(sc, Seconds(1))

        // #1 Load Station data from S3/HDFS into an RDD and collect() it to local machine
        val isd_raw = sc.textFile(options.stationsPath).collect().tail.map(x => StationData.extract(x))

        // #2 Create an appropriate key for joining, this could be usaf+wban. The result should be a pair sequence

        // #3 Convert pair sequence to local map via toMap

        // #4 Put station data map into broadcast variable
        val isd:Broadcast[Map[String,StationData]] = null

        // #5 Creata a User defined Function (udf) for looking up the country from usaf and wban
        val country = udf(() => null)

        // #6 Create text stream from Socket via the socketTextStream method of the StreamingContext
        val stream = ssc.socketTextStream(options.streamHostname, options.streamPort, StorageLevel.MEMORY_ONLY)

        // #7 Extract weather data from stream, this can be done again via WeatherData.extractRow
        val weatherData:DStream[Row] =  null

        // #8 Create a sliding window from the stream with a size of 10 seconds and which will progress every second
        val windowedData:DStream[Row] = null

        // Process every RDD inside the stream
        windowedData.foreachRDD(rdd => {
            // #9 Create a DataFrame from the RDD using the 'createDataFrame' method in the SparkSession 'session'

            // #10 Lookup country from embedded station data.

            // #11 Perform min/max aggregations of temperature and wind speed grouped by year and country
            val result:DataFrame = null

            // #12 Print results
            result.collect()
                .foreach(println)
        })

        // Return StreamingContext
        ssc
    }

    def run() = {
        // ... and run!
        val ssc = createContext()
        ssc.start()
        ssc.awaitTermination()
    }
}
