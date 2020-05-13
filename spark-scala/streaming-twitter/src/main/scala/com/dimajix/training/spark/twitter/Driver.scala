package com.dimajix.training.spark.twitter

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
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
        import spark.implicits._

        val lines = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", options.kafkaBootstrapServer)
            .option("subscribe", options.inputTopic)
            .option("startingOffsets", "latest")
            .load()

        val ts_text = lines.select(
            $"timestamp",
            get_json_object($"value".cast("string"), "$.text").as("text")
        )

        val topics = ts_text.select(
                ts_text("timestamp"),
                explode(split(ts_text("text")," ")).as("topic")
            )
            .filter($"topic".startsWith("#"))
            .filter($"topic" =!= "#")

        val windowedCounts = topics
            .withWatermark("timestamp", "10 seconds")
            .groupBy(window($"timestamp", "5 seconds", "1 seconds"), $"topic")
            .agg(sum(lit(1)).as("count"))

        val output = windowedCounts.select(
            to_json(
                struct(
                    $"window",
                    $"topic",
                    $"count"
                )
            ) as "value"
        )

        val query = output
            .writeStream
            .option("checkpointLocation", options.checkpointPath)
            .outputMode("update")
            .format("kafka")
            .option("kafka.bootstrap.servers", options.kafkaBootstrapServer)
            .option("topic", options.outputTopic)
            .trigger(Trigger.ProcessingTime("1 seconds"))
            .queryName("topic_counts")
            .start()

        query.awaitTermination()
    }
}
