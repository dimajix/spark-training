package de.dimajix.training.spark.wordcount

import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
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
    val raw_input = sql.sparkContext.textFile(options.inputPath)

    val schema = StructType(
      StructField("line", StringType, false) :: Nil
    )
    val input = sql.createDataFrame(raw_input.map(line => Row(line)), schema)
    val words = input.select(explode(split(col("line")," ")).as("word"))
    val wc = words.groupBy(words("word")).count.orderBy(desc("count"))
    wc.write.parquet(options.outputPath)
  }
}
