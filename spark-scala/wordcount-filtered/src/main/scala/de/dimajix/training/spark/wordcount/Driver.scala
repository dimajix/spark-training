package de.dimajix.training.spark.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
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
    val conf = new SparkConf()
      .setAppName("Spark Attribution")
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)

    // ... and run!
    driver.run(sql)
  }
}


class Driver(options:Options) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[Driver])

  def run(sql: SQLContext) = {
    val sc = sql.sparkContext

    // Create a list of stop words
    val stopWords = sc.broadcast(Set("the","a","and","to","of"," "))

    // Create User-Defined-Function for sql
    val isStopWord = udf((word:String) => stopWords.value.contains(word.toLowerCase))

    // Read data from HDFS
    val raw_input = sql.sparkContext.textFile(options.inputPath)
    val schema = StructType(
      StructField("line", StringType, false) :: Nil
    )
    val input = sql.createDataFrame(raw_input.map(line => Row(line)), schema)

    // Perform word count
    input.select(explode(split(col("line")," ")).as("word"))
      .filter(!isStopWord(col("word")))
      .groupBy("word")
      .count
      .orderBy(desc("count"))
      .write.parquet(options.outputPath)
  }
}
