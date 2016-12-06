package de.dimajix.training.spark.wordcount

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
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
        .appName("Spark SQL Word Count")
        .getOrCreate()

    // ... and run!
    driver.run(sql)
  }
}


class Driver(options:Options) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[Driver])

  def run(sql: SparkSession) = {
    val raw_input = sql.sparkContext.textFile(options.inputPath)

    sql.udf.register("SPLIT", (s:String,r:String) => s.split(r))

    val schema = StructType(
      StructField("line", StringType, false) :: Nil
    )
    val input = sql.createDataFrame(raw_input.map(line => Row(line)), schema)
    input.createOrReplaceTempView("alice")
    sql.sql(
      """
        |SELECT
        |   txt.word, COUNT(txt.word) as freq
        |FROM (
        |   SELECT
        |     EXPLODE(SPLIT(line,' ')) AS word
        |   FROM alice) as txt
        |WHERE
        |   txt.word <> ''
        |GROUP BY
        |   txt.word
        |ORDER BY
        |   freq DESC
      """.stripMargin)
      //.saveAsParquetFile(outputPath)

    val words = input.explode("line","word") { line:String => line.split(" ") }
    words.createOrReplaceTempView("txt")
    sql.sql(
      """
        |SELECT
        |   txt.word, COUNT(txt.word) as freq
        |WHERE
        |   txt.word <> ''
        |GROUP BY
        |   txt.word
        |ORDER BY
        |   freq DESC
      """.stripMargin)
      .write.parquet(options.outputPath)
  }
}
