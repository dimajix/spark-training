package de.dimajix.training.spark.wordcount

import scala.collection.JavaConversions._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
  * Created by kaya on 03.12.15.
  */
object WordCountDriver {
  def main(args: Array[String]) : Unit = {
    // First create driver, so can already process arguments
    val driver = new WordCountDriver(args)

    // Now create SparkContext (possibly flooding the console with logging information)
    val conf = new SparkConf()
      .setAppName("Spark Attribution")
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)

    // ... and run!
    driver.run(sql)
  }
}


class WordCountDriver(args: Array[String]) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[WordCountDriver])

  @Option(name = "--input", usage = "input directory", metaVar = "<inputDirectory>")
  private var inputPath: String = "alice"
  @Option(name = "--output", usage = "output directory", metaVar = "<outputDirectory>")
  private var outputPath: String = "alice_wordcount"

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

  def run(sql: SQLContext) = {
    val raw_input = sql.sparkContext.textFile(inputPath)

    sql.udf.register("SPLIT", (s:String,r:String) => s.split(r))

    val schema = StructType(
      StructField("line", StringType, false) :: Nil
    )
    val input = sql.createDataFrame(raw_input.map(line => Row(line)), schema)
    input.registerTempTable("alice")
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
    words.registerTempTable("txt")
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
      .saveAsParquetFile(outputPath)
  }
}
