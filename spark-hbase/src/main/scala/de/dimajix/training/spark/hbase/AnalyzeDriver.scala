package de.dimajix.training.spark.hbase

import scala.collection.JavaConversions._

import com.cloudera.spark.hbase.HBaseContext
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
  * Created by kaya on 03.12.15.
  */
object AnalyzeDriver {
  def main(args: Array[String]) : Unit = {
    // First create driver, so can already process arguments
    val driver = new AnalyzeDriver(args)

    // Now create SparkContext (possibly flooding the console with logging information)
    val conf = new SparkConf()
      .setAppName("Spark Attribution")
    val sc = new SparkContext(conf)

    // ... and run!
    driver.run(sc)
  }
}


class AnalyzeDriver(args: Array[String]) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[AnalyzeDriver])

  @Option(name = "--htable", usage = "Hbase Table name", metaVar = "<htable>")
  private var tableName: String = "training:weather"
  @Option(name = "--output", usage = "output dir", metaVar = "<outputDirectory>")
  private var outputPath: String = "weather/minmax"

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

  def run(sc: SparkContext) = {
    val conf = HBaseConfiguration.create()
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"))
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))

    var scan = new Scan()
    scan.setCaching(100)

    val hbaseContext = new HBaseContext(sc, conf)

    var getRdd = hbaseContext.hbaseRDD( tableName, scan)
    getRdd.foreach(v => println(Bytes.toString(v._1)))
    getRdd.collect.foreach(v => println(Bytes.toString(v._1)))
  }
}
