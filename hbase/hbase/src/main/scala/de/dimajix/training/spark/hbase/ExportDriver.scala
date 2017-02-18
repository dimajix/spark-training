package de.dimajix.training.spark.hbase

import scala.collection.JavaConversions._

import com.cloudera.spark.hbase.HBaseContext
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
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
object ExportDriver {
  def main(args: Array[String]) : Unit = {
    // First create driver, so can already process arguments
    val driver = new ExportDriver(args)

    // Now create SparkContext (possibly flooding the console with logging information)
    val conf = new SparkConf()
      .setAppName("Spark Attribution")
    val sc = new SparkContext(conf)

    // ... and run!
    driver.run(sc)
  }
}


class ExportDriver(args: Array[String]) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[ExportDriver])

  @Option(name = "--weather", usage = "weather dirs", metaVar = "<weatherDirectory>")
  private var inputPath: String = "data/weather/2005,data/weather/2006,data/weather/2007,data/weather/2008,data/weather/2009,data/weather/2010,data/weather/2011"
  @Option(name = "--stations", usage = "stations definitioons", metaVar = "<stationsPath>")
  private var stationsPath: String = "data/weather/ish-history.csv"
  @Option(name = "--htable", usage = "HBase Table name", metaVar = "<htable>")
  private var tableName: String = "training:weather"

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
    val raw_weather = sc.textFile(inputPath)
    val weather = raw_weather.map(WeatherData.extract)

    val conf = HBaseConfiguration.create()
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"))
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))


    val hbaseContext = new HBaseContext(sc, conf)
    hbaseContext.bulkPut[WeatherData](weather,
      tableName,
      (weather) => {
        val put = new Put(Bytes.toBytes(weather.usaf + weather.wban))
        put.add(Bytes.toBytes("cf"), Bytes.toBytes(weather.date + weather.time), Bytes.toBytes(weather.temperature))
        put
      },
      true)
  }
}
