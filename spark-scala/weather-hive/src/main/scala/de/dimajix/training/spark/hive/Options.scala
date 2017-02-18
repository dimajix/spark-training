package de.dimajix.training.spark.hive

import scala.collection.JavaConversions._

import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option

/**
  * Created by kaya on 06.12.16.
  */
class Options(args: Seq[String]) {
    @Option(name = "--weather", usage = "input table", metaVar = "<weather_table>")
    var weatherTable: String = "training.weather"
    @Option(name = "--stations", usage = "stations definitioons", metaVar = "<stations_table>")
    var stationsTable: String = "training.stations"
    @Option(name = "--output", usage = "output table", metaVar = "<output_table>")
    var outputPath: String = "training.weather_minmax"

    parseArgs(args)

    private def parseArgs(args: Seq[String]) {
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

}
