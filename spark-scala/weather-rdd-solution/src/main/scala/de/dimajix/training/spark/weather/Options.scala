package de.dimajix.training.spark.weather

import scala.collection.JavaConversions._

import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option

/**
  * Created by kaya on 06.12.16.
  */
class Options(args: Seq[String]) {
    @Option(name = "--input", usage = "input dirs", metaVar = "<inputDirectory>")
    var inputPath: String = "weather/2005,weather/2006,weather/2007,weather/2008,weather/2009,weather/2010,weather/2011"
    @Option(name = "--output", usage = "output dir", metaVar = "<outputDirectory>")
    var outputPath: String = "weather/minmax"
    @Option(name = "--stations", usage = "stations definitioons", metaVar = "<stationsPath>")
    var stationsPath: String = "weather/isd"

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
