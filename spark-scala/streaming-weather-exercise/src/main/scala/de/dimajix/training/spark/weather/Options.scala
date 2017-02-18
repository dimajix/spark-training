package de.dimajix.training.spark.weather

import scala.collection.JavaConversions._

import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option

/**
  * Created by kaya on 06.12.16.
  */
class Options(args: Seq[String]) {
    @Option(name = "--hostname", usage = "hostname of stream server", metaVar = "<hostname>")
    var streamHostname: String = "quickstart"
    @Option(name = "--port", usage = "port of stream server", metaVar = "<port>")
    var streamPort: Int = 9977
    @Option(name = "--stations", usage = "stations definitioons", metaVar = "<stationsPath>")
    var stationsPath: String = "data/weather/isd"

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
