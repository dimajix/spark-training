package de.dimajix.training.spark.wordcount

import scala.collection.JavaConversions._

import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser

/**
  * Created by kaya on 06.12.16.
  */
class Options(args: Seq[String]) {
    @org.kohsuke.args4j.Option(name = "--hostname", usage = "hostname of stream server", metaVar = "<hostname>")
    var streamHostname: String = "quickstart"
    @org.kohsuke.args4j.Option(name = "--port", usage = "port of stream server", metaVar = "<port>")
    var streamPort: Int = 9977


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
