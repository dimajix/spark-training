package com.dimajix.training.spark.wordcount

import scala.collection.JavaConversions._

import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option

/**
  * Created by kaya on 06.12.16.
  */
class Options(args: Seq[String]) {
    @Option(name = "--input", usage = "input directory", metaVar = "<inputDirectory>")
    var inputPath: String = "s3://dimajix-training/data/alice"
    @Option(name = "--output", usage = "output directory", metaVar = "<outputDirectory>")
    var outputPath: String = "alice_wordcount"

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
