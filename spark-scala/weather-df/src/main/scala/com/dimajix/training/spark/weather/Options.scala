package com.dimajix.training.spark.weather

import scala.collection.JavaConversions._

import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option

/**
  * Created by kaya on 06.12.16.
  */
class Options(args: Seq[String]) {
    @Option(name = "--help", usage = "display help", help = true)
    var help:Boolean = false
    @Option(name = "--input", usage = "input table", metaVar = "<inputTable>")
    var inputPath: String = "wordcount.text"
    @Option(name = "--output", usage = "output table", metaVar = "<outputTable>")
    var outputPath: String = "wordcount.word"

    parseArgs(args)

    private def parseArgs(args: Seq[String]) {
        val parser: CmdLineParser = new CmdLineParser(this)
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

        if (help) {
            parser.printUsage(System.out)
            System.exit(0)
        }
    }

}
