package com.dimajix.training.scala.skeleton

import scala.collection.JavaConversions._

import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option

/**
  * Created by kaya on 06.12.16.
  */
class Options(args: Seq[String]) {
    @Option(name = "--input", usage = "input table", metaVar = "<inputTable>")
    private var inputPath: String = "wordcount.text"
    @Option(name = "--output", usage = "output table", metaVar = "<outputTable>")
    private var outputPath: String = "wordcount.word"

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
