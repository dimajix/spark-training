package de.dimajix.training.scala.skeleton

import scala.collection.JavaConversions._

import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option

/**
  * Created by kaya on 03.12.15.
  */
object SkeletonApp {
  def main(args: Array[String]) : Unit = {
    // First create driver, so can already process arguments
    val app = new SkeletonApp(args)

    // ... and run!
    app.run()
  }
}


class SkeletonApp(args: Array[String]) {
  @Option(name = "--input", usage = "input table", metaVar = "<inputTable>")
  private var inputPath: String = "wordcount.text"
  @Option(name = "--output", usage = "output table", metaVar = "<outputTable>")
  private var outputPath: String = "wordcount.word"

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

  def run() = {
    println("Starting...")

    println("Finished!")
  }
}
