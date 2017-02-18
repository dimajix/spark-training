package de.dimajix.training.hbase

import scala.collection.JavaConversions._

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.util.Bytes
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option
import org.slf4j.Logger
import org.slf4j.LoggerFactory


object Example {
  def main(args: Array[String]) : Unit = {
    val driver = new Example(args)

    driver.run()
  }
}


class Example(args: Array[String])  {
  private val logger: Logger = LoggerFactory.getLogger(classOf[Example])

  @Option(name = "--table", usage = "HBase table", metaVar = "<table>")
  private var tableName: String = "wordcount.text"
  @Option(name = "--family", usage = "HBase column family", metaVar = "<family>")
  private var familyName: String = "f"

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
    val conf = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf(tableName))

    runPutGet(table)
    runScan(table)

    table.close()
    connection.close()
  }

  def runPutGet(table:Table) = {
    val family = Bytes.toBytes(familyName)
    val row = Bytes.toBytes("myLittleRow")
    val column = Bytes.toBytes("someQualifier")
    val value = Bytes.toBytes("Some Value")

    val put = new Put(row)
    put.addColumn(family, column, value)
    table.put(put)

    val get = new Get(row)
    val result = table.get(get)
  }

  def runScan(table:Table) = {
    val family = Bytes.toBytes(familyName)
    val column = Bytes.toBytes("someQualifier")

    val scan = new Scan()
    scan.addColumn(family, column)
    val scanner = table.getScanner(scan)
    for (rr <- scanner)
    {
      System.out.println("Found row: " + rr)
    }

    scanner.close()
  }
}
