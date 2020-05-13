package com.dimajix.training.spark.skeleton

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

class SimpleSpec extends FlatSpec with Matchers with BeforeAndAfter {
    var session:SparkSession = _

    // Initialize a SparkContext before any test is performed
    before {
        val master = "local[2]"
        val appName = "spark-unittest"

        session = SparkSession.builder()
            .master(master)
            .appName(appName)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
    }

    // Shutdown SparkContext after all tests have been executed
    after {
        session.stop()
        session = null
    }

    "The SparkSession" should "be valid" in {
        session should not be (null)
    }

    "The WordCount" should "deliver correct results" in {
        val text =
            """
              |This is a text, specifically written for Spark in order
              |to test if a word count algorithm can be tested in a
              |unittest.
              |
              |That would be so great if that worked without the need
              |to install a local Spark cluster
            """.stripMargin

        val sc = session.sparkContext
        val result = sc.parallelize(Seq(text))
            .flatMap(_.split(" "))
            .filter(_ != "")
            .map(word => (word,1))
            .countByKey()

        result.isEmpty should be (false)
        result("a") should be (3)
        result("Spark") should be (2)
        result("if") should be (2)
    }
}
