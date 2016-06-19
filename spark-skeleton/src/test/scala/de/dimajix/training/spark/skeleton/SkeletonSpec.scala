package de.dimajix.training.spark.skeleton

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

class SkeletonSpec extends FlatSpec with Matchers with BeforeAndAfter {
    var sc:SparkContext = _

    // Initialize a SparkContext before any test is performed
    before {
        val master = "local[2]"
        val appName = "spark-unittest"

        val conf = new SparkConf()
            .setMaster(master)
            .setAppName(appName)
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        sc = new SparkContext(conf)
    }

    // Shutdown SparkContext after all tests have been executed
    after {
        sc.stop()
        sc = null
    }

    "The SparkContext" should "be valid" in {
        sc should not be (null)
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
