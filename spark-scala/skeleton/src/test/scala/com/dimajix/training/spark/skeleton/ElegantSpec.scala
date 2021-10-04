package com.dimajix.training.spark.skeleton

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.training.spark.util.LocalSparkSession

/**
  * Created by kaya on 18.02.17.
  */
class ElegantSpec extends AnyFlatSpec with Matchers with LocalSparkSession {
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
