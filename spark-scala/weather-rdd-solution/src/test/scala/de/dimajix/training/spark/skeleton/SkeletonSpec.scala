package de.dimajix.training.spark.skeleton

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

class SkeletonSpec extends FlatSpec with Matchers with BeforeAndAfter {
  var sc:SparkContext = _

  before {
    val master = "local[2]"
    val appName = "attribution-unittest"

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    sc = new SparkContext(conf)
  }
  after {
    sc.stop()
    sc = null
  }

  "The SparkContext" should "be valid" in {
    sc should not be (null)
  }
}
