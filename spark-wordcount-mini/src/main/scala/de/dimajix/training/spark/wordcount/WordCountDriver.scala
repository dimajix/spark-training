package de.dimajix.training.spark.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCountDriver {
  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    sc.textFile("alice")
      .flatMap(_.split(" "))
      .filter(_ != "")
      .map(x => (x,1))
      .reduceByKey(_ + _)
      .sortBy(_._2,ascending=false)
      .saveAsTextFile("alice_wordcount")
  }
}
