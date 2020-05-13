package com.dimajix.training.scala.skeleton

/**
  * Created by kaya on 03.12.15.
  */
object SkeletonApp {
  def main(args: Array[String]) : Unit = {
    // First create driver, so can already process arguments
    val options = new Options(args)
    val app = new SkeletonApp(options)

    // ... and run!
    app.run()
  }
}


class SkeletonApp(options: Options) {
  def run() = {
    println("Starting...")

    println("Finished!")
  }
}
