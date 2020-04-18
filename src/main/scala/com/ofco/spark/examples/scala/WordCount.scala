package com.ofco.spark.examples.scala

import com.ofco.spark.examples.scala.Processor.process
import com.ofco.spark.examples.scala.SparkUtils.{init, tryWithResource}

object WordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: <input file> <output file>")
      System.exit(1)
    }

    val input: String = args(0)
    val output: String = args(1)

    tryWithResource(init("Spark Scala WordCount")) {
      spark => process(spark, input, output)
    }
  }
}
