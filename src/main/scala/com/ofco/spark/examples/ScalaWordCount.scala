package com.ofco.spark.examples

import com.ofco.spark.examples.SparkUtils.{init, tryWithResource}
import com.ofco.spark.examples.Processor.process

object ScalaWordCount {
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