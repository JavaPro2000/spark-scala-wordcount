package com.spark.wordcount

import com.spark.util.SparkUtils
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: <input file> <output file>")
      System.exit(1)
    }

    val input: String = args(0)
    val output: String = args(1)

    val spark: SparkSession = SparkUtils.init("Spark Scala Wordcount")

    Processor.process(spark, input, output)

    spark.stop()
  }
}
