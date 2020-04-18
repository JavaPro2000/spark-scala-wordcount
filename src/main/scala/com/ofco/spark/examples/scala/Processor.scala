package com.ofco.spark.examples.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Processor {
  def process(spark: SparkSession, input: String, output: String): Unit = {
    val lines: RDD[String] = extract(spark, input)
    val counts: RDD[(String, Long)] = transform(lines)
    load(counts, output)
  }

  def extract(spark: SparkSession, input: String): RDD[String] = {
    spark.sparkContext.textFile(input)
  }

  def transform(lines: RDD[String]): RDD[(String, Long)] = {
    lines.flatMap(line => line.split(' '))
      .map(word => (word, 1l))
      .reduceByKey((a, b) => a + b)
  }

  def load(counts: RDD[(String, Long)], output: String): Unit = {
    HDFSUtils.deleteFolder(output)
    counts.saveAsTextFile(output)
  }
}
