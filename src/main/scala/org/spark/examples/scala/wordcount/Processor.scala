package org.spark.examples.scala.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.spark.examples.scala.util.HDFSUtils

object Processor {
  def process(spark: SparkSession, input: String, output: String): Unit = {
    val lines: RDD[String] = extract(spark, input)
    val counts: RDD[(String, Long)] = transform(lines)
    load(counts, output)
  }

  def extract(spark: SparkSession, input: String): RDD[String] = {
    val lines: RDD[String] = spark.sparkContext.textFile(input)
    lines
  }

  def transform(lines: RDD[String]): RDD[(String, Long)] = {
    val words: RDD[String] = lines.flatMap(line => line.split(' '))
    val ones: RDD[(String, Long)] = words.map(word => (word, 1))
    val counts: RDD[(String, Long)] = ones.reduceByKey((a, b) => a + b)
    counts
  }

  def load(counts: RDD[(String, Long)], output: String): Unit = {
    HDFSUtils.deleteFolder(output)
    counts.saveAsTextFile(output)
  }
}
