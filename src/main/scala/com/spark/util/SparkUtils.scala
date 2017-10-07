package com.spark.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtils {
  def init(appName: String): SparkSession = {
    val conf = new SparkConf()
    conf.setAppName(appName)

    val spark: SparkSession = SparkSession
      .builder
      .config(conf)
      .getOrCreate()
    spark
  }
}
