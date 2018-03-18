package com.ofco.spark.examples

import org.apache.spark.sql.SparkSession

object SparkUtils {
  def init(appName: String): SparkSession = {
    SparkSession.builder
      .appName(appName)
      .getOrCreate()
  }
}