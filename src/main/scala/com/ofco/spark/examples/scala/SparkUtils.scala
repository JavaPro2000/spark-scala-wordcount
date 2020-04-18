package com.ofco.spark.examples.scala

import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object SparkUtils {
  def init(appName: String): SparkSession = {
    SparkSession.builder
      .appName(appName)
      .getOrCreate()
  }

  def tryWithResource[A <: AutoCloseable, B](resource: A)(block: A => B): B = {
    Try(block(resource)) match {
      case Success(result) =>
        resource.close()
        result
      case Failure(e) =>
        resource.close()
        throw e
    }
  }
}
