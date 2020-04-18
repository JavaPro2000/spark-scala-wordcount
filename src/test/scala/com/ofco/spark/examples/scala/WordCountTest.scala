package com.ofco.spark.examples.scala

import org.scalatest.{BeforeAndAfter, FunSuite}

class WordCountTest extends FunSuite with BeforeAndAfter {

  test("testMain") {
    val input_file = "data/data.txt"
    val output_dir = "tmp/output"

    WordCount.main(Array(input_file, output_dir))
  }

  before {
    SparkTestUtils.initTestEnv()
  }

}
