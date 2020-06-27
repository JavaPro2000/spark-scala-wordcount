package com.ofco.spark.examples.scala

import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class WordCountTest extends AnyFunSuite with BeforeAndAfter {

  test("testMain") {
    val input_file = "data/data.txt"
    val output_dir = "tmp/output"

    WordCount.main(Array(input_file, output_dir))
  }

  before {
    SparkTestUtils.initTestEnv()
  }

}
