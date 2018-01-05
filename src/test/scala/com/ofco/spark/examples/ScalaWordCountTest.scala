package com.ofco.spark.examples

import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach, FunSuite}

class ScalaWordCountTest extends FunSuite with BeforeAndAfter {

  test("testMain") {
    val input_file = "src/test/resources/data/data.txt"
    val output_dir = "tmp/output"

    ScalaWordCount.main(Array(input_file, output_dir))
  }

  before {
    SparkTestUtils.initTestEnv()
  }

}