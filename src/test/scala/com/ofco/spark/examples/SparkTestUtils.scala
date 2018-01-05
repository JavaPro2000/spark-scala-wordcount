package com.ofco.spark.examples

object SparkTestUtils {
  def initTestEnv() {
    val user_dir = System.getProperty("user.dir").replace('\\', '/')
    val spark_hive_warehouse_dir = "file:///" + user_dir + "/tmp/spark-hive"

    sys.props.put("hadoop.home.dir", user_dir + "/hadoop")
    sys.props.put("hive.exec.scratchdir", user_dir + "/tmp/hive/scratch")
    sys.props.put("spark.sql.warehouse.dir", spark_hive_warehouse_dir)
    sys.props.put("spark.local.dir", user_dir + "/tmp/spark/scratch")
    sys.props.put("spark.master", "local")
  }
}