package com.ofco.spark.examples

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


object HDFSUtils {
  def deleteFolder(folder: String): Unit = {
    val fs = FileSystem.get(new Configuration())
    val path = new Path(folder)
    if (fs.exists(path)) fs.delete(path, true)
  }
}
