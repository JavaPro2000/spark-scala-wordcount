package com.ofco.spark.examples.scala

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object HDFSUtils {
  def deleteFolder(folder: String): Unit = {
    val path = new Path(folder)
    deleteFolder(path)
  }

  def deleteFolder(folder: String, conf: Configuration): Unit = {
    val path = new Path(folder)
    deleteFolder(path, conf)
  }

  def deleteFolder(folder: String, fs: FileSystem): Unit = {
    val path = new Path(folder)
    deleteFolder(path, fs)
  }

  def deleteFolder(path: Path): Unit = {
    deleteFolder(path, new Configuration())
  }

  def deleteFolder(path: Path, conf: Configuration): Unit = {
    val fs = FileSystem.get(conf)
    deleteFolder(path, fs)
  }

  def deleteFolder(path: Path, fs: FileSystem): Unit = {
    if (fs.exists(path)) fs.delete(path, true)
  }
}
