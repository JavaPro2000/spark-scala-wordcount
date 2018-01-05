package com.spark

/**
 * @author Oswaldo Fuenmayor
 */
object Main {

  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)

  def main(args: Array[String]) {
    println("Hello World!")
    println("concat arguments = " + foo(args))
  }

}
