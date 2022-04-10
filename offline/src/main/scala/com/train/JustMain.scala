package com.train

import com.train.controller.SparkController

object JustMain {
  // ODS -> DWD
  def run1: Unit = {
    SparkController.hiveProfile
  }

  // DWD -> DWS
  def run2: Unit = {
    println("Hello")
  }

  def main(args: Array[String]): Unit = {
    run1
    run2
  }
}
