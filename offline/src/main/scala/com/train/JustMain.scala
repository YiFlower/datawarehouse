package com.train

import com.train.controller.DWDController

object JustMain {
  // ODS -> DWD
  def run1: Unit = {
    DWDController.controller
  }

  // DWD -> DWS
  def run2: Unit = {
    println("DWD -> DWS")
  }

  // DWS -> APP
  def run3: Unit ={
    println("DWS -> APP")
  }

  def main(args: Array[String]): Unit = {
    run1
    run2
  }
}
