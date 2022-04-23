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
    if (args.length != 2) {
      println("请输入参数！参数一：需要操作的数仓层级；参数二：操作的分区日期")
      System.exit(1)
    }
    run1
    run2
    run3
  }
}
