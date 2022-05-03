package com.trains

import com.trains.controller.{APPController, DWDController, DWSController}

object JustMain {
  def main(args: Array[String]): Unit = {
    var HRC = ""
    var INIT_DATE = ""
    if (args.length != 2) {
      println("请输入参数！参数一：需要操作的数仓层级；参数二：操作的分区日期")
      println("-----------------例：ods 2019-07---------------------")
      System.exit(1)
    } else {
      HRC = args(0)
      INIT_DATE = args(1)
    }
    // RUN
    if (HRC.toUpperCase() == "ODS") {
      run1(INIT_DATE)
    }
    else if (HRC.toUpperCase() == "DWD") {
      run2(INIT_DATE)
    }
    else if (HRC.toUpperCase() == "DWS") {
      run3(INIT_DATE)
    }
    else {
      println("-----PARAMS ERROR-----")
    }
    //    val INIT_DATE = "2019-07"
    //    run1(INIT_DATE)
    //    run2(INIT_DATE)
    //    run3(INIT_DATE)
  }

  // ODS -> DWD
  def run1(INIT_DATE: String): Unit = {
    DWDController.controller(INIT_DATE)
  }

  // DWD -> DWS
  def run2(INIT_DATE: String): Unit = {
    DWSController.controller(INIT_DATE)
  }

  // DWS -> APP
  def run3(INIT_DATE: String): Unit = {
    APPController.controller(INIT_DATE)
  }
}
