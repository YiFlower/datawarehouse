package com.train.controller

import com.train.Utils.{LoggerLevel, SparkSessionUtils}

object DWSController extends LoggerLevel {
  def controller: Unit = {
    val session = SparkSessionUtils.getLocalSparkSession("DWD -> DWS", true)
    // 需要处理的三个表
    val hive_dwd_behavior_tab = "bdw_dwd.user_item_behavior_history"
    val hive_dim_user_tab = "bdw_dim.user_profile"
    val hive_dim_item_tab = "bdw_dim.item_profile"

    var result = session.sql(
      s"""
         |SELECT * FROM ${hive_dwd_behavior_tab} a LEFT JOIN
         |${hive_dim_item_tab} b ON a.commodity_id = b.commodity_id LEFT JOIN
         |${hive_dim_user_tab} c ON a.user_id = c.user_id
         |""".stripMargin)


    result.show(20,false)
  }
}