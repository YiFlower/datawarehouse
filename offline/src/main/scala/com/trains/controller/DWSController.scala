package com.trains.controller

import com.trains.Utils.{LoggerLevel, SparkSessionUtils}

object DWSController extends LoggerLevel {
  def controller(INIT_DATE: String): Unit = {
    val date = INIT_DATE
    val session = SparkSessionUtils.getLocalSparkSession("DWD -> DWS", true)
    // 需要处理的三个表
    val hive_dwd_behavior_tab = "bdw_dwd.dwd_user_behavior_fact_di"
    val hive_dim_user_tab = "bdw_dim.dwd_user_dim_h"
    val hive_dim_item_tab = "bdw_dim.dwd_item_dim_h"
    // 保存到目标表
    val hive_dws_behavior_tab = "bdw_dws.dws_user_behavior_fact_di"

    var result = session.sql(
      s"""
         |INSERT OVERWRITE TABLE ${hive_dws_behavior_tab} PARTITION(partion_month_date='${date}')
         |SELECT a.behavior_date,a.user_id,a.commodity_id,a.behavior_type,
         |b.commodity_category_id,b.commodity_city,c.user_age,c.user_gender,
         |c.user_occupation,c.resident_city
         |FROM ${hive_dwd_behavior_tab} a INNER JOIN
         |${hive_dim_item_tab} b ON a.commodity_id = b.commodity_id INNER JOIN
         |${hive_dim_user_tab} c ON a.user_id = c.user_id
         |WHERE a.partion_month_date = '${date}' ORDER BY a.behavior_date
         |""".stripMargin)

    result.show(20, false)
  }
}