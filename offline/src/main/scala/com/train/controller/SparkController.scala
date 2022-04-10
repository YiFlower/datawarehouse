package com.train.controller

import com.train.Utils.{SparkSessionUtils, LoggerLevel}

object SparkController extends LoggerLevel{
  def hiveProfile: Unit ={
    val session = SparkSessionUtils.getLocalSparkSession("ODS -> DWD", true)
    session.sql(
      """
        |SELECT * FROM bdw_ods.item_profile LIMIT 3
        |""".stripMargin).show()

    session.sql(
      """
        |SELECT * FROM bdw_ods.user_profile LIMIT 3
        |""".stripMargin).show()

    session.sql(
      """
        |SELECT * FROM bdw_ods.user_item_behavior_history LIMIT 3
        |""".stripMargin).show()

    SparkSessionUtils.stopSpark(session)
  }
}
