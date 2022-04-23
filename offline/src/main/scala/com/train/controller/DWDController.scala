package com.train.controller

import com.train.Utils.{SparkSessionUtils, LoggerLevel}

object DWDController extends LoggerLevel {
  def controller: Unit = {
    val session = SparkSessionUtils.getLocalSparkSession("ODS -> DWD", true)
    // 需要处理的三个表
    val hive_ods_behavior_tab = "bdw_ods.user_item_behavior_history"
    val hive_ods_user_tab = "bdw_ods.user_profile"
    val hive_ods_item_tab = "bdw_ods.item_profile"
    // 视图表名
    val user_behavior_view = "user_behavior"
    val user_profile_view = "user_profile"
    val item_profile_view = "item_profile"
    // 1.转换为视图，并探查数据结构
    // tb1：用户行为表
    session.sql(
      s"""
         |SELECT * FROM ${hive_ods_behavior_tab}
         |""".stripMargin).createOrReplaceTempView(s"${user_behavior_view}")

    var result = session.sql(
      s"""
         |SELECT * FROM ${user_behavior_view}
         |""".stripMargin)

    // tb2：用户维度表
    session.sql(
      s"""
         |SELECT * FROM ${hive_ods_user_tab}
         |""".stripMargin).createOrReplaceTempView(s"${user_profile_view}")

    result = session.sql(
      s"""
         |SELECT * FROM ${user_profile_view}
         |""".stripMargin)

    // tb3：商品维度表
    session.sql(
      s"""
         |SELECT * FROM ${hive_ods_item_tab}
         |""".stripMargin).createOrReplaceTempView(s"${item_profile_view}")

    result = session.sql(
      s"""
         |SELECT * FROM ${item_profile_view}
         |""".stripMargin)

    // 2.空值、重复值处理
    // tb1
    session.sql(
      s"""
         |SELECT DISTINCT * FROM ${user_behavior_view}
         |""".stripMargin).createOrReplaceTempView(s"${user_behavior_view}")

    // tb2
    session.sql(
      s"""
         |SELECT DISTINCT * FROM ${user_profile_view}
         |""".stripMargin).createOrReplaceTempView(s"${user_profile_view}")

    // tb3
    session.sql(
      s"""
         |SELECT DISTINCT * FROM ${item_profile_view}
         |""".stripMargin).createOrReplaceTempView(s"${item_profile_view}")

    // 3.异常值处理
    // tb1：无需处理
    // tb2：对年龄做探查，并清除掉包含异常年龄的数据
    result = session.sql(
      s"""
        |SELECT MAX(CAST(user_age AS INT)) AS max_user_age
        |,MIN(CAST(user_age AS INT)) AS min_user_age
        |,AVG(CAST(user_age AS INT)) AS avg_user_age
        |,PERCENTILE(CAST(user_age AS INT),ARRAY(0.2,0.4,0.6,0.8,1)) AS percentile_user_age
        |FROM ${user_profile_view}
        |""".stripMargin)

    // 过滤掉异常数据
    session.sql(
      s"""
        |SELECT * FROM ${user_profile_view}
        |WHERE user_age >= 16 OR user_age <=85
        |""".stripMargin).createOrReplaceTempView(s"${user_profile_view}")

    // tb3：无需处理

    // 4.标准化处理（转码）
    /**
     * tb1
     * 1. 格式化日期 yyyy-MM-dd
     * 2. 将用户行为类型转换为中文 - clk->点击、fav->喜欢、cart->加购物车、pay->购买
     *
     * tb2、tb3无需操作
     * */
    session.sql(
      s"""
        |SELECT SUBSTRING(behavior_time,1,10) AS behavior_date,time_stamp,user_id,commodity_id
        |,CASE behavior_type WHEN 'clk' THEN '点击' WHEN 'fav' THEN '喜欢'
        |WHEN 'cart' THEN '加购物车' WHEN 'pay' THEN '购买' END AS behavior_type
        |,partion_month_date FROM ${user_behavior_view}
        |""".stripMargin).createOrReplaceTempView(s"$user_profile_view")

    session.sql(
      s"""
        |SELECT * FROM ${user_profile_view}
        |""".stripMargin).show(20,false)

    // 5.写入DWD、DIM
    /**
     * tb1写入DWD
     * tb2、tb3写入DIM
     * 方法：直接保存视图即可
     * */

    result.show(20, false)
    SparkSessionUtils.stopSpark(session)
  }
}
