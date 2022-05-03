package com.trains.controller

import com.trains.Utils.{LoggerLevel, SparkSessionUtils}

object DWDController extends LoggerLevel {
  val session = SparkSessionUtils.getLocalSparkSession("ODS -> DWD", true)
  // 需要处理的三个表
  val hive_ods_behavior_tab = "bdw_ods.ods_user_behavior_fact_di"
  val hive_ods_user_tab = "bdw_ods.ods_user_dim_h"
  val hive_ods_item_tab = "bdw_ods.ods_item_dim_h"
  // 视图表名
  val user_behavior_view = "user_behavior"
  val user_profile_view = "user_profile"
  val item_profile_view = "item_profile"

  def controller(INIT_DATE: String): Unit = {
    /**
     * 1：ods_user_behavior_fact_di
     * 2：ods_user_dim_h
     * 3：ods_item_dim_h
     * 初始化后，无需运行table2和table3
     * */
    transfor1(INIT_DATE)
    // transfor2
    // transfor3
    SparkSessionUtils.stopSpark(session)
  }

  def transfor1(INIT_DATE: String): Unit = {
    val date = INIT_DATE
    // 1.转换为视图，并探查数据结构
    // tb1：用户行为表
    session.sql(
      s"""
         |SELECT * FROM ${hive_ods_behavior_tab} WHERE partion_month_date = '${date}'
         |""".stripMargin).createOrReplaceTempView(s"${user_behavior_view}")

    var result = session.sql(
      s"""
         |SELECT * FROM ${user_behavior_view}
         |""".stripMargin)

    // 2.空值、重复值处理
    // tb1
    session.sql(
      s"""
         |SELECT DISTINCT * FROM ${user_behavior_view}
         |""".stripMargin).createOrReplaceTempView(s"${user_behavior_view}")

    // 3.异常值处理
    // tb1：无需处理

    // 4.标准化处理（转码）
    /**
     * tb1
     * 1. 格式化日期 yyyy-MM-dd
     * 2. 将用户行为类型转换为中文 - clk->点击、fav->喜欢、cart->加购物车、pay->购买
     * */
    session.sql(
      s"""
         |SELECT SUBSTRING(behavior_time,1,10) AS behavior_date,time_stamp,user_id,commodity_id
         |,CASE behavior_type WHEN 'clk' THEN '点击' WHEN 'fav' THEN '喜欢'
         |WHEN 'cart' THEN '加购物车' WHEN 'pay' THEN '购买' END AS behavior_type
         |,partion_month_date FROM ${user_behavior_view}
         |""".stripMargin).createOrReplaceTempView(s"$user_behavior_view")

    // 5.写入DWD、DIM
    /**
     * tb1写入DWD
     * tb2、tb3写入DIM
     * 方法：直接保存视图即可
     * */
    session.sql(
      s"""
         |INSERT OVERWRITE TABLE bdw_dwd.dwd_user_behavior_fact_di PARTITION(partion_month_date='${date}')
         |SELECT behavior_date,time_stamp,user_id,commodity_id,behavior_type
         |FROM ${user_behavior_view} WHERE partion_month_date='${date}' ORDER BY behavior_date
         |""".stripMargin)

    result.show(20, false)
  }

  def transfor2: Unit = {
    // 1.转换为视图，并探查数据结构
    // tb2：用户维度表
    session.sql(
      s"""
         |SELECT * FROM ${hive_ods_user_tab}
         |""".stripMargin).createOrReplaceTempView(s"${user_profile_view}")

    var result = session.sql(
      s"""
         |SELECT * FROM ${user_profile_view}
         |""".stripMargin)

    // 2.空值、重复值处理
    // tb2
    session.sql(
      s"""
         |SELECT DISTINCT * FROM ${user_profile_view}
         |""".stripMargin).createOrReplaceTempView(s"${user_profile_view}")

    // 3.异常值处理
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

    // 4.标准化处理（转码）
    /**
     * tb2
     * 3. 将用户性别转换为男女 1：未知，2：女，3：男（过滤掉未知）
     * */
    session.sql(
      s"""
         |select user_id,user_age,
         |case user_gender
         |WHEN '2' THEN '男'
         |WHEN '3' THEN '女' END AS user_gender,
         |user_occupation,resident_city,crowd_tag FROM ${user_profile_view} WHERE user_gender != '1'
         |""".stripMargin).createOrReplaceTempView(s"${user_profile_view}")

    // 5.写入DWD、DIM
    /**
     * tb1写入DWD
     * tb2、tb3写入DIM
     * 方法：直接保存视图即可
     * */
    session.sql(
      s"""
         |INSERT INTO TABLE bdw_dim.dwd_user_dim_h
         |SELECT * FROM ${user_profile_view} ORDER BY user_gender
         |""".stripMargin)

    result.show(20, false)
  }


  def transfor3: Unit = {
    // 1.转换为视图，并探查数据结构
    // tb3：商品维度表
    session.sql(
      s"""
         |SELECT * FROM ${hive_ods_item_tab}
         |""".stripMargin).createOrReplaceTempView(s"${item_profile_view}")

    var result = session.sql(
      s"""
         |SELECT * FROM ${item_profile_view}
         |""".stripMargin)

    // 2.空值、重复值处理
    // tb3
    session.sql(
      s"""
         |SELECT DISTINCT * FROM ${item_profile_view}
         |""".stripMargin).createOrReplaceTempView(s"${item_profile_view}")

    // 3.异常值处理
    // tb3：无需处理

    // 4.标准化处理（转码）
    // tb3：无需处理
    // 5.写入DWD、DIM
    /**
     * tb1写入DWD
     * tb2、tb3写入DIM
     * 方法：直接保存视图即可
     * */
    session.sql(
      s"""
         |INSERT INTO TABLE bdw_dim.dwd_item_dim_h
         |SELECT * FROM ${item_profile_view}
         |""".stripMargin)

    result.show(20, false)
  }
}