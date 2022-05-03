package com.trains.controller

import com.trains.Utils.{LoggerLevel, SparkSessionUtils}

import java.util.Properties

object APPController extends LoggerLevel {
  // mysql properties
  val url = "jdbc:mysql://192.168.1.101:3306/show_db?useUnicode=true&characterEncoding=utf8"
  val properties = new Properties()
  properties.setProperty("user", "root")
  properties.setProperty("password", "password")
  properties.setProperty("driverClass", "com.mysql.jdbc.Driver")

  def controller(INIT_DATE: String): Unit = {
    val date = INIT_DATE
    val session = SparkSessionUtils.getLocalSparkSession("DWD -> DWS", true)
    // DWS表
    val hive_dws_behavior_tab = "bdw_dws.dws_user_behavior_fact_di"
    // 建立视图
    val init_dws_behavior_view = "dws_behavior_view1"
    session.sql(
      //  WHERE partion_month_date = '${date}'
      s"""
         |SELECT * FROM ${hive_dws_behavior_tab}
         |""".stripMargin).createOrReplaceTempView(s"${init_dws_behavior_view}")

    // 1. 报表指标
    // 1.1 月增长用户数
    var result1 = session.sql(
      s"""
         |SELECT count(1) AS new_user_count,'${date}' AS count_month FROM
         |(SELECT DISTINCT(user_id) FROM ${init_dws_behavior_view} WHERE partion_month_date = '${date}') a
         |RIGHT JOIN
         |(SELECT DISTINCT(user_id) FROM ${init_dws_behavior_view} WHERE partion_month_date < '${date}') b
         |ON a.user_id = b.user_id WHERE a.user_id IS NULL
         |""".stripMargin)
    // Save
    result1.write.mode("overwrite").jdbc(url, "new_user_month", properties)

    // 1.2 平台用户年龄段
    var result2 = session.sql(
      s"""
         |SELECT COUNT(1) AS total_num,b.user_age_class FROM(
         |SELECT CASE WHEN a.user_age <= '25' THEN '16-25'
         | WHEN a.user_age <= '35' THEN '25-35'
         | WHEN a.user_age <= '45' THEN '35-45'
         | WHEN a.user_age <= '55' THEN '45-55'
         | WHEN a.user_age <= '70' THEN '55-70'
         | WHEN a.user_age <= '85' THEN '70-85'
         |ELSE "未知" END AS user_age_class FROM (
         |SELECT DISTINCT user_id,user_age FROM ${init_dws_behavior_view}
         |WHERE partion_month_date = "${date}" AND user_age >= 16 AND user_age <= 85 AND user_age != "")a)b
         |GROUP BY b.user_age_class ORDER BY b.user_age_class
         |""".stripMargin)
    result2.write.mode("overwrite").jdbc(url, "user_age_class", properties)

    // 1.3 当月业务新开展城市
    var result3 = session.sql(
      s"""
         |SELECT count(1) AS new_city_count,'${date}' AS count_month FROM
         |(SELECT DISTINCT(commodity_city) FROM ${init_dws_behavior_view} WHERE partion_month_date = '${date}') a
         |RIGHT JOIN
         |(SELECT DISTINCT(commodity_city) FROM ${init_dws_behavior_view} WHERE partion_month_date < '${date}') b
         |ON a.commodity_city = b.commodity_city WHERE a.commodity_city IS NULL
         |""".stripMargin)
    result3.write.mode("overwrite").jdbc(url, "new_city_month", properties)

    // 1.4 当月平台每日交易量环比
    var result4 = session.sql(
      s"""
         |SELECT (c.transaction_total - c.last_transaction_total) AS month_add_transaction,
         |ROUND((c.transaction_total - c.last_transaction_total)/IF(c.last_transaction_total = 0,c.transaction_total,c.last_transaction_total),2) AS add_transaction_ratio,c.transaction_date FROM(
         |SELECT b.*,
         |LAG(b.transaction_total,1,0) OVER(PARTITION BY b.date_day ORDER BY b.transaction_date) AS last_transaction_total FROM (
         |SELECT a.*,SUBSTR(a.transaction_date,9) AS date_day FROM(
         |SELECT COUNT(1) AS transaction_total,behavior_date AS transaction_date
         |FROM ${init_dws_behavior_view}
         |WHERE behavior_date < SUBSTR(ADD_MONTHS(CONCAT("${date}","-01"),1),1,7)
         |AND behavior_date >= SUBSTR(ADD_MONTHS(CONCAT("${date}","-01"),-1),1,7)
         | AND behavior_type = "购买"
         |GROUP BY behavior_date ORDER BY behavior_date)a)b)c WHERE SUBSTR(c.transaction_date,1,7) = "${date}"
         |ORDER BY transaction_date
         |""".stripMargin)
    result4.write.mode("overwrite").jdbc(url, "trading_volume_day_on_day", properties)


    // 2. 分析指标
    // 2.1 当月交易总量趋势图
    var result5 = session.sql(
      s"""
         |SELECT COUNT(1) AS transaction_total,behavior_date AS transaction_date
         |FROM ${init_dws_behavior_view}
         |WHERE behavior_type = "购买" AND partion_month_date = "${date}"
         |GROUP BY behavior_date ORDER BY behavior_date
         |""".stripMargin)
    result5.write.mode("overwrite").jdbc(url, "total_transactions_month", properties)

    // 2.2 当月点击&购买转化比
    var result6 = session.sql(
      s"""
         |SELECT ROUND(b.pay_num/a.click_num,3) AS conversion_rate,a.behavior_date FROM(
         |(SELECT behavior_date,COUNT(1) AS click_num FROM ${init_dws_behavior_view}
         |WHERE behavior_type = "点击" AND partion_month_date = "${date}" GROUP BY behavior_date) a
         |INNER JOIN
         |(SELECT behavior_date,COUNT(1) AS pay_num FROM bdw_dws.dws_user_behavior_fact_di
         |WHERE behavior_type = "购买" AND partion_month_date = "${date}" GROUP BY behavior_date) b
         |ON a.behavior_date = b.behavior_date) ORDER BY a.behavior_date
         |""".stripMargin)
    result6.write.mode("overwrite").jdbc(url, "click_conversion", properties)

    // 2.3 当月活跃用户Top10
    var result7 = session.sql(
      s"""
         |SELECT COUNT(1) AS active_num, c.user_id FROM(
         |SELECT b.*,DATE_SUB(b.behavior_date,b.rk1) AS dis FROM(
         |SELECT a.*,ROW_NUMBER() OVER(PARTITION BY a.user_id ORDER BY a.behavior_date) AS rk1 FROM(
         |SELECT DISTINCT user_id,behavior_date
         |FROM ${init_dws_behavior_view} WHERE partion_month_date = "${date}")a)b)c
         |GROUP BY c.user_id,c.dis
         |ORDER BY active_num DESC LIMIT 10
         |""".stripMargin)
    result7.write.mode("overwrite").jdbc(url, "active_user_top", properties)

    // 2.4 当月优质客户Top10
    var result8 = session.sql(
      s"""
         |SELECT COUNT(1) AS pay_num,user_id FROM ${init_dws_behavior_view}
         |WHERE partion_month_date = "${date}" AND behavior_type = "购买"
         |GROUP BY user_id ORDER BY pay_num DESC LIMIT 10
         |""".stripMargin)
    result8.write.mode("overwrite").jdbc(url, "quality_customer_top", properties)

    // 2.5 当月商品热度Top10
    var result9 = session.sql(
      s"""
         |SELECT COUNT(1) AS click_num,commodity_id FROM ${init_dws_behavior_view}
         |WHERE partion_month_date = "${date}" AND behavior_type = "点击"
         |GROUP BY commodity_id ORDER BY click_num DESC LIMIT 10
         |""".stripMargin)
    result9.write.mode("overwrite").jdbc(url, "commodity_heat_top", properties)

    // 2.6 各年龄段最喜爱的商品
    var result10 = session.sql(
      s"""
         |SELECT d.total_num,d.user_age_class,d.commodity_id FROM(
         |SELECT c.*,ROW_NUMBER() OVER(PARTITION BY c.user_age_class ORDER BY c.total_num DESC) AS rk FROM(
         |SELECT b.*,COUNT(1) OVER(PARTITION BY b.user_age_class,b.commodity_id) AS total_num,
         |b.user_age_class FROM(
         |SELECT a.commodity_id,CASE WHEN a.user_age <= '25' THEN '16-25'
         | WHEN a.user_age <= '35' THEN '25-35'
         | WHEN a.user_age <= '45' THEN '35-45'
         | WHEN a.user_age <= '55' THEN '45-55'
         | WHEN a.user_age <= '70' THEN '55-70'
         | WHEN a.user_age <= '85' THEN '70-85'
         |ELSE "未知" END AS user_age_class FROM (
         |SELECT user_id,user_age,commodity_id FROM ${init_dws_behavior_view}
         |WHERE partion_month_date = "${date}" AND behavior_type = "喜欢"
         |AND user_age >= 16 AND user_age <= 85 AND user_age != "")a)b)c)d
         |WHERE rk = 1 ORDER BY user_age_class
         |""".stripMargin)
    result10.write.mode("overwrite").jdbc(url, "age_class_like_products", properties)

    // 2.7 不同性别最喜欢的商品
    var result11 = session.sql(
      s"""
         |SELECT c.like_count,c.user_gender,c.commodity_id FROM(
         |SELECT ROW_NUMBER() OVER(PARTITION BY b.user_gender ORDER BY b.like_count DESC) AS rk,
         |b.user_gender,b.commodity_id,b.like_count FROM(SELECT
         |COUNT(1) OVER(PARTITION BY a.user_gender,a.commodity_id) AS like_count,a.user_gender,a.commodity_id FROM(
         |SELECT user_id,user_gender,behavior_date,commodity_id FROM ${init_dws_behavior_view}
         |WHERE partion_month_date = "${date}" AND behavior_type = "喜欢")a)b)c
         |WHERE c.rk = 1
         |""".stripMargin)
    result11.write.mode("overwrite").jdbc(url, "gender_like_products", properties)

    // result.show(20, false)
    session.stop()
  }
}