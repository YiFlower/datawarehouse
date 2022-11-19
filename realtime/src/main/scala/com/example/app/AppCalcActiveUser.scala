package com.example.app

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object AppCalcActiveUser {

  def calc: Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)


    // Kafka Sources
    tableEnv.executeSql(
      """
        |CREATE TABLE KafkaSourceDwsTable (
        |  `dt` STRING,
        |  `user_id` STRING,
        |  `item_id` STRING,
        |  `behavior` STRING,
        |  `commodity_category_id` STRING,
        |  `commodity_city` STRING,
        |  `user_age` STRING,
        |  `user_gender` STRING,
        |  `user_occupation` STRING,
        |  `resident_city` STRING,
        |  `ts` BIGINT
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'example-dw-dws',
        |  'properties.bootstrap.servers' = 'cdh02:9092,cdh03:9092',
        |  'properties.group.id' = 'flink-group-001',
        |  'scan.startup.mode' = 'earliest-offset',
        |  'format' = 'csv'
        |)
        |""".stripMargin)


    // 活跃会员数
    tableEnv.executeSql(
      """
        |CREATE TABLE mysqlSinkAppActiveUserTable (
        |  `dt` VARCHAR(15) PRIMARY KEY,
        |  `user_name` VARCHAR(20),
        |  PRIMARY KEY (dt) NOT ENFORCED
        |) WITH (
        | 'connector' = 'jdbc',
        | 'url' = 'jdbc:mysql://192.168.1.100:3306/app?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8',
        | 'username' = 'root',
        | 'password' = 'password',
        | 'table-name' = 'active_user_count',
        | 'scan.fetch-size' = '200'
        |)
        |""".stripMargin)



    tableEnv.executeSql(
      """
        |INSERT INTO mysqlSinkAppActiveUserTable
        |SELECT
        |  dt,
        |  user_id AS user_name
        |FROM(
        |  SELECT
        |    dt,
        |    user_id,
        |    MAX(uc) AS ucc
        |  FROM(
        |    SELECT
        |      FROM_UNIXTIME(ts, 'yyyy-MM-dd') AS dt,
        |      user_id,
        |      COUNT(1) AS uc
        |    FROM KafkaSourceDwsTable
        |    WHERE behavior = 'pay'
        |    GROUP BY FROM_UNIXTIME(ts, 'yyyy-MM-dd'),user_id
        |  )GROUP BY dt,user_id
        |)
        |""".stripMargin)

  }


  def main(args: Array[String]): Unit = {
    calc
  }
}
