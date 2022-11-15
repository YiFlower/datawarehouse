package com.example.app

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object AppCalcSalesCount {

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


    // 累计销售表
    tableEnv.executeSql(
      """
        |CREATE TABLE mysqlSinkAppTotalSalesTable (
        |  `dt` VARCHAR(15) PRIMARY KEY,
        |  `total` VARCHAR(20)
        |) WITH (
        | 'connector' = 'jdbc',
        | 'url' = 'jdbc:mysql://192.168.1.100:3306/app?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8',
        | 'username' = 'root',
        | 'password' = 'password',
        | 'table-name' = 'total_sales',
        | 'scan.fetch-size' = '200'
        |)
        |""".stripMargin)


    tableEnv.executeSql(
      """
        |INSERT INTO mysqlSinkAppTotalSalesTable
        |SELECT
        |  FROM_UNIXTIME(ts, 'yyyy-MM-dd') AS dt,
        |  CAST(COUNT(1) AS STRING) AS total
        |FROM KafkaSourceDwsTable
        |WHERE behavior = 'pay'
        |GROUP BY FROM_UNIXTIME(ts, 'yyyy-MM-dd')
        |""".stripMargin)


  }


  def main(args: Array[String]): Unit = {
    calc
  }
}
