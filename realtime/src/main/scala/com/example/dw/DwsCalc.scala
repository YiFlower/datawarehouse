package com.example.dw

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object DwsCalc {

  def calc: Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)


    // 商品维度表
    tableEnv.executeSql(
      """
        |create table mysqlItemDimTable (
        |  commodity_id string,
        |  commodity_category_id string,
        |  commodity_city string,
        |  commodity_tag string
        |) with (
        | 'connector' = 'jdbc',
        | 'url' = 'jdbc:mysql://192.168.1.100:3306/bdw_dim?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8',
        | 'username' = 'root',
        | 'password' = 'password',
        | 'table-name' = 'item_dim_h',
        | 'scan.fetch-size' = '200'
        |)
        |""".stripMargin)


    // 用户维度表
    tableEnv.executeSql(
      """
        |create table mysqlUserDimTable (
        |  user_id string,
        |  user_age string,
        |  user_gender string,
        |  user_occupation string,
        |  resident_city string,
        |  crowd_tag string
        |) with (
        | 'connector' = 'jdbc',
        | 'url' = 'jdbc:mysql://192.168.1.100:3306/bdw_dim?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8',
        | 'username' = 'root',
        | 'password' = 'password',
        | 'table-name' = 'user_dim_h',
        | 'scan.fetch-size' = '200'
        |)
        |""".stripMargin)


    // ODS -> DWD
    tableEnv.executeSql(
      """
        |CREATE TABLE KafkaTable (
        |  `user_id` BIGINT,
        |  `item_id` BIGINT,
        |  `behavior` STRING,
        |  `ts` BIGINT
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'bigdata-01',
        |  'properties.bootstrap.servers' = 'cdh02:9092,cdh03:9092',
        |  'properties.group.id' = 'flink-group-001',
        |  'scan.startup.mode' = 'earliest-offset',
        |  'format' = 'csv'
        |)
        |""".stripMargin)


    // show
    tableEnv.executeSql(
      """
        |SELECT * FROM mysqlItemDimTable LIMIT 10
        |""".stripMargin).print()

    tableEnv.executeSql(
      """
        |SELECT * FROM mysqlUserDimTable LIMIT 10
        |""".stripMargin).print()

    tableEnv.executeSql(
      """
        |SELECT * FROM KafkaTable LIMIT 10
        |""".stripMargin).print()

  }


  def main(args: Array[String]): Unit = {
    calc
  }
}
