package com.example.dw

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object DwdCalc {

  def calc: Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)


    // 商品维度表
    tableEnv.executeSql(
      """
        |create table mysqlItemDimTable (
        |  commodity_id STRING,
        |  commodity_category_id STRING,
        |  commodity_city STRING,
        |  commodity_tag STRING
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
        |  user_id STRING,
        |  user_age STRING,
        |  user_gender STRING,
        |  user_occupation STRING,
        |  resident_city STRING,
        |  crowd_tag STRING
        |) with (
        | 'connector' = 'jdbc',
        | 'url' = 'jdbc:mysql://192.168.1.100:3306/bdw_dim?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8',
        | 'username' = 'root',
        | 'password' = 'password',
        | 'table-name' = 'user_dim_h',
        | 'scan.fetch-size' = '200'
        |)
        |""".stripMargin)


    // DWD
    tableEnv.executeSql(
      """
        |CREATE TABLE KafkaSourceDwdTable (
        |  `user_id` STRING,
        |  `item_id` STRING,
        |  `behavior` STRING,
        |  `ts` BIGINT,
        |  `rq` STRING,
        |  `dt` STRING
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'example-dw-dwd',
        |  'properties.bootstrap.servers' = 'cdh02:9092,cdh03:9092',
        |  'properties.group.id' = 'flink-group-001',
        |  'scan.startup.mode' = 'earliest-offset',
        |  'format' = 'csv'
        |)
        |""".stripMargin)


    tableEnv.executeSql(
      """
        |CREATE TABLE KafkaSinkDwsTable (
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


    // DWD -> DWS
    tableEnv.executeSql(
      """
        |INSERT INTO KafkaSinkDwsTable
        |  SELECT
        |    a.dt,
        |    a.user_id,
        |    a.item_id,
        |    a.behavior,
        |    b.commodity_category_id,
        |    b.commodity_city,
        |    c.user_age,
        |    c.user_gender,
        |    c.user_occupation,
        |    c.resident_city,
        |    a.ts
        |  FROM KafkaSourceDwdTable a
        |  INNER JOIN mysqlItemDimTable b ON a.item_id = b.commodity_id
        |  INNER JOIN mysqlUserDimTable c ON a.user_id = c.user_id
        |""".stripMargin)

  }


  def main(args: Array[String]): Unit = {
    calc
  }
}
