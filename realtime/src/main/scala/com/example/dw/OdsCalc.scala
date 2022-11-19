package com.example.dw

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object OdsCalc {

  def calc: Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    // ODS -> DWD
    tableEnv.executeSql(
      """
        |CREATE TABLE KafkaSourceOdsTable (
        |  `user_id` BIGINT,
        |  `item_id` BIGINT,
        |  `behavior` STRING,
        |  `ts` BIGINT
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'example-dw-ods',
        |  'properties.bootstrap.servers' = 'cdh02:9092,cdh03:9092',
        |  'properties.group.id' = 'flink-group-001',
        |  'scan.startup.mode' = 'earliest-offset',
        |  'format' = 'csv'
        |)
        |""".stripMargin)

    tableEnv.executeSql(
      """
        |CREATE TABLE KafkaSinkDwdTable (
        |  `user_id` BIGINT,
        |  `item_id` BIGINT,
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

    // load
    tableEnv.executeSql(
      """
        |INSERT INTO KafkaSinkDwdTable
        |  SELECT
        |    user_id,
        |    item_id,
        |    behavior,
        |    ts,
        |    FROM_UNIXTIME(ts, 'yyyy-MM-dd') AS rq,
        |    FROM_UNIXTIME(ts, 'yyyy-MM-dd HH:mm:ss') AS dt
        |  FROM KafkaSourceOdsTable
        |  WHERE user_id IS NOT NULL
        |  AND item_id IS NOT NULL
        |  AND behavior IS NOT NULL
        |  AND ts IS NOT NULL
        |""".stripMargin)
  }


  def main(args: Array[String]): Unit = {
    calc
  }
}
