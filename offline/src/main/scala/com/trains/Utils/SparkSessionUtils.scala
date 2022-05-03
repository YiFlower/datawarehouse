package com.trains.Utils

import org.apache.spark.sql.SparkSession

object SparkSessionUtils extends LoggerLevel {
  def getLocalSparkSession(appName: String): SparkSession = {
    SparkSession.builder().master("local[*]").appName(appName).getOrCreate() //
  }

  def getLocalSparkSession(appName: String, support: Boolean): SparkSession = {
    if (support) SparkSession.builder().master("local[*]").appName(appName).enableHiveSupport()
      // 配置Hive地址
      .config("hive.metastore.uris", "thrift://192.168.1.100:9083")
      .getOrCreate()
    else getLocalSparkSession(appName)
  }

  def getLocalSparkSession(appName: String, master: String): SparkSession = {
    SparkSession.builder().master(master).appName(appName).getOrCreate()
  }

  def getLocalSparkSession(appName: String, master: String, support: Boolean): SparkSession = {
    if (support) SparkSession.builder().master(master).appName(appName).enableHiveSupport()
      // 配置Hive地址
      .config("hive.metastore.uris", "thrift://192.168.1.100:9083")
      .getOrCreate()
    else getLocalSparkSession(appName, master)
  }

  def stopSpark(ss: SparkSession) = {
    if (ss != null) {
      ss.stop()
    }
  }
}
