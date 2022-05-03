#!/bin/bash
-----------------------------
-- function:Synchronize dimension tables


-- sources1:mysql,fliggy_trip.user_item_behavior_history
-- target1:bdw_ods.ods_user_behavior_fact_di
-- author:Yih
-- lastdate:2022-05-02
-----------------------------
TIME=$(date "+%Y-%m-%d %H:%M:%S")
# MySQL数据时间范围： 2019-06-03 -> 2021-06-03
# 读取用户输入导入数据范围参数，如2019-06，表示导入一个月数据量
EXPORT_DATE=$1
if [ ! -n "$1" ]
  then
        echo "---参数为空，请输入导入数据日期。格式：YYYY-MM---"
        exit
fi
echo "---application start,run time is $TIME---"
echo "---export data offset(month):$EXPORT_DATE---"

# sqoop bash
(sqoop import \
--connect jdbc:mysql://192.168.1.101:3306/fliggy_trip \
--username root \
--delete-target-dir \
--password password \
--query "SELECT time AS behavior_time,time_stamp,user_id,commodity_id,behavior_type, date AS behavior_date FROM (SELECT *,FROM_UNIXTIME(time_stamp,'%Y-%m-%d %H:%i:%S') AS time,FROM_UNIXTIME(time_stamp,'%Y-%m') AS date FROM user_item_behavior_history) a WHERE a.date =\"$EXPORT_DATE\" AND \$CONDITIONS" \
--direct \
-m 4 \
--split-by time_stamp \
--as-textfile \
--target-dir /user/sqoop/ods_user_behavior_fact_di/$EXPORT_DATE)
# 使用HQL从HDFS LOAD到Hive表 -S：静音模式，不输出MR log
if [ $? -eq 0 ]
then
        echo "---sqoop shell is succeed---"
        `hive -e "load data inpath '/user/sqoop/ods_user_behavior_fact_di/$EXPORT_DATE' overwrite into table bdw_ods.ods_user_behavior_fact_di  partition(partion_month_date = '$EXPORT_DATE');"`
else
        echo "load data is faild"
fi
echo "---application end---"
