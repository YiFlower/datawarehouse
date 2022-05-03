-----------------------------
-- function:CREATE TABLE
-- sources: -- 
-- target: ODS、DWD、DWS、APP
-- author:Yih
-- lastdate:2022-05-02
-----------------------------
-- ODS
set hive.exec.dynamic.partition=true; 
set hive.exec.dynamic.partition.mode=nonstrict;
CREATE TABLE IF NOT EXISTS bdw_ods.ods_user_behavior_fact_di(
`behavior_time` STRING,
`time_stamp` STRING,
`user_id` STRING,
`commodity_id` STRING,
`behavior_type` STRING
) partitioned by(`partion_month_date` STRING) 
row format delimited fields terminated by ','
STORED AS TEXTFILE
location '/user/hive/warehouse/bdw_ods.db/ods_user_behavior_fact_di/';

create table bdw_ods.ods_item_dim_h (
`commodity_id` STRING,
`commodity_category_id` STRING,
`commodity_city` STRING,
`commodity_tag` STRING
)
row format delimited fields terminated by ','
STORED AS TEXTFILE
location '/user/hive/warehouse/bdw_ods.db/ods_item_dim_h/';

create table bdw_ods.ods_user_dim_h (
`user_id` STRING,
`user_age` STRING,
`user_gender` STRING,
`user_occupation` STRING,
`resident_city` STRING,
`crowd_tag` STRING
)row format delimited fields terminated by ','
STORED AS TEXTFILE
location '/user/hive/warehouse/bdw_ods.db/ods_user_dim_h/';

-- item_dim
sqoop import \
--connect jdbc:mysql://192.168.1.101:3306/fliggy_trip \
--username root \
--delete-target-dir \
--password password \
--query "SELECT * FROM item_profile WHERE \$CONDITIONS" \
--direct \
-m 1 \
--split-by commodity_id \
--as-textfile \
--target-dir /user/sqoop/ods_item_dim_h

-- user_dim
sqoop import \
"-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect jdbc:mysql://192.168.1.101:3306/fliggy_trip \
--username root \
--delete-target-dir \
--password password \
--query "SELECT * FROM user_profile WHERE \$CONDITIONS" \
--direct \
-m 4 \
--split-by user_id \
--as-textfile \
--target-dir /user/sqoop/ods_user_dim_h
	
-- load
hive -e "load data inpath '/user/sqoop/ods_user_dim_h' into table bdw_ods.ods_user_dim_h;"
hive -e "load data inpath '/user/sqoop/ods_item_dim_h' into table bdw_ods.ods_item_dim_h;"

-- DWD
set hive.exec.dynamic.partition=true; 
set hive.exec.dynamic.partition.mode=nonstrict;
CREATE TABLE IF NOT EXISTS bdw_dwd.dwd_user_behavior_fact_di(
`behavior_date` STRING,
`time_stamp` STRING,
`user_id` STRING,
`commodity_id` STRING,
`behavior_type` STRING
) partitioned by(`partion_month_date` STRING) 
row format delimited fields terminated by ','
STORED AS TEXTFILE
location '/user/hive/warehouse/bdw_dwd.db/dwd_user_behavior_fact_di/';

-- DIM
CREATE TABLE IF NOT EXISTS bdw_dim.dwd_item_dim_h (
`commodity_id` STRING,
`commodity_category_id` STRING,
`commodity_city` STRING,
`commodity_tag` STRING
)
row format delimited fields terminated by ','
STORED AS TEXTFILE
location '/user/hive/warehouse/bdw_dim.db/dwd_item_dim_h/';

CREATE TABLE IF NOT EXISTS bdw_dim.dwd_user_dim_h (
`user_id` STRING,
`user_age` STRING,
`user_gender` STRING,
`user_occupation` STRING,
`resident_city` STRING,
`crowd_tag` STRING
)row format delimited fields terminated by ','
STORED AS TEXTFILE
location '/user/hive/warehouse/bdw_dim.db/dwd_user_dim_h/';

-- DWS
set hive.exec.dynamic.partition=true; 
set hive.exec.dynamic.partition.mode=nonstrict;
CREATE TABLE IF NOT EXISTS bdw_dws.dws_user_behavior_fact_di(
`behavior_date` STRING,
`user_id` STRING,
`commodity_id` STRING,
`behavior_type` STRING,
`commodity_category_id` STRING,
`commodity_city` STRING,
`user_age` STRING,
`user_gender` STRING,
`user_occupation` STRING,
`resident_city` STRING
) partitioned by(`partion_month_date` STRING) 
row format delimited fields terminated by ','
STORED AS TEXTFILE
location '/user/hive/warehouse/bdw_dws.db/dws_user_behavior_fact_di/';

-- APP
CREATE DATABASE show_db DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;
-- 报表指标
-- 月增长用户数
create table new_user_month(new_user_count INT,count_month varchar(15));
-- 平台用户年龄段
create table user_age_class(total_num INT,user_age_class varchar(15));
-- 当月业务新开展城市
create table new_city_month(new_user_count INT,count_month varchar(15));
-- 当月平台每日交易量环比
create table trading_volume_day_on_day(month_add_transaction INT,transaction_date varchar(15));


-- 分析指标
-- 当月交易总量趋势
create table total_transactions_month(transaction_total INT,transaction_date varchar(15));
-- 当月点击&购买转化比
create table click_conversion(conversion INT,calc_month varchar(15));
-- 各年龄段最喜爱的商品
create table age_class_like_products(like_count INT,user_age_class varchar(15),commodity_id varchar(15));
-- 不同性别最喜欢的商品
create table gender_like_products(like_count INT,user_gender varchar(15),commodity_id varchar(15)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
-- 当月活跃用户Top10
create table active_user_top(active_num INT,user_id varchar(15));
-- 当月优质客户Top10
create table quality_customer_top(pay_num INT,user_id varchar(15));
-- 当月商品热度Top10
create table commodity_heat_top(pay_num INT,user_id varchar(15));