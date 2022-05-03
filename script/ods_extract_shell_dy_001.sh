-----------------------------
-- function:Synchronize dimension tables


-- sources1:mysql,fliggy_trip.item_profile
-- sources2:mysql,fliggy_trip.user_profile
-- target1:bdw_ods.ods_item_dim_h
-- target2:bdw_ods.ods_user_dim_h
-- author:Yih
-- lastdate:2022-05-02
-----------------------------
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