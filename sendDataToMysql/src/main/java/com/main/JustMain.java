package com.main;

import com.trains.controller.SendData;
import com.utils.MysqlUtils;

public class JustMain {
    public static void run() {
        // 1.1测试MySQL连接
        // MysqlUtils.testConnect();
        // 1.2创建数据库和三个表
        MysqlUtils.execSql("CREATE DATABASE IF NOT EXISTS `fliggy_trip` DEFAULT CHARSET utf8 COLLATE utf8_general_ci;");
        // 用户行为表
        MysqlUtils.execSql("CREATE TABLE IF NOT EXISTS `fliggy_trip`.`user_item_behavior_history`(`user_id` VARCHAR(20)," +
                "`commodity_id` VARCHAR(20),`behavior_type` VARCHAR(10),`time_stamp` INT(10),INDEX(`time_stamp`)) DEFAULT CHARSET=utf8;");
        // 所有的用户基础属性画像
        MysqlUtils.execSql("CREATE TABLE IF NOT EXISTS `fliggy_trip`.`user_profile` (`user_id` VARCHAR(20), user_age VARCHAR(5)," +
                "user_gender VARCHAR(3),`user_occupation` VARCHAR(3),`resident_city` VARCHAR(5), `crowd_tag` VARCHAR(20))" +
                "DEFAULT CHARSET=utf8;");
        // 所有的商品基础属性画像
        MysqlUtils.execSql("CREATE TABLE IF NOT EXISTS `fliggy_trip`.`item_profile` (`commodity_id` VARCHAR(20),`commodity_category_id` VARCHAR(5)," +
                "`commodity_city` VARCHAR(5), `commodity_tag` VARCHAR(10))DEFAULT CHARSET=utf8;");

        // 2.1读取本地文件
        SendData readLocalFile = new SendData();
        // 用户行为表、用户画像表、商品画像表
        String userBehaviorDir = "D:\\DataSet\\user_item_behavior_history.csv";  // user_item_behavior_history.csv
        String userProfileDir = "D:\\DataSet\\user_profile.csv";
        String itemProfileDir = "D:\\DataSet\\item_profile.csv";
//        readLocalFile.readLocalDataFile(userBehaviorDir);
//        readLocalFile.readLocalDataFile(userProfileDir);
//        readLocalFile.readLocalDataFile(itemProfileDir);
        // 2.2 写入到MySQL
        // 为了减少内存压力，在ReadLocalFile类中读取到数据后即写入MySQL

        // 3. 关闭资源
        MysqlUtils.closeResources();
    }

    public static void main(String[] args) {
        run();
    }
}
