package com.trains.controller;

import com.utils.MysqlUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class SendData {
    // 映射Map，通过传入的文件名，获取数据库里面的表名
    private static HashMap<String,String> tbNameMap = new HashMap<String,String>();
    static {
        tbNameMap.put("user_item_behavior_history.csv","fliggy_trip.user_item_behavior_history");
        tbNameMap.put("user_profile.csv","fliggy_trip.user_profile");
        tbNameMap.put("item_profile.csv","fliggy_trip.item_profile");
    }


    // 1. 读取本地文件
    public ArrayList<String> readLocalDataFile(String fileName) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(
                    new FileReader(fileName));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        // 切割出路径，获取表名
        String tbName = fileName.split("\\\\")[2];
        tbName = tbNameMap.get(tbName);

        // 临时接收line
        String tmpLineStr = "";
        // 存储到数组
        ArrayList<String> dataArr = new ArrayList<>();
        try {
            while ((tmpLineStr = br.readLine()) != null) {
                dataArr.add(tmpLineStr);
                if (dataArr.size() % 1000 == 0) { // 每1000个值，插入一次MySQL
                    sendData(tbName,dataArr);  // 写入MySQL
                    dataArr.clear(); // 重置
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataArr;
    }

    // 2. 写入MySQL
    public void sendData(String tbName, ArrayList<String> dataArr) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < dataArr.size(); i++) {
            sb = sb.append("('").append(dataArr.get(i).replace(",", "','")).append("'),");
        }
        // 去掉sb最后一个逗号
        sb = sb.deleteCharAt(sb.length() - 1);
        try {
            MysqlUtils.execSql("INSERT INTO " + tbName + " VALUES" + sb + ";");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
