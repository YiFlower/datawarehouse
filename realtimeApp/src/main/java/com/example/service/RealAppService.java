package com.example.service;

import com.example.entity.ActiveUserCount;
import com.example.entity.HotCity;
import com.example.entity.NewUserCount;
import com.example.entity.TotalSales;

public interface RealAppService {
    /**
     * 用户新增数
     */
    NewUserCount getNewUserCount();

    /**
     * 累计销售表（件数）
     */
    TotalSales getTotalSales();


    /**
     * 城市热力表
     */
    HotCity getHotCity();


    /**
     * 活跃会员数
     */
    ActiveUserCount getActiveUserCount();

}
