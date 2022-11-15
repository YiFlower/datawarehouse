package com.example.dao;

import com.example.entity.ActiveUserCount;
import com.example.entity.HotCity;
import com.example.entity.NewUserCount;
import com.example.entity.TotalSales;
import org.springframework.stereotype.Component;

@Component
public interface RealAppMapper {
    /**
     * 用户新增数
     */
    NewUserCount queryNewUserCount();

    /**
     * 累计销售表（件数）
     */
    TotalSales queryTotalSales();


    /**
     * 城市热力表
     */
    HotCity queryHotCity();


    /**
     * 活跃会员数
     */
    ActiveUserCount queryActiveUserCount();

}
