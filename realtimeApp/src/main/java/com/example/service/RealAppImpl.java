package com.example.service;

import com.example.dao.RealAppMapper;
import com.example.entity.ActiveUserCount;
import com.example.entity.HotCity;
import com.example.entity.NewUserCount;
import com.example.entity.TotalSales;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

@Service
@Repository
public class RealAppImpl implements RealAppService {
    @Autowired
    RealAppMapper realAppMapper;

    @Override
    public NewUserCount getNewUserCount() {
        try {
            return realAppMapper.queryNewUserCount();
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public TotalSales getTotalSales() {
        try {
            return realAppMapper.queryTotalSales();
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public HotCity getHotCity() {
        try {
            return realAppMapper.queryHotCity();
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public ActiveUserCount getActiveUserCount() {
        try {
            return realAppMapper.queryActiveUserCount();
        } catch (Exception e) {
            return null;
        }
    }
}
