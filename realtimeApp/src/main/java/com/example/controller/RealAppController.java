package com.example.controller;

import com.example.entity.ActiveUserCount;
import com.example.entity.HotCity;
import com.example.entity.NewUserCount;
import com.example.entity.TotalSales;
import com.example.service.RealAppImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@Slf4j
@CrossOrigin
@RestController
@RequestMapping("/api/v1/realtime")
public class RealAppController {
    @Autowired
    RealAppImpl realApp;

    @GetMapping("/getNewUserCount")
    public Object getNewUserCount() {
        NewUserCount newUserCount = realApp.getNewUserCount();
        if (newUserCount == null) {
            return 0;
        }
        return newUserCount.getAddUser();
    }

    @GetMapping("/getTotalSales")
    public Object getTotalSales() {
        TotalSales totalSales = realApp.getTotalSales();
        if (totalSales == null) {
            return 0;
        }
        return totalSales.getTotal();
    }

    @GetMapping("/getHotCity")
    public Object getHotCity() {
        HotCity hotCity = realApp.getHotCity();
        if (hotCity == null) {
            return 0;
        }
        return hotCity.getCity();
    }

    @GetMapping("/getActiveUserCount")
    public Object getActiveUserCount() {
        ActiveUserCount activeUserCount = realApp.getActiveUserCount();
        if (activeUserCount == null) {
            return 0;
        }
        return activeUserCount.getUserName();
    }
}
