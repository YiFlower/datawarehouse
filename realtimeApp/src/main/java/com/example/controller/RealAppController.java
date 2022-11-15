package com.example.controller;

import com.example.service.RealAppImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/hbase")
public class RealAppController {
    @Autowired
    RealAppImpl realApp;

    @GetMapping("/getAllTableName")
    public Object getAllTableName() {
        realApp.getNewUserCount();
        realApp.getTotalSales();
        realApp.getHotCity();
        realApp.getActiveUserCount();

        return null;
    }
}
