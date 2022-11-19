package com.example.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;


@CrossOrigin
@Controller
public class WebController {

    @RequestMapping("/index")
    public String indexPage() {
        return "realtime";
    }
}
