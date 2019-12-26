package com.itheima.report.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author: Mr.Li
 * @Date: 16:33
 */
@RestController
@RequestMapping("/")
public class TestController {

    @RequestMapping("/test")
    public String test(String json){
        System.out.println(json);
        return  json;
    }
}
