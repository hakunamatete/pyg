package com.itheima.report.controller;

import com.alibaba.fastjson.JSON;
import com.itheima.report.bean.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/")
public class ReportController {

    @Value("${kafka.topic}")
    private String topic;

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @RequestMapping("/receive")
    public Map receive(@RequestBody String json) {
        // 保存返回给前端的结果
        Map<String, String> result = new HashMap<>();

        try {
            // 将所有的数据都封装在一个Message实体类中
            Message message = new Message();

            // 消息体（点击流消息）
            message.setMessage(json);
            // 点击的数量
            message.setCount(1L);
            // 事件时间
            message.setTimestamp(System.currentTimeMillis());

            // 将实体类转换为JSON字符串
            String messageJSON = JSON.toJSONString(message);
            System.out.println(messageJSON);

            // 发送消息到kafka的指定topic中
            kafkaTemplate.send(topic,messageJSON);

            result.put("result", "ok");
        } catch (Exception e) {
            e.printStackTrace();
            result.put("result", "failed");
        }

        return result;
    }
}
