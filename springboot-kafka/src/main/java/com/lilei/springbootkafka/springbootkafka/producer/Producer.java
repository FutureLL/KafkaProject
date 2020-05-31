package com.lilei.springbootkafka.springbootkafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @description:
 * @author: Mr.Li
 * @date: Created in 2020/5/31 17:47
 * @version: 1.0
 * @modified By:
 */

@RestController
public class Producer {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @RequestMapping("message/send")
    public String produce() {

        kafkaTemplate.send("first","nihao");
        return "success";
    }
}
