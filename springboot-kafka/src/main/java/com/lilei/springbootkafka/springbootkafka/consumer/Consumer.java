package com.lilei.springbootkafka.springbootkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @description:
 * @author: Mr.Li
 * @date: Created in 2020/5/31 17:57
 * @version: 1.0
 * @modified By:
 */
@Component
public class Consumer {

    // 定义此消费者接收 topics = "first"的消息,与 Producer 中的 topic 对应上即可
    @KafkaListener(topics = "first")
    public void consum(ConsumerRecord<?, ?> consumerRecord) {
        System.out.println("topic: " + consumerRecord.topic() + ", value: " + consumerRecord.value().toString());
    }
}
