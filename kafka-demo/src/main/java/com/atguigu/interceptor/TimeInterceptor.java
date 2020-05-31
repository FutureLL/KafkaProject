package com.atguigu.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @description: 时间拦截器
 * @author: Mr.Li
 * @date: Created in 2020/5/28 19:40
 * @version: 1.0
 * @modified By:
 */
public class TimeInterceptor implements ProducerInterceptor<String, String> {

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {

        // 获取数据
        String value = record.value();

        // 创建一个新的 record,把时间戳写入消息体的最前部
        // ProducerRecord: Creates a record with a specified timestamp to be sent to a specified topic and partition
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(),
                record.key(), System.currentTimeMillis() + "," + value);
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }
}
