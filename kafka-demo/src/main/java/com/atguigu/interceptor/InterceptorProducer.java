package com.atguigu.interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @description: 主程序
 * @author: Mr.Li
 * @date: Created in 2020/5/28 20:37
 * @version: 1.0
 * @modified By:
 */
public class InterceptorProducer {

    public static void main(String[] args) {
        // 1、创建 Kafka 生产者信息
        Properties properties = new Properties();

        // 2、设置配置信息
        // 指定连接的 Kafka 集群,命令行是: --broker-list
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // ACK 应答级别
        properties.put(ProducerConfig.ACKS_CONFIG, "all");

        // 重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 1);

        // 批次大小,要写入到 RecordAccumulator 中: 16K
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        // 等待时间,1ms 发送一次数据：1ms
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        // RecordAccumulator 缓冲区大小: 32GB
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        // 指定 Key,Value 的序列化类
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 3、构建拦截链
        List<String> interceptors = new ArrayList<>();
        interceptors.add("com.atguigu.interceptor.TimeInterceptor");
        interceptors.add("com.atguigu.interceptor.CounterInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        // 4、创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 5、发送消息
        for (int i = 0; i < 10; i++) {

            producer.send(new ProducerRecord<String, String>("first", "Interceptor", "message -- " + i));
        }

        // 5、一定要关闭 producer,这样才会调用 interceptor 的 close 方法
        producer.close();
    }
}
