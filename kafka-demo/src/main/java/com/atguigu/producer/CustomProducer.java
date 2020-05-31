package com.atguigu.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @description:
 * @author: Mr.Li
 * @date: Created in 2020/5/27 20:16
 * @version: 1.0
 * @modified By:
 */
public class CustomProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // 1、创建 Kafka 生产者信息
        Properties properties = new Properties();

        // 2、指定连接的 Kafka 集群,命令行是: --broker-list
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // 3、ACK 应答级别
        properties.put(ProducerConfig.ACKS_CONFIG, "all");

        // 4、重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 1);

        // 5、批次大小,要写入到 RecordAccumulator 中: 16K
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        // 6、等待时间,1ms 发送一次数据：1ms
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        // 7、RecordAccumulator 缓冲区大小: 32GB
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        // 8、指定 Key,Value 的序列化类
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 9、创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // 10、发送数据,批量发送,批量消费
        for (int i = 0; i < 10; i++) {
            Future<RecordMetadata> first = producer.send(new ProducerRecord<String, String>("first", "atguigu -- " + i));
            first.get();
        }

        // 11、关闭连接,资源回收
        producer.close();
    }
}
