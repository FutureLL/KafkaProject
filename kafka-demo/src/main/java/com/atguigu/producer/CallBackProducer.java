package com.atguigu.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @description: 异步发送, 带回调函数的 API
 * @author: Mr.Li
 * @date: Created in 2020/5/27 11:59
 * @version: 1.0
 * @modified By:
 */
public class CallBackProducer {

    public static void main(String[] args) {

        // 1、创建配置信息
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // 还有一个配置,因为用的都是默认值,所以可以不写

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 2、创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 3、发送数据
        for (int i = 0; i < 10; i++) {

            // send(): Asynchronously send a record to a topic.
            // ProducerRecord(): Create a record with no key.
            producer.send(new ProducerRecord<>("first", "atguigu -- " + i), new Callback() {

                @Override
                // 成功返回 metadata,失败返回 exception
                // RecordMetadata: The metadata for a record that has been acknowledged by the server
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println(metadata.partition() + " -- " + metadata.offset());
                    }
                }
            });

            // Lambda 表达式
            producer.send(new ProducerRecord<>("first", "atguigu -- " + i), (metadata, exception) -> {
                if (exception == null) {
                    // partition -- offset
                    System.out.println("partition: " + metadata.partition() + " -- offset" + metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });

            // Lambda 表达式
            // ProducerRecord(): 方法中指定了两个参数,partition 以及 key
            // key: The key that will be included in the record
            producer.send(new ProducerRecord<>("first", 0, "atguigu", "first -- " + i), (metadata, exception) -> {
                if (exception == null) {
                    // partition -- offset
                    System.out.println("partition: " + metadata.partition() + " -- offset: " + metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });
        }

        // 4、关闭资源
        producer.close();
    }
}
