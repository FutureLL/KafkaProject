package com.atguigu.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @description: 自定义存储 offset
 * @author: Mr.Li
 * @date: Created in 2020/5/28 16:40
 * @version: 1.0
 * @modified By:
 */
public class RebalanceConsumer {

    private static Map<TopicPartition, Long> currentOffset = new HashMap<>();

    public static void main(String[] args) {

        // 1、创建消费者配置信息
        Properties properties = new Properties();

        // 2、给配置信息赋值
        // 连接集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // 消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "bigdata_Two");

        // 关闭自动提交 offset 功能
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // key、value 的反序列化,Ctrl + N 搜索 StringDeserializer 类,赋值全限定名
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // 消费者订阅主题
        /**
         * ConsumerRebalanceListener:
         *   A callback interface that the user can implement to trigger custom
         *   actions when the set of partitions assigned to the consumer changes.
         *
         * 一个回调接口,用户可以实现触发自定义操作时设定的分区分配到消费者的变化
         */
        consumer.subscribe(Collections.singletonList("first"), new ConsumerRebalanceListener() {

            // 该方法会在 Rebalance 之前调用
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                commitOffset(currentOffset);
            }

            // 该方法会在 Rebalance 之后调用
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                currentOffset.clear();
                for (TopicPartition partition : partitions) {
                    // 定位到最近提交的 offset 位置继续消费
                    consumer.seek(partition, getOffset(partition));
                }
            }
        });

        // 获取数据,批量获取
        while (true) {
            // 消费者拉取数据
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {

                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());

                currentOffset.put(new TopicPartition(record.topic(), record.partition()), record.offset());
            }
            // 异步提交
            commitOffset(currentOffset);
        }
    }

    // 获取某分区的最新 offset
    private static long getOffset(TopicPartition partition) {
        /**
         * 如果要保存到 MySQL,那么提交的时候要把数据提交到 MySQL,
         * 当需要 getOffset() 的时候,就要去 MySQL 中获取。
         */
        return 0;
    }

    // 提交该消费者所有分区的 offset
    private static void commitOffset(Map<TopicPartition, Long> currentOffset) {
        /**
         * 提交到 MySQL,对 offset 进行维护
         * 给 MySQL 创建表 (Group, Topic, Partitions, offset)
         */
    }
}
