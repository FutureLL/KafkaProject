package com.atguigu.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @description: 自定义 partition
 * @author: Mr.Li
 * @date: Created in 2020/5/27 16:18
 * @version: 1.0
 * @modified By:
 */
public class MyPartitioner implements Partitioner {

    // 分区
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // byte[] 说明已经序列化了

        // partitionCountForTopic(): Get the number of partitions for the given topic
        // Integer integer = cluster.partitionCountForTopic(topic);

        // return key.toString().hashCode() % integer;

        // 为了简单我们直接返回 1
        return 1;
    }

    // 负责关闭资源
    @Override
    public void close() {

    }

    // 读取配置信息
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
