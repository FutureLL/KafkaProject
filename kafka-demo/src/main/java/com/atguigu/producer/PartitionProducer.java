package com.atguigu.producer;

import com.atguigu.partitioner.MyPartitioner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @description: 使用自定义 partition 的 producer
 * @author: Mr.Li
 * @date: Created in 2020/5/27 16:59
 * @version: 1.0
 * @modified By:
 */
public class PartitionProducer extends MyPartitioner {

    public static void main(String[] args) throws InterruptedException {

        // 1、创建 Kafka 生产者信息
        Properties properties = new Properties();

        // 2、指定连接的 Kafka 集群,命令行是: --broker-list
        // properties.put("bootstrap.servers", "localhost:9092");
        // public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // 3、ACK 应答级别
        // properties.put("acks", "all");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");

        // 4、重试次数
        // properties.put("retries", 1);
        properties.put(ProducerConfig.RETRIES_CONFIG, 1);

        /**
         * 相关参数：
         * batch.size：只有数据积累到 batch.size 之后，sender 才会发送数据。
         * linger.ms：如果数据迟迟未达到 batch.size，sender 等待 linger.time 之后就会发送数据。
         */
        // 5、批次大小,要写入到 RecordAccumulator 中: 16K
        // properties.put("batch.size", 16384);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        // 6、等待时间,1ms 发送一次数据：1ms
        // properties.put("linger.ms", 1);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        // 7、RecordAccumulator 缓冲区大小: 32GB
        // properties.put("buffer.memory", 33554432);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        // 8、指定 Key,Value 的序列化类
        // properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 设置自定义 partition 分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.atguigu.partitioner.MyPartitioner");

        // Ctrl + P: 查看方法参数
        // 9、创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // 10、发送数据,批量发送,批量消费
        for (int i = 0; i < 10; i++) {

            // send(): Asynchronously send a record to a topic.
            // ProducerRecord(): Create a record with no key.
            producer.send(new ProducerRecord<String, String>("first", "atguigu -- " + i), (metadata, exception) -> {
                if (exception == null) {
                    // partition -- offset
                    System.out.println("partition: " + metadata.partition());
                } else {
                    exception.printStackTrace();
                }
            });
        }

        /**
         * 注意: 如果当发送数据量不到 16K 或者不足 1ms,
         * 在没有主动 close() 关闭资源的话,那么会清空内存中的数据,
         * 消费者接收不到消息。如果需要检测,那么给当前线程设置一个睡眠即可。
         * 但是这种方式不靠谱,调用 close() 靠谱，关闭连接,资源回收。
         *
         * Thread.sleep(100);
         */

        // 11、关闭连接,资源回收
        producer.close();
    }
}
