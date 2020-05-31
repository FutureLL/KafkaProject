package com.atguigu.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * @description: 自动提交 offset 及手动提交 offset
 *               手动提交只要调用两个方法即可,commitSync 同步提交或者 commitAsync 异步提交
 * @author: Mr.Li
 * @date: Created in 2020/5/27 20:46
 * @version: 1.0
 * @modified By:
 */
public class MyConsumer {

    public static void main(String[] args) {

        // 1、创建消费者配置信息
        Properties properties = new Properties();

        // 2、给配置信息赋值
        // 连接集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // 开启自动提交 offset 功能
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        /**
         * properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
         * 如果把 true 改成 false,首先下面这行代码会失去作用,比如说,之前消费到100,
         * 保存到90,如果有新数据91~100,在启动 consumer 会将数据进行消费,但是 offset
         * 没有提交、更新,里边保存的还是90,重新启动还是从91开始,说明只在第一次启动的时候。
         * 怎么证明第一次启动,因为程序中有循环,所以它是不停的拉取的,但是没有从91开始消费,
         * 它把 offset 进行存储,不是每一次拉取都要访问 offset,因为这样太慢,在内存中维护了一个100,
         * 下一次从100开始消费,没有将90替换成100,只要当前进程没挂,不会访问90
         */

        // 自动提交 offset 的时间间隔
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        // key、value 的反序列化,Ctrl + N 搜索 StringDeserializer 类,赋值全限定名
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // 消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "bigdata_One");

        // 重置消费者的 offset
        /**
         * What to do when there is no initial offset in Kafka or
         * if the current offset does not exist any more on the server
         * (e.g. because that data has been deleted).
         *
         * 只有当满足上述两个条件时才会生效
         * 举例: 当新建消费者组时,满足上述条件,就会根据下边的参数,进行消费数据,
         *       当在此运行这个类,由于消费者组已经存在,并且没有被删除,不满足上
         *       述条件中的任何一个,那么就不会重置消费者组的 offset,根据当前的
         *       offset 进行消费数据
         *
         * 面试题: 如何重新消费某个主题的数据
         * 解决: 更换消费者组,并且添加如下的配置
         *
         * 两个参数:latest、earliest,默认为 latest
         */
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // 订阅主题,订阅多个主题: Arrays.asList("first","second"),本例中只订阅了一个
        // Ctrl + Alt + V: 查看返回值类型,如下代码返回值为 void 类型,订阅主题类似于添加了一个属性
        // 当订阅的是一个未知的 topic,没什么影响,但是会有一个警告,加入 Log4J 能看到
        consumer.subscribe(Collections.singletonList("first"));

        // 获取数据,批量获取
        // 使用 while 保证程序不会因为运行完成而退出,阻塞程序,用于拉去生产者发送的数据
        while (true) {
            // poll: Fetch data for the topics or partitions specified using one of the subscribe/assign APIs.
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);

            // 解析并打印 ConsumerRecords
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key() + " -- " + consumerRecord.value());
            }

            // 同步提交与异步提交: 两种方式都需要关闭自动提交
            // properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            // 同步提交,当前线程会阻塞,直到 offset 提交成功
            consumer.commitSync();
            // 异步提交，提交跟拉取分开,主线程继续拉取数据,有专门的线程进行提交
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception != null) {
                        System.err.println("Commit failed for" + offsets);
                    }
                }
            });
        }
        // 关闭连接
        // consumer.close();
    }
}
