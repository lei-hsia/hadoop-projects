package com.lei.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class MyConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        // 连接的集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        // 开启自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 自动提交的interval
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        // key value反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserialization");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserialization");
        // 消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka01");

        // 重置消费者的offset
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 还要换组+这个属性earliest(不是--from-beginning)
        // 每次consumer拿数据，不论是从zk/kafka本地拿数据，都只是在consumer启动的时候拉取一次，否则会重复消费相同的数据

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 订阅主题
        consumer.subscribe(Arrays.asList("first","second"));

        while (true) {
            // 拉取数据
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            // 解析并打印，遍历
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key()+consumerRecord.value()); //consumer能得到key,说明key是存到了本地的，否则获取不到的
            }
        }

        // consumer.close();
    }
}
