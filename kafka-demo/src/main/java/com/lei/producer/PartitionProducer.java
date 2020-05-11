package com.lei.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class PartitionProducer {
    public static void main(String[] args) {
        // 1. config
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.lei.partitioner.MyPartitioner");

        // 2. 创建生产者对象的地方没有能加partitioner的地方，这里不能加
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 2. 发送消息
        for (int i = 0; i < 100; i++) { // 发送信息的地方，ProducerRecord也没有加入partitioner的地方
            producer.send(new ProducerRecord<>("first_topic", "atguigu--"+i), ((recordMetadata, e) -> {
                if (e != null) {
                    System.out.println(recordMetadata.partition() + "--" + recordMetadata.offset()); // 看自定义partition的结果
                } else {
                    e.printStackTrace();
                }
            }));
        }

        // 3. 关闭资源
        producer.close();
    }
}
