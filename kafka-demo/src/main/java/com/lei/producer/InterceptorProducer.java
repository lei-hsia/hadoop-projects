package com.lei.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;

import java.util.ArrayList;
import java.util.Properties;

public class InterceptorProducer {
    public static void main(String[] args) {

        // 1. config
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // partitioner, interceptor跟serializer一样，都是写在配置中的
        ArrayList<String> interceptors = new ArrayList<>();
        interceptors.add("com.lei.interceptor.TimeInterceptor");
        interceptors.add("com.lei.interceptor.CounterInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 2. 发送消息
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>("first_topic", "atguigu--"+i));
        }

        // 3. 关闭资源
        producer.close();
    }
}
