package com.lei.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SynchronousProducer {
    // Future对象阻塞前面的线程,同步发送消息，这样能保证发送的消息全局有序，不仅仅是区内有序(一个partition,也不能解决)
    public static void main(String[] args) {

        // 1. config
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 2. new Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 3. send
        ArrayList<String> list = new ArrayList<>();
        list.add("a");
        list.add("b");
        list.add("c");

        for (int i = 0; i < 100; i++) {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>("first", list.get(i % 3), "atguigu--" + i));
            try {
                future.get();  // 同步发送的方式, future对象调用get方法保证同步
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 4. close
        producer.close();

    }
}
