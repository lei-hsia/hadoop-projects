package com.lei.producer;

import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.Properties;

public class CallbackProducer {
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
            producer.send(new ProducerRecord<String, String>("first", list.get(i%3), "atguigu--" + i), (recordMetadata, e) -> {
                if (recordMetadata != null) {
                    System.out.println(recordMetadata.partition() + "---" + recordMetadata.offset());
                } else {
                    e.printStackTrace();
                }
            });
        }

        // 4. close
        producer.close();

    }
}
