package com.lei.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/*   手动提交，自己持久化维护已经消费到的offset */
public class CustomConsumer {

    private static Map<TopicPartition, Long> currentOffset = new HashMap<>();

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserialization");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserialization");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka01");

        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Arrays.asList("first", "second"), new ConsumerRebalanceListener() {
            @Override // 在Rebalance之前调用
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                commitOffset(currentOffset);
            }

            @Override // Rebalance了!!
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                currentOffset.clear();
                for (TopicPartition partition : collection) {
                    consumer.seek(partition, getOffset(partition));
                }
            }
        });

        while (true) {
            ConsumerRecords<Object, Object> consumerRecords =  consumer.poll(1000);
            for (ConsumerRecord<Object, Object> record : consumerRecords) {
                System.out.println(record.partition() + record.offset());

                currentOffset.put(new TopicPartition(record.topic(), record.partition()), record.offset());
                commitOffset(currentOffset); // 异步提交
            }
        }
    }

    // 获取某个partition最近的offset
    public static int getOffset(TopicPartition partition) {
        return 0;
    }


    public static void commitOffset(Map<TopicPartition, Long> currentOffset) {

    }
}
