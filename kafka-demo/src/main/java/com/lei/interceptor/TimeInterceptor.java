package com.lei.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class TimeInterceptor implements ProducerInterceptor<String, String> {

    @Override
    public void configure(Map<String, ?> map) {

    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // record: 没有set方法，说明一旦构建不能改，所以只能创建一个新的record对象

        // 1. 取出数据
        String value = record.value();
        // 2. 创建新的ProducerRecord对象，返回
        return new ProducerRecord<>(record.topic(), record.partition(), record.key(),
                System.currentTimeMillis()+","+record.value());
    }

    @Override // 发送之后，返回值
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

}
