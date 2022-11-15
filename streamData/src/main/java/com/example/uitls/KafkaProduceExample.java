package com.example.uitls;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProduceExample {
    public KafkaProducer<String, String> kafkaProduceObject(String brokerList) {
        Properties props = new Properties();

        // 服务器ip:端⼝号，集群⽤逗号分隔
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

        // ack模式
        props.put(ProducerConfig.ACKS_CONFIG, "-1");
        // 失败重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, "3");

        // key序列化指定类
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // value序列化指定类
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // ⽣产者对象
        return new KafkaProducer<String, String>(props);
    }
}
