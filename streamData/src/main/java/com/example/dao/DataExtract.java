package com.example.dao;

import com.example.uitls.KafkaProduceExample;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.*;
import java.nio.charset.StandardCharsets;

@Slf4j
public class DataExtract {
    private String brokeList = "cdh02:9092,cdh03:9092";
    private String topicName = "bigdata-01";
    private String winDataPath = "E:\\DataSet\\user_item_behavior_history.csv";
    private String unixDataPath = "/data1/dataset/user_item_behavior_history.csv";

    @SneakyThrows
    public void dataLoad() {
        // kafka生产者实例化
        KafkaProduceExample kafka = new KafkaProduceExample();
        KafkaProducer<String, String> producer = kafka.kafkaProduceObject(brokeList);

        // 读取本地数据，并发送
        String s;
        int count = 0;

        String localDataPath;
        if (System.getProperties().getProperty("os.name").equals("Linux")) {
            localDataPath = unixDataPath;
        } else {
            localDataPath = winDataPath;
        }

        InputStreamReader in = new InputStreamReader(new FileInputStream(localDataPath), StandardCharsets.UTF_8);
        BufferedReader br = new BufferedReader(in);


        while ((s = br.readLine()) != null) {
            count++;

            // 发送到Kafka
            producer.send(new ProducerRecord<>(topicName, s),
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e != null) {
                                log.error("消息发送失败！{}", e.getMessage());
                            }
                        }
                    });

            if (count % 500 == 0) {
                log.info("发送数据...第{}条，摘要：{}", count, s);
                Thread.sleep((long) (500 + Math.random() * 1000));
            }
        }
    }

    public static void main(String[] args) {
        new DataExtract().dataLoad();
    }
}
