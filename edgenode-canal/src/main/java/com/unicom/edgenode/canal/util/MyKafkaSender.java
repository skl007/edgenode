package com.unicom.edgenode.canal.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyKafkaSender {

    public static KafkaProducer<String, String> kafkaProducer = null;


    public static KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        Properties kafkaProperties = PropertiesUtil.load("config.properties");
        properties.put("bootstrap.servers", kafkaProperties.getProperty("bootstrap.servers"));
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = null;
        try {
            producer = new KafkaProducer<String, String>(properties);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return producer;
    }

    public static void send(String topic, String msg) {
        if (kafkaProducer == null) {
            kafkaProducer = createKafkaProducer();
        }
        System.out.println("发送数据到kafka： "+msg);
        kafkaProducer.send(new ProducerRecord<String, String>(topic, msg));
    }
}