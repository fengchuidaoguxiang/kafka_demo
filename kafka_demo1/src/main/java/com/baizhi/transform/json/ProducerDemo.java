package com.baizhi.transform.json;

import com.baizhi.transform.jdk.UserToByteArray;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Properties;

/**
 * 生产者
 */
public class ProducerDemo {

    public static void main(String[] args) {

        // 生产者的配置信息
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.79.31:9092,192.168.79.32:9092,192.168.79.33:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserToByteArrayWithJson.class);

        // record k v 泛型
        KafkaProducer<String, User> producer = new KafkaProducer<>(prop);

        // 通过生产者发布消息
        ProducerRecord<String, User> record =
                new ProducerRecord<String, User>("t100", "user002", new User(1, "l4", true, new Date()));
        producer.send(record);

        producer.flush();
        // 释放资源
        producer.close();
    }
}
