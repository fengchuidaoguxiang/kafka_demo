package com.baizhi.idempotence;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 生产者
 * 开启幂等写操作支持
 */
public class ProducerDemo {

    public static void main(String[] args) {

        // 生产者的配置信息
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.79.31:9092,192.168.79.32:9092,192.168.79.33:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        prop.put(ProducerConfig.ACKS_CONFIG, "all");
        prop.put(ProducerConfig.RETRIES_CONFIG, 10);
        prop.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1);
        // 开启幂等写操作支持
        prop.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);


        // record k v 泛型
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        // 通过生产者发布消息
//        ProducerRecord<String, String> record = new ProducerRecord<String, String>("t2","user003", "xh");
//        producer.send(record);

        ProducerRecord<String, String> record = new ProducerRecord<String, String>("t3", "user00206", "xh206");
        producer.send(record);

        producer.flush();
        // 释放资源
        producer.close();
    }
}
