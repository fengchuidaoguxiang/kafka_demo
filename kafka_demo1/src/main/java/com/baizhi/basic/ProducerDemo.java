package com.baizhi.basic;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
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
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // record k v 泛型
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        // 通过生产者发布消息
//        ProducerRecord<String, String> record = new ProducerRecord<String, String>("t2","user003", "xh");
//        producer.send(record);

//        for (int i = 50; i < 70; i++) {
//                    ProducerRecord<String, String> record = new ProducerRecord<String, String>("t1","user00" + i, "xh" + i);
//                    producer.send(record);
//        }

        // 测试生产者发布策略
        // key != null
//        ProducerRecord<String, String> record1 = new ProducerRecord<String, String>("t1","user003", "xh1");
//        ProducerRecord<String, String> record2 = new ProducerRecord<String, String>("t1","user003", "xh2");
//        ProducerRecord<String, String> record3 = new ProducerRecord<String, String>("t1","user003", "xh3");
//        producer.send(record1);
//        producer.send(record2);
//        producer.send(record3);

        // key == null
//        ProducerRecord<String, String> record1 = new ProducerRecord<String, String>("t1","xh1");
//        ProducerRecord<String, String> record2 = new ProducerRecord<String, String>("t1","xh2");
//        ProducerRecord<String, String> record3 = new ProducerRecord<String, String>("t1","xh3");
//        producer.send(record1);
//        producer.send(record2);
//        producer.send(record3);

        // 手动指定分区序号  p0分区
        ProducerRecord<String, String> record1 = new ProducerRecord<String, String>("t1", 0, "user008", "xh6");
        producer.send(record1);
        ProducerRecord<String, String> record2 = new ProducerRecord<String, String>("t1", 2, "user009", "xf7");
        producer.send(record2);

        producer.flush();
        // 释放资源
        producer.close();
    }
}
