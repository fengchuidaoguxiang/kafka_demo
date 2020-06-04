package com.baizhi.transaction.produceonly;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

/**
 * 生产者 事务
 */
public class ProducerDemo {

    public static void main(String[] args) {

        // 生产者的配置信息
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.79.31:9092,192.168.79.32:9092,192.168.79.33:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 事务操作 需要开启幂等写操作支持
        prop.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // 事务操作，必须有一个唯一标识的事务编号
        prop.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());

        // record k v 泛型
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        // 初始化事务环境
        producer.initTransactions();

        // 开启事务
        producer.beginTransaction();

        try {
            // 通过生产者发布消息
            for (int i = 220; i < 240; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("t3", "user00" + i, "xh" + i);
                /*
                if (i == 235) {
                    // 模拟业务错误
                    int m = 1 / 0;
                }
                 */
                producer.send(record);

            }

            // 提交事务
            producer.commitTransaction();
        }catch (Exception e){
            // 取消事务
            producer.abortTransaction();
            e.printStackTrace();
        }

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
