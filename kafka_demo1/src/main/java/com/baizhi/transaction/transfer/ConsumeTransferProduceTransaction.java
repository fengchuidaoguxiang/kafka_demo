package com.baizhi.transaction.transfer;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

/**
 *  消费生产并存事务
 */
public class ConsumeTransferProduceTransaction {

    public static void main(String[] args) {
        KafkaProducer<String, String> kafkaProducer = buildKafkaProducer();
        // 初始化事务
        kafkaProducer.initTransactions();
        KafkaConsumer<String, String> kafkaConsumer = buildKafkaConsumer();
        kafkaConsumer.subscribe(Arrays.asList("t5"));

        while(true){
            kafkaProducer.beginTransaction();
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(5));
            consumerRecords.forEach(record -> {
                try {
                    Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                    System.out.println(record.key() + "\t" + record.value() + "\t" + record.offset());

                    // 模拟业务错误
//                    if("xz".equals(record.value())){
//                        int m = 1 / 0;
//                    }

//                    kafkaProducer.send(new ProducerRecord<String, String>("t6", record.key(), record.value() + "?"));
//                    // 注意：消费生产并存事务中，消费者的消费位置(offset)，需要通过生产者sendOffsetsToTransaction方法去提交
//                    // 在map中存放消费者消费位置信息
//                    TopicPartition key = new TopicPartition(record.topic(), record.partition());
//                    OffsetAndMetadata value = new OffsetAndMetadata( record.offset() + 1 );
//                    map.put(key,value);
//                    kafkaProducer.sendOffsetsToTransaction( map,"g1");
//                    // 提交事务
//                    kafkaProducer.commitTransaction();

                    TopicPartition key = new TopicPartition(record.topic(), record.partition());
                    OffsetAndMetadata value = new OffsetAndMetadata( record.offset());
                    map.put(key, value);

                    kafkaProducer.commitTransaction();
                }catch ( Exception e){
                    // 回滚事务
                    kafkaProducer.abortTransaction();
                    e.printStackTrace();
                }});
        }
    }

    public static KafkaProducer<String, String> buildKafkaProducer(){
        // 配置对象
        // 生产者的配置信息
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.79.31:9092,192.168.79.32:9092,192.168.79.33:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 事务操作 需要开启幂等写操作支持
        prop.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // 事务操作，必须有一个唯一标识的事务编号
        prop.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(prop);

        return kafkaProducer;
    }

    public static KafkaConsumer<String, String> buildKafkaConsumer(){
        // 配置对象
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.79.31:9092,192.168.79.32:9092,192.168.79.33:9092");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // 必须使用手动提交消费位置
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");
        prop.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");// 修改事务隔离级别  读已提交 不会脏读
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 消费者对象
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(prop);
        return kafkaConsumer;
    }
}
