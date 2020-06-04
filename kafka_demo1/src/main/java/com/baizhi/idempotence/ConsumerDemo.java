package com.baizhi.idempotence;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

/**
 * 消费者
 */
public class ConsumerDemo {

    public static void main(String[] args) {
        // 配置对象
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.79.31:9092,192.168.79.32:9092,192.168.79.33:9092");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // earliest
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "g3");
//        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // 消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

        // 订阅主题
        consumer.subscribe(Arrays.asList("t3"));

        // 循环拉取t2 topic中新增数据
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            records.forEach(record -> {
                System.out.println(record.key()
                        + "\t" + record.value()
                        + "\t" + record.timestamp()
                        + "\t" + record.offset()
                        + "\t" + record.partition()
                        + "\t" + record.topic()
                );

                // 可以通过异步或者同步方式手动提交当前消费者的消费位置，建议使用异步方式【效率高】
//            consumer.commitAsync();
                HashMap<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                // 从已提交的offset + 1 提交开始拉取新数据
                map.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                consumer.commitSync(map);
            });
        }
    }
}
