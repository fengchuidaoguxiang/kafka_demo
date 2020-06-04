package com.atguigu.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class PartitionProducer {

    public static void main(String[] args) {
        //1.创建配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.79.160:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //添加分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.atguigu.partitioner.MyPartitioner");

        //2.创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //3.发送数据
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("aaa", "wjx", "atguigu----" + i), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println(metadata.partition() + "-----" + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });
        }

        //4.关闭资源
        producer.close();
    }
}
