package com.atguigu.consumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class Test {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
//        // 1.创建消费者配置信息
//        Properties properties = new Properties();
//
//        // 2.给配置信息赋值
//        //  连接的集群
//        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.79.176:9092");
////        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaIP);
//
//        // 开启自动提交
////        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
////        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
//        // 自动提交的延时
////        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
//        // Key,Value的反序列化
//        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
//        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
//        // 重置消费者的offset
//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
//          properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, position);



        Properties props = new Properties();
        // 只需要提供一个或多个 broker 的 IP 和端口
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.79.176:9092");
        // 创建 AdminClient 对象
        AdminClient client = KafkaAdminClient.create(props);
        // 获取 topic 列表
        Set topics = client.listTopics().names().get();
        System.out.println(topics);
    }
}
