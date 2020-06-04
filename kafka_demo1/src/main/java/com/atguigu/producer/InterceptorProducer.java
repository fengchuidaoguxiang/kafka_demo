package com.atguigu.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class InterceptorProducer {

    public static void main(String[] args) {
        //1.创建Kafka生产者的配置信息
        Properties properties = new Properties();
        //2.指定连接的Kafka集群
//        properties.put("bootstrap.servers", "192.168.79.160:9092");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.79.160:9092");

        //3.ACK应答级别
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        //4.重试次数
        properties.put("retries", 3);
        //5.批次大小
        properties.put("batch.size", 16384);//16384 = 16KB
        //6.等待时间
        properties.put("linger.ms", 1);
        //7.RecordAccumulator 缓冲区大小
        properties.put("buffer.memory", 33554432);//33554432 = 32MB
        //8.Key, Value的序列化类
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //添加拦截器
        ArrayList<String> interceptors = new ArrayList<String>();
        interceptors.add("com.atguigu.interceptor.TimeInterceptor");
        interceptors.add("com.atguigu.interceptor.CounterInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);


        //9.创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        System.out.println("tt");
        //10.发送数据
        for (int i = 0; i < 10; i++) {
            Future<RecordMetadata> first = producer.send(new ProducerRecord<String, String>("first", "wjx", "atguigu-----" + i));
        }

//        try {
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//        System.out.println("*********************");

        //11.关闭资源
        producer.close();
    }
}
