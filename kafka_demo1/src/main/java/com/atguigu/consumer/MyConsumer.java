package com.atguigu.consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.sql.SQLOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MyConsumer {
    static {
        BasicConfigurator.configure();
    }

    public static void main(String[] args) {
        try {

//        Logger logger = Logger.getLogger("fileAppender");
//        logger.info("hello ------------------------------world");

            // 生产测试
            if (StringUtils.equals("1", args[0])) {
                System.out.println("------生产端测试---------");
                String ipAndPort = args[1];
                System.out.println("kafka IP: " + ipAndPort);
                String topic = args[2];
                System.out.println("topic: " + topic);

                producer(ipAndPort, topic);

            }
            // 消费测试
            else if (StringUtils.equals("2", args[0])) {
                System.out.println("------消费端测试---------");

                // 消费位置
                String position = args[1];

                // Kafka机器IP
                String kafkaIP = args[2];

                System.out.println("kafka IP: " + kafkaIP);

                // 消费者组ID
                String groupId = args[3];
                System.out.println("groupId: " + groupId);

                // TOPIC
                String topic = args[4];
                System.out.println("topic: " + topic);
                consumer(position, kafkaIP, groupId, topic);
            }
            // 判断topic是否存在
            else if (StringUtils.equals("3", args[0])) {
                System.out.println("------判断topic是否存在---------");
                String ipAndPort = args[1];
                String topic = args[2];
                topicExist(ipAndPort, topic);

            }
            // 获取topic列表
            else if (StringUtils.equals("4", args[0])) {
                System.out.println("------获取topic列表---------");
                topicList(args[1]);
            }

            // 获取topic的偏移量
            else if (StringUtils.equals("5", args[0])) {
                System.out.println("------获取topic的偏移量---------");
                String ipAndPort = args[1];
                String topic = args[2];
                String groupid = args[3];

                topicExist2(ipAndPort, topic, groupid);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void topicList(String arg) {
        String ipAndPort = arg;
        Properties props = new Properties();
        // 只需要提供一个或多个 broker 的 IP 和端口
//            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.79.176:9092");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ipAndPort);

        // 创建 AdminClient 对象
        AdminClient client = KafkaAdminClient.create(props);
        // 获取 topic 列表
        Set topics = null;
        try {
            topics = client.listTopics().names().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println("topic 列表：" + topics);
    }

    private static void topicExist(String ipAndPort, String topic) {
        Properties props = new Properties();
        // 只需要提供一个或多个 broker 的 IP 和端口
//            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.79.176:9092");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ipAndPort);

        // 创建 AdminClient 对象
        AdminClient client = KafkaAdminClient.create(props);
        // 获取 topic 列表
        Set topics = null;
        try {
            topics = client.listTopics().names().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
//            System.out.println("topic 列表："+topics);
        if (topics == null) {
            System.out.println("topic列表获取不到");
            return;
        }
        // 判断是否存在
        if (topics.contains(topic)) {
            System.out.println("topic: [ " + topic + " ] 存在.");
        } else {
            System.out.println("topic: [ " + topic + " ] 不存在！！！");
        }
    }

    private static void consumer(String position, String kafkaIP, String groupId, String topic) {
//        // 消费位置
//        String position = arg;
//
//        // Kafka机器IP
//        String kafkaIP = arg1;
//
//        System.out.println("kafka IP: " + kafkaIP);
//
//        // 消费者组ID
//        String groupId = arg2;
//        System.out.println("groupId: " + groupId);
//
//        // TOPIC
//        String topic = arg3;
//        System.out.println("topic: " + topic);


        // 1.创建消费者配置信息
        Properties properties = new Properties();

        // 2.给配置信息赋值
        //  连接的集群
//        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.79.176:9092");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaIP);

        // 开启自动提交
//        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
//        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // 自动提交的延时
//        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        // Key,Value的反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 重置消费者的offset
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//          properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, position);

        //消费者组
//        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"mybigdata");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        //创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //订阅主题
//        consumer.subscribe(Arrays.asList("first", "second","third","four"));
        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            //获取数据
            ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
            //解析并打印consumerRecords
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key() + "---------" + consumerRecord.value());
            }
        }
    }


    private static void producer(String ipAndPort, String topic) {
        //1.创建Kafka生产者的配置信息
        Properties properties = new Properties();
        //2.指定连接的Kafka集群
//        properties.put("bootstrap.servers", "192.168.79.160:9092");
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.79.176:9092");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ipAndPort);

//        //3.ACK应答级别
//        properties.put(ProducerConfig.ACKS_CONFIG, "all");
//        //4.重试次数
//        properties.put("retries", 3);
//        //5.批次大小
//        properties.put("batch.size", 16384);//16384 = 16KB
        //6.等待时间
//        properties.put("linger.ms", 10000);
//        //7.RecordAccumulator 缓冲区大小
//        properties.put("buffer.memory", 33554432);//33554432 = 32MB
        //8.Key, Value的序列化类
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //9.创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
//        System.out.println("tt");
        //10.发送数据
        for (int i = 0; i < 10000; i++) {
            Future<RecordMetadata> first = producer.send(new ProducerRecord<String, String>(topic, "wjxProducer", "----------kafka数据-------" + i));
            try {
                //同步方式
                RecordMetadata recordMetadata = first.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        //11.关闭资源
        producer.close();
        System.out.println("生产端结束");
    }

    private static void topicExist2(String ipAndPort, String topic, String groupid) {
        Properties props = new Properties();
        // 只需要提供一个或多个 broker 的 IP 和端口
//            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.79.176:9092");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ipAndPort);

        // 创建 AdminClient 对象
        AdminClient client = KafkaAdminClient.create(props);
        // 获取 topic 列表
        Set topics = null;
        try {
            topics = client.listTopics().names().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
//            System.out.println("topic 列表："+topics);
        if (topics == null) {
            System.out.println("topic列表获取不到");
            return;
        }
        // 判断是否存在
        if (topics.contains(topic)) {
            System.out.println("topic: +" + topic + " 存在.");
            System.out.println("topic: +" + topic + ",消费组id:" + groupid + ",消费偏移量" + client.listConsumerGroupOffsets(groupid));
        } else {
            System.out.println("topic: +" + topic + " 不存在！！！");
        }
    }
}
