package com.baizhi.transaction.transfer;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
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
public class ConsumeTransferProduceTransaction2 {

    public static void main(String[] args) {
        ConsumeTransferProduceTransaction2 consumeTransferProduceTransaction2 = new ConsumeTransferProduceTransaction2();
        consumeTransferProduceTransaction2.consumeTransferProduce();
    }

    public static void mainbak(String[] args) {
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
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // 必须使用手动提交消费位置
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");
        prop.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");// 修改事务隔离级别  读已提交 不会脏读
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 消费者对象
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(prop);
        return kafkaConsumer;
    }


    //=========================================================================
    /**
     * 在一个事务内,即有生产消息又有消费消息，即常说的Consume-tansform-produce模式
     */
    public void consumeTransferProduce() {
        // 1.构建上产者
        Producer producer = buildProducer();
        // 2.初始化事务(生成productId),对于一个生产者,只能执行一次初始化事务操作
        producer.initTransactions();
        // 3.构建消费者和订阅主题
        Consumer consumer = buildConsumer();
        consumer.subscribe(Arrays.asList("t5"));
        while (true) {
            // 4.开启事务
            producer.beginTransaction();
            // 5.1 接受消息
            ConsumerRecords<String, String> records = consumer.poll(500);
            try {
                // 5.2 do业务逻辑;
                System.out.println("customer Message---");
                Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
                for (ConsumerRecord<String, String> record : records) {
                    // 5.2.1 读取消息,并处理消息。print the offset,key and value for the consumer records.
                    System.out.printf("offset = %d, key = %s, value = %s\n",
                            record.offset(), record.key(), record.value());

                    // 5.2.2 记录提交的偏移量
                    commits.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset()));

                    // 6.生产新的消息。比如外卖订单状态的消息,如果订单成功,则需要发送跟商家结转消息或者派送员的提成消息
                    producer.send(new ProducerRecord<String, String>("t6", "data2"));
                }

                // 7.提交偏移量
                producer.sendOffsetsToTransaction(commits, "group0323");

                // 8.事务提交
                producer.commitTransaction();

            } catch (Exception e) {
                // 7.放弃事务
                producer.abortTransaction();
            }
        }
    }

    /**
     * 需要:
     * 1、设置transactional.id
     * 2、设置enable.idempotence
     * @return
     */
    private Producer buildProducer() {
        // create instance for properties to access producer configs
        Properties props = new Properties();
        // bootstrap.servers是Kafka集群的IP地址。多个时,使用逗号隔开
        props.put("bootstrap.servers", "192.168.79.31:9092,192.168.79.32:9092,192.168.79.33:9092");
        // 设置事务id
        props.put("transactional.id", "first-transactional");
        // 设置幂等性
        props.put("enable.idempotence",true);
        //Set acknowledgements for producer requests.
        props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        props.put("retries", 1);
        //Specify buffer size in config,这里不进行设置这个属性,如果设置了,还需要执行producer.flush()来把缓存中消息发送出去
        //props.put("batch.size", 16384);
        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        // Kafka消息是以键值对的形式发送,需要设置key和value类型序列化器
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        return producer;
    }


    /**
     * 需要:
     * 1、关闭自动提交 enable.auto.commit
     * 2、isolation.level为read_committed
     * 而且在代码里面也不能使用手动提交commitSync( )或者commitAsync( )
     * @return
     */
    public Consumer buildConsumer() {
        Properties props = new Properties();
        // bootstrap.servers是Kafka集群的IP地址。多个时,使用逗号隔开
        props.put("bootstrap.servers", "192.168.79.31:9092,192.168.79.32:9092,192.168.79.33:9092");
        // 消费者群组
        props.put("group.id", "group0323");
        // 设置隔离级别
        props.put("isolation.level","read_committed");
        // 关闭自动提交
        props.put("enable.auto.commit", "false");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer
                <String, String>(props);
        return consumer;
    }
}
