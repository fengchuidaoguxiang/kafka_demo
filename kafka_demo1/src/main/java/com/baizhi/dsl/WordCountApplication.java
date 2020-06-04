package com.baizhi.dsl;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.ArrayList;
import java.util.Properties;

/**
 *  dsl风格实现单词计数（实时）
 *
 *      高阶方法    简化了处理器API
 */
public class WordCountApplication {

    public static void main(String[] args) {

        // 1. 定义一个流构建器对象  用于Topology构建
        StreamsBuilder sb = new StreamsBuilder();
        // 此时kStream反应的是t11 topic的记录序列
        KStream< String,String > kStream = sb.stream("t11");
        KTable<String, Long> kTable = kStream
                // 将一个输入展开为多个输出   line
                // 输入：Hello World   输出： （Hello, Hello） (World, Workd);
                .flatMap((k, v) -> {
                    String[] words = v.split("\\s");
                    ArrayList<KeyValue<String, String>> keyValues = new ArrayList<>();
                    for (int i = 0; i < words.length; i++) {
                        keyValues.add(new KeyValue<String, String>(words[i], words[i]));
                    }
                    return keyValues;
                })
                // 将一个输入 转换为另外的一个输出
                // 输入：（Hello,Hello） (World,World) 输出：（Hello, 1） (World, 1)
                .map((String k, String v) -> new KeyValue<String, Long>(k, 1L))
                // 根据K 进行分组操作
                // 输入：（Hello,1） (World,1) 输出：（Hello, [1,1,1,1,1]） （World, [1,1]）
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                // 统计k相同的value个数
                // 输入：（Hello, [1,1,1,1,1]） （World, [1,1]）  输出：（Hello, 4） （World, 2）
                .count();


        // 将结果表 转换为 KStream 写出到结果Topic中
        kTable.toStream().to("t12", Produced.with(Serdes.String(), Serdes.Long()));

        // 2. 构建Kafka Stream的配置对象
        // 配置对象
        Properties prop = new Properties();
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.79.31:9092,192.168.79.32:9092,192.168.79.33:9092");
        // Key & Value 默认的序列化和反序列化器
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-dsl"); // 应用名 以后作为消费组的标识
        prop.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);

        // 3. 构建流处理应用
        KafkaStreams kafkaStreams = new KafkaStreams( sb.build(),prop);

        System.out.println(sb.build().describe());

        // 4. 开始运行
        kafkaStreams.start();
    }
}
