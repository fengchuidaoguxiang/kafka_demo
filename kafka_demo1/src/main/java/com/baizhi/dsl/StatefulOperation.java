package com.baizhi.dsl;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import sun.net.www.MeteredStream;

import java.util.Arrays;
import java.util.Properties;

/**
 *  有状态的转换操作
 */
public class StatefulOperation {

    public static void main(String[] args) {

        // 1. 定义一个流构建器对象  用于Topology构建
        StreamsBuilder sb = new StreamsBuilder();
        // 此时kStream反应的是t11 topic的记录序列
        KStream< String,String > kStream = sb.stream("t11");

        //=================================================================================
        /*
        kStream
                .flatMapValues(line -> Arrays.asList(line.split("\\s")))
                .map( (k, v) -> new KeyValue<>(v, 1L))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                // 参数1：初始化器，提供一个初始化值
                // 参数2：聚合器，用以进行聚合操作
                // 参数3：因为涉及到状态数据的更新，读写操作changelog的topic，指定KV序列化和反序列化器的类型
                .aggregate(
                        () -> 0L ,
                        (k, v, aggValue) ->  v + aggValue,
                        Materialized.with(Serdes.String() , Serdes.Long())
                        )
                .toStream()
                .foreach((k,v) -> System.out.println(k + " \t " + v));
         */

        //=================================================================================

        kStream
                .flatMapValues(line -> Arrays.asList(line.split("\\s")))
                .map( (k, v) -> new KeyValue<>(v, 1L))
                .peek((k, v) -> System.out.println( k + "\t" + v ))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                // 参数1：计算器
                // 参数2：因为涉及到状态数据的更新，读写操作changelog的topic，指定KV序列化和反序列化器的类型
                // 参数3：
                .reduce((v1, v2) -> v1 - v2, Materialized.with(Serdes.String() , Serdes.Long()))
                .toStream()
                .foreach((k,v) -> {
                    // System.out.println(k + " \t " + v);
                });


        // 2. 构建Kafka Stream的配置对象
        // 配置对象
        Properties prop = new Properties();
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.79.31:9092,192.168.79.32:9092,192.168.79.33:9092");
        // Key & Value 默认的序列化和反序列化器
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-dsl4"); // 应用名 以后作为消费组的标识
        prop.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);

        // 3. 构建流处理应用
        KafkaStreams kafkaStreams = new KafkaStreams( sb.build(),prop);

        System.out.println(sb.build().describe());

        // 4. 开始运行
        kafkaStreams.start();
    }
}
