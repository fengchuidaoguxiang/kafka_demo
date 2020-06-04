package com.baizhi.dsl;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

/**
 *  基于窗口的有状态的聚合操作
 */
public class WindowStatefulOperation {

    public static void main(String[] args) {

        // 1. 定义一个流构建器对象  用于Topology构建
        StreamsBuilder sb = new StreamsBuilder();
        // 此时kStream反应的是t11 topic的记录序列
        KStream< String,String > kStream = sb.stream("t13");
        kStream
                .flatMapValues(line -> Arrays.asList(line.split("\\s")))
                .map( (k, v) -> new KeyValue<>(v, 1L))
//                .peek((k, v) -> System.out.println( k + "\t" + v ))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                // 翻滚窗口
                // .windowedBy(TimeWindows.of(Duration.ofSeconds(10)))
                // 跳跃窗口 窗口大小10秒  跳跃步长3秒
                // w1：0s-10s   w2：3s-13s   w3：6s-16s
                //.windowedBy(TimeWindows.of(Duration.ofSeconds(10)).advanceBy(Duration.ofSeconds(3)))
                // 会话窗口   会话的活跃间隙是10秒
                .windowedBy(SessionWindows.with(Duration.ofSeconds(10)))
                // 参数1：计算器
                // 参数2：因为涉及到状态数据的更新，读写操作changelog的topic，指定KV序列化和反序列化器的类型
                // 参数3：
                .reduce((v1, v2) -> v1 + v2, Materialized.with(Serdes.String() , Serdes.Long()))
                .toStream()
                .foreach((k,v) -> {
                    String key = k.key();
                    Window window = k.window();
                    long startTS = window.start();
                    long endTS = window.end();
                    SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
                    String start = sdf.format(new Date(startTS));
                    String end = sdf.format(new Date(endTS));
                    System.out.println( key + "\t" + start + "<---------->" + end + "\t" + v );
                });


        // 2. 构建Kafka Stream的配置对象
        // 配置对象
        Properties prop = new Properties();
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.79.31:9092,192.168.79.32:9092,192.168.79.33:9092");
        // Key & Value 默认的序列化和反序列化器
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-dsl5"); // 应用名 以后作为消费组的标识
        prop.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);

        // 3. 构建流处理应用
        KafkaStreams kafkaStreams = new KafkaStreams( sb.build(),prop);

        System.out.println(sb.build().describe());

        // 4. 开始运行
        kafkaStreams.start();
    }
}
