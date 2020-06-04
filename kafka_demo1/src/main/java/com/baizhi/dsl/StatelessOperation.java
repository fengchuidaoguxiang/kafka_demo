package com.baizhi.dsl;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

/**
 *  无状态的转换操作
 */
public class StatelessOperation {

    public static void main(String[] args) {

        // 1. 定义一个流构建器对象  用于Topology构建
        StreamsBuilder sb = new StreamsBuilder();
        // 此时kStream反应的是t11 topic的记录序列
        KStream< String,String > kStream = sb.stream("t11");

        //=================================================================================
        /*
        KStream<String, String>[] branch = kStream
                // 分流
                .branch(
                        (k, v) -> v.length() == 4,
                        (k, v) -> v.startsWith("AA"),
                        (k, v) -> true
                );
        branch[0].foreach( (k,v) -> System.out.println( k + "\t" + v) );
        */

        //=================================================================================
        /*
        kStream
                // 过滤，保存符合条件的结果
//                .filter( (k, v) -> v.equals("Hello") )
                // 过滤，保存不符合条件的结果
                .filterNot((k, v) -> v.equals("Hello"))
                .foreach((k, v) -> System.out.println(k + "\t" + v));
        */
        //=================================================================================
        /*
        kStream
                // 输入(一行英文短语) : k=null v=Hello Hadoop
                // 输出(null, Hello) (null, Hadoop)
                .flatMapValues( (String line) -> {
                    String[] words = line.split("\\s");
                    return Arrays.asList(words);
                })
                .foreach((k, v) -> System.out.println(k + "\t" + v));
        */
        //=================================================================================
        /*
        kStream
                // 输入(一行英文短语) : k=null v=Hello Hadoop
                // 输出(null, Hello) (null, Hadoop)
                .flatMapValues( (String line) -> {
                    String[] words = line.split("\\s");
                    return Arrays.asList(words);
                })
                // (Hello, 1L)  (Hadoop, 1L)
                 .map((k, v) -> new KeyValue<String, Long>( v, 1l ))
                // (Hello, 10L)  (Hadoop, 10L)
                .mapValues( v -> v * 10)
                .foreach((k, v) -> System.out.println(k + "\t" + v));
          */
        //=================================================================================
        /*
        kStream
                 // line: Hello Hadoop
                 .flatMapValues(line -> Arrays.asList(line.split("\\s")))
                 // (Hello, Hello) (Hadoop, Hadoop)
                 .map( (k, v) -> new KeyValue<>(v, v) )
                 //  根据KeyValue对象的value进行分组
                 .groupBy( (String k,String v) -> v,Grouped.with(Serdes.String(), Serdes.String()))
                 .count()
                 .toStream()
                 .foreach((k, v) -> System.out.println(k + "\t" + v));
        */
        //=================================================================================
        /*
        KStream<String, String>[] branch = kStream.branch((k, v) -> v.length() == 4,
                (k, v) -> v.startsWith("AA"),
                (k, v) -> true
        );
        // 流和流merge 注意两个流的泛型必须一致
        KStream<String, String> newKStream = branch[0].merge(branch[1]);
        newKStream.foreach((k, v) -> System.out.println(k + "\t" + v));
        */

        //=================================================================================
        /*
        kStream
                // 不会改变数据的输入和输出，只用于调试
                // line
                .peek( (k,v) -> System.out.println(k + "\t" + v) )
                .flatMapValues((k,v) -> Arrays.asList(v.split("\\s")))
                // (null, word)
                .foreach((k,v) -> System.out.println(k + "\t --------" + v));
         */
        //=================================================================================
        /*
        kStream
                // 不会改变数据的输入和输出，只用于调试
                // line
                .peek( (k,v) -> System.out.println(k + "\t" + v) )
                .flatMapValues((k,v) -> Arrays.asList(v.split("\\s")))
                // (null, word)
                //.foreach((k,v) -> System.out.println(k + "\t --------" + v));
                .print(Printed.toSysOut());
                //.print(Printed.toFile("d:/abc"));
        */
        //=================================================================================

        kStream
                // 不会改变数据的输入和输出，只用于调试
                // line
                .peek( (k,v) -> System.out.println(k + "\t" + v) )
                .flatMapValues((k,v) -> Arrays.asList(v.split("\\s")))
                .selectKey((k, v) -> v + "?"  )
                // (null, word)
                //.foreach((k,v) -> System.out.println(k + "\t --------" + v));
                .print(Printed.toSysOut());
                //.print(Printed.toFile("d:/abc"));

        // 2. 构建Kafka Stream的配置对象
        // 配置对象
        Properties prop = new Properties();
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.79.31:9092,192.168.79.32:9092,192.168.79.33:9092");
        // Key & Value 默认的序列化和反序列化器
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-dsl2"); // 应用名 以后作为消费组的标识
        prop.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);

        // 3. 构建流处理应用
        KafkaStreams kafkaStreams = new KafkaStreams( sb.build(),prop);

        System.out.println(sb.build().describe());

        // 4. 开始运行
        kafkaStreams.start();
    }
}
