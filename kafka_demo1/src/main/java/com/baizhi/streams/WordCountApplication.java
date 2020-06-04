package com.baizhi.streams;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Properties;

/**
 *  通过kafka streams api 实时统计单词出现的次数
 */
public class WordCountApplication {

    public static void main(String[] args) throws InterruptedException {

        // 1. 配置对象
        Properties prop = new Properties();
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.79.31:9092,192.168.79.32:9092,192.168.79.33:9092");
        // Key & Value 默认的序列化和反序列化器
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount"); // 应用名 以后作为消费组的标识
        prop.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);

        // 2. 拓扑任务对象
        Topology topology = new Topology();
        // 添加数据源 name = source别名
        topology.addSource( "s1","t7");
        // 添加处理器 name = 处理器的别名  parentNames = 父组件名     s1 ---->  p1
        topology.addProcessor("p1", () -> new WordCountProcessor(), "s1");

        // 当前处理器的处理结果  需要进行状态管理
        StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("Counts"),
                Serdes.String(),
                Serdes.Long()
        ).withLoggingDisabled();//  disabled 表示只进行local state store
                               //  enabled  表示使用local state store + remote state store(changelog)
        topology.addStateStore( storeBuilder, "p1");

        // 保存计算结果  s1 -------> p1 --------> k1
        topology.addSink("k1", "t8", new StringSerializer(),new LongSerializer(),"p1");
        // 3. 流数据处理的对象
        KafkaStreams streams = new KafkaStreams( topology ,prop);
        // 4. 启动流处理应用
        streams.start();
    }

}
