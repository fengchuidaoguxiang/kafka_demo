package com.atguigu.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class MyPartitioner implements Partitioner {


    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
//        Integer integer = cluster.partitionCountForTopic(topic);
//        return key.toString().hashCode() % integer;
        //分区号
        return 0;//0号分区
    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
