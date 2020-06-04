package com.baizhi.basic;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * 管理API
 */
public class AdminDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties prop = new Properties();
        prop.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.79.31:9092,192.168.79.32:9092,192.168.79.33:9092");
        AdminClient adminClient = KafkaAdminClient.create(prop);

//        List<NewTopic> topics = Arrays.asList(new NewTopic("t4", 3, (short) 3));
        // 创建topic
//        adminClient.createTopics( topics );

        // 删除topic
//        adminClient.deleteTopics(Arrays.asList("t4"));

        // 展示所有（只展示用户创建的Topic列表）
//        ListTopicsResult topics = adminClient.listTopics();
//        KafkaFuture<Set<String>> names = topics.names();
//        Set<String> tNames = names.get();
//        tNames.forEach(name -> System.out.println(name));

        // 描述一个topic
//        DescribeTopicsResult result = adminClient.describeTopics(Arrays.asList("t1"));
//        Map<String, KafkaFuture<TopicDescription>> map = result.values();
//        map.forEach((k, v) -> {
//            try {
//                System.out.println(k + "\t" + v.get());
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (ExecutionException e) {
//                e.printStackTrace();
//            }
//        });

        adminClient.close();
    }
}
