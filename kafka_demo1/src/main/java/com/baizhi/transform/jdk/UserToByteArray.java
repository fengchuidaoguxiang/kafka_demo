package com.baizhi.transform.jdk;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * 序列化器
 */
public class UserToByteArray implements Serializer<User> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    /**
     * 序列化方法 User ----> Bytes
     *
     * @param topic
     * @param user
     * @return
     */
    @Override
    public byte[] serialize(String topic, User user) {
        return SerializationUtils.serialize(user);
    }

    @Override
    public void close() {

    }
}
