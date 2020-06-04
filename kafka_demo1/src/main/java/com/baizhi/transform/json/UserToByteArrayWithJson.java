package com.baizhi.transform.json;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * 序列化器
 */
public class UserToByteArrayWithJson implements Serializer<User> {
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
        String strJson = JSONObject.toJSONString(user);
        return strJson.getBytes();
    }

    @Override
    public void close() {

    }
}
