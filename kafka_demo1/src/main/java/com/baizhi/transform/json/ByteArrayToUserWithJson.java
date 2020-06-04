package com.baizhi.transform.json;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * 反序列化器
 */
public class ByteArrayToUserWithJson implements Deserializer<User> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    /**
     * 反序列化方法   bytes ---> User
     *
     * @param topic
     * @param bytes
     * @return
     */
    @Override
    public User deserialize(String topic, byte[] bytes) {
        User user = JSONObject.parseObject(new String(bytes), User.class);
        return user;
    }

    @Override
    public void close() {

    }
}
