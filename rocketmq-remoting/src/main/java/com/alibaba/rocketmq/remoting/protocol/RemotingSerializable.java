package com.alibaba.rocketmq.remoting.protocol;

import com.alibaba.fastjson.JSON;


/**
 * 复杂对象的序列化，利用json来实现
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public abstract class RemotingSerializable {
    public String toJson() {
        return toJson(this);
    }


    public static String toJson(final Object obj) {
        return JSON.toJSONString(obj);
    }


    public static <T> T fromJson(String json, Class<T> classOfT) {
        return JSON.parseObject(json, classOfT);
    }


    public byte[] encode() {
        final String json = this.toJson();
        if (json != null) {
            return json.getBytes();
        }
        return null;
    }


    public static byte[] encode(final Object obj) {
        final String json = toJson(obj);
        if (json != null) {
            return json.getBytes();
        }
        return null;
    }


    public static <T> T decode(final byte[] data, Class<T> classOfT) {
        final String json = new String(data);
        return fromJson(json, classOfT);
    }
}
