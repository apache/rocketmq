package com.alibaba.rocketmq.remoting.protocol;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


/**
 * 复杂对象的序列化，利用gson来实现
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public abstract class RemotingSerializable {
    protected final static GsonBuilder builder = new GsonBuilder();
    protected final static Gson gson = builder.create();


    public String toJson() {
        return gson.toJson(this);
    }


    public static <T> T fromJson(String json, Class<T> classOfT) {
        return gson.fromJson(json, classOfT);
    }


    public byte[] encode() {
        final String json = this.toJson();
        if (json != null) {
            return json.getBytes();
        }
        return null;
    }


    public static <T> T decode(final byte[] data, Class<T> classOfT) {
        final String json = new String(data);
        return gson.fromJson(json, classOfT);
    }
}
