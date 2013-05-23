/**
 * 
 */
package com.alibaba.rocketmq.research.gson;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public abstract class Convert {
    public String encode() {
        GsonBuilder builder = new GsonBuilder();
        Gson gson = builder.create();
        return gson.toJson(this);
    }


    public static <T> T decode(String json, Class<T> classOfT) {
        GsonBuilder builder = new GsonBuilder();
        Gson gson = builder.create();
        return gson.fromJson(json, classOfT);
    }
}
