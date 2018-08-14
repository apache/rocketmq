package org.apache.rocketmq.hook.cache;

import java.util.Date;

public interface Cache {
    String NULL = "0x00";

    boolean exist(String key);

    void delete(String key);

    void expire(String key, long millisecond);

    void set(String key, Object value);
    <T> T  getObject(String key);

    String getString(String key);
    Integer getInt(String key);
    Long getLong(String key);
    Double getDouble(String key);
    Date getDate(String key);

    <T> T getObject(String key, Invoker<T> invoker, int second);



}