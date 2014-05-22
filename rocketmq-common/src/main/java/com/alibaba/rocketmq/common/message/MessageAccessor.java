package com.alibaba.rocketmq.common.message;

import java.util.Map;


public class MessageAccessor {

    public static void putProperty(final Message msg, final String name, final String value) {
        msg.putProperty(name, value);
    }


    public static void clearProperty(final Message msg, final String name) {
        msg.clearProperty(name);
    }


    public static void setProperties(final Message msg, Map<String, String> properties) {
        msg.setProperties(properties);
    }
}
