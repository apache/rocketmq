package org.apache.rocketmq.common.utils;

import java.util.UUID;

public class RequestIdUtil {
    public static String createUniqueRequestId(){
        return UUID.randomUUID().toString();
    }
}
