package org.apache.rocketmq.hook;

public interface AbstractKeyGenerator {
    public String genKey(String appName, String topic) ;
}
