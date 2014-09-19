package com.alibaba.rocketmq.broker;

import java.io.File;


public class BrokerPathConfigHelper {
    private static String brokerConfigPath = System.getProperty("user.home") + File.separator + "store"
            + File.separator + "config" + File.separator + "broker.properties";


    public static String getBrokerConfigPath() {
        return brokerConfigPath;
    }


    public static void setBrokerConfigPath(String path) {
        brokerConfigPath = path;
    }


    public static String getTopicConfigPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "topics.json";
    }


    public static String getConsumerOffsetPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "consumerOffset.json";
    }


    public static String getSubscriptionGroupPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "subscriptionGroup.json";
    }

}
