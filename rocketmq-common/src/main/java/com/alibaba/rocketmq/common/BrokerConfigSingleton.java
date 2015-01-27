package com.alibaba.rocketmq.common;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * User: yubao.fyb
 * Date: 14/12/15
 * Time: 13:35
 */
public class BrokerConfigSingleton {
    private static AtomicBoolean isInit = new AtomicBoolean();
    private static BrokerConfig brokerConfig;


    public static void setBrokerConfig(BrokerConfig brokerConfig) {
        if (!isInit.compareAndSet(false, true)) {
            throw new IllegalArgumentException("已经初始化过了");
        }
        BrokerConfigSingleton.brokerConfig = brokerConfig;
    }

    public static BrokerConfig getBrokerConfig() {
        if (brokerConfig == null) {
            throw new IllegalArgumentException("没有设置值");
        }
        return brokerConfig;
    }
}
