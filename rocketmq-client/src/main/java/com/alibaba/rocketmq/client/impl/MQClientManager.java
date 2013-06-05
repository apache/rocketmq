/**
 * $Id: MQClientManager.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.rocketmq.client.ClientConfig;
import com.alibaba.rocketmq.client.impl.factory.MQClientFactory;


/**
 * Clientµ¥Àý¹ÜÀí
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class MQClientManager {
    private static MQClientManager instance = new MQClientManager();

    private AtomicInteger factoryIndexGenerator = new AtomicInteger();
    private ConcurrentHashMap<ClientConfig, MQClientFactory> factoryTable =
            new ConcurrentHashMap<ClientConfig, MQClientFactory>();


    private MQClientManager() {

    }


    public static MQClientManager getInstance() {
        return instance;
    }


    public MQClientFactory getAndCreateMQClientFactory(final ClientConfig clientConfig) {
        MQClientFactory factory = this.factoryTable.get(clientConfig);
        if (null == factory) {
            factory = new MQClientFactory(clientConfig, this.factoryIndexGenerator.getAndIncrement());
            MQClientFactory prev = this.factoryTable.putIfAbsent(clientConfig, factory);
            if (prev != null) {
                factory = prev;
            }
            else {
                // TODO log
            }
        }

        return factory;
    }
}
