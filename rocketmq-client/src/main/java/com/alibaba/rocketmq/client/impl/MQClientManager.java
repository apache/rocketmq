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
    private ConcurrentHashMap<String/* clientId */, MQClientFactory> factoryTable =
            new ConcurrentHashMap<String, MQClientFactory>();


    private MQClientManager() {

    }


    public static MQClientManager getInstance() {
        return instance;
    }


    public MQClientFactory getAndCreateMQClientFactory(final ClientConfig clientConfig) {
        String clientId = clientConfig.buildMQClientId();
        MQClientFactory factory = this.factoryTable.get(clientId);
        if (null == factory) {
            factory =
                    new MQClientFactory(clientConfig.cloneClientConfig(),
                        this.factoryIndexGenerator.getAndIncrement(), clientId);
            MQClientFactory prev = this.factoryTable.putIfAbsent(clientId, factory);
            if (prev != null) {
                factory = prev;
            }
            else {
                // TODO log
            }
        }

        return factory;
    }


    public void removeClientFactory(final String clientId) {
        this.factoryTable.remove(clientId);
    }
}
