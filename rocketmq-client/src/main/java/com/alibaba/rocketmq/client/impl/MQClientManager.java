/**
 * $Id: MQClientManager.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.rocketmq.client.MQClientConfig;
import com.alibaba.rocketmq.client.impl.factory.MQClientFactory;


/**
 * Meta Clientµ¥Àý¹ÜÀí
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class MQClientManager {
    private static MQClientManager instance = new MQClientManager();

    private AtomicInteger factoryIndexGenerator = new AtomicInteger();
    private ConcurrentHashMap<MQClientConfig, MQClientFactory> factoryTable =
            new ConcurrentHashMap<MQClientConfig, MQClientFactory>();


    private MQClientManager() {

    }


    public static MQClientManager getInstance() {
        return instance;
    }


    public MQClientFactory getAndCreateMetaClientFactory(final MQClientConfig mQClientConfig) {
        MQClientFactory factory = this.factoryTable.get(mQClientConfig);
        if (null == factory) {
            factory = new MQClientFactory(mQClientConfig, this.factoryIndexGenerator.getAndIncrement());
            MQClientFactory prev = this.factoryTable.putIfAbsent(mQClientConfig, factory);
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
