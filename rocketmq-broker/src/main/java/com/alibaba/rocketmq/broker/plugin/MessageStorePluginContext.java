/**
 *
 */
package com.alibaba.rocketmq.broker.plugin;

import com.alibaba.rocketmq.common.BrokerConfig;
import com.alibaba.rocketmq.store.MessageArrivingListener;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;
import com.alibaba.rocketmq.store.stats.BrokerStatsManager;

/**
 * @author qinan.qn@taobao.com
 *         2015年12月12日
 */
public class MessageStorePluginContext {
    private MessageStoreConfig messageStoreConfig;
    private BrokerStatsManager brokerStatsManager;
    private MessageArrivingListener messageArrivingListener;
    private BrokerConfig brokerConfig;

    /**
     * @param messageStoreConfig
     * @param brokerStatsManager
     * @param messageArrivingListener
     * @param brokerConfig
     */
    public MessageStorePluginContext(MessageStoreConfig messageStoreConfig,
                                     BrokerStatsManager brokerStatsManager, MessageArrivingListener messageArrivingListener,
                                     BrokerConfig brokerConfig) {
        super();
        this.messageStoreConfig = messageStoreConfig;
        this.brokerStatsManager = brokerStatsManager;
        this.messageArrivingListener = messageArrivingListener;
        this.brokerConfig = brokerConfig;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    public MessageArrivingListener getMessageArrivingListener() {
        return messageArrivingListener;
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }


}
