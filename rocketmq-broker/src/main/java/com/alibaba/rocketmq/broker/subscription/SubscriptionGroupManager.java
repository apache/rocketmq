package com.alibaba.rocketmq.broker.subscription;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.constant.LoggerName;


/**
 * 用来管理订阅组，包括订阅权限等
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class SubscriptionGroupManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private final BrokerController brokerController;

    // Topic配置
    private final ConcurrentHashMap<String, TopicConfig> subscriptionGroupTable =
            new ConcurrentHashMap<String, TopicConfig>(1024);


    public SubscriptionGroupManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }
}
