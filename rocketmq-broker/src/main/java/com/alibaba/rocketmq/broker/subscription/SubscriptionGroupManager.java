package com.alibaba.rocketmq.broker.subscription;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.logger.LoggerName;


/**
 * 用来管理订阅组，包括订阅权限等
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class SubscriptionGroupManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    // Topic配置
    private final ConcurrentHashMap<String, TopicConfig> topicConfigTable =
            new ConcurrentHashMap<String, TopicConfig>(1024);

}
