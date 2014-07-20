package com.alibaba.rocketmq.common.protocol.body;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * Consumer内部数据结构
 */
public class ConsumerRunningInfo extends RemotingSerializable {
    public static final String PROP_NAMESERVER_ADDR = "PROP_NAMESERVER_ADDR";
    public static final String PROP_THREADPOOL_CORE_SIZE = "PROP_THREADPOOL_CORE_SIZE";
    public static final String PROP_CONSUMEORDERLY = "PROP_CONSUMEORDERLY";

    // 各种配置及运行数据
    private Properties properties = new Properties();
    // 订阅关系
    private Set<SubscriptionData> subscriptionSet = new HashSet<SubscriptionData>();
    // 消费进度、Rebalance、内部消费队列的信息
    private HashMap<MessageQueue, ProcessQueueInfo> mqTable = new HashMap<MessageQueue, ProcessQueueInfo>();


    public Properties getProperties() {
        return properties;
    }


    public void setProperties(Properties properties) {
        this.properties = properties;
    }


    public Set<SubscriptionData> getSubscriptionSet() {
        return subscriptionSet;
    }


    public void setSubscriptionSet(Set<SubscriptionData> subscriptionSet) {
        this.subscriptionSet = subscriptionSet;
    }


    public HashMap<MessageQueue, ProcessQueueInfo> getMqTable() {
        return mqTable;
    }


    public void setMqTable(HashMap<MessageQueue, ProcessQueueInfo> mqTable) {
        this.mqTable = mqTable;
    }
}
