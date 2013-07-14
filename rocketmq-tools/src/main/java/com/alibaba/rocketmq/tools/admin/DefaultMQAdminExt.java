package com.alibaba.rocketmq.tools.admin;

import com.alibaba.rocketmq.client.ClientConfig;
import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.admin.ConsumerProgress;
import com.alibaba.rocketmq.common.admin.TopicOffsetTable;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-14
 */
public class DefaultMQAdminExt extends ClientConfig implements MQAdminExt {

    private final DefaultMQAdminExtImpl defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(this);


    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        defaultMQAdminExtImpl.createTopic(key, newTopic, queueNum);
    }


    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return defaultMQAdminExtImpl.searchOffset(mq, timestamp);
    }


    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return defaultMQAdminExtImpl.maxOffset(mq);
    }


    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return defaultMQAdminExtImpl.minOffset(mq);
    }


    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return defaultMQAdminExtImpl.earliestMsgStoreTime(mq);
    }


    @Override
    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.viewMessage(msgId);
    }


    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.queryMessage(topic, key, maxNum, begin, end);
    }


    @Override
    public void start() {
        defaultMQAdminExtImpl.start();
    }


    @Override
    public void shutdown() {
        defaultMQAdminExtImpl.shutdown();
    }


    @Override
    public void createAndUpdateTopicConfigByCluster(String cluster, TopicConfig config) {
        defaultMQAdminExtImpl.createAndUpdateTopicConfigByCluster(cluster, config);
    }


    @Override
    public void createAndUpdateTopicConfigByAddr(String addr, TopicConfig config) {
        defaultMQAdminExtImpl.createAndUpdateTopicConfigByAddr(addr, config);
    }


    @Override
    public void createAndUpdateSubscriptionGroupConfigByCluster(String cluster, SubscriptionGroupConfig config) {
        defaultMQAdminExtImpl.createAndUpdateSubscriptionGroupConfigByCluster(cluster, config);
    }


    @Override
    public void createAndUpdateSubscriptionGroupConfigByAddr(String addr, SubscriptionGroupConfig config) {
        defaultMQAdminExtImpl.createAndUpdateSubscriptionGroupConfigByAddr(addr, config);
    }


    @Override
    public SubscriptionGroupConfig examineSubscriptionGroupConfig(String addr, String group) {
        return defaultMQAdminExtImpl.examineSubscriptionGroupConfig(addr, group);
    }


    @Override
    public TopicOffsetTable examineTopicOffset(String topic) {
        return defaultMQAdminExtImpl.examineTopicOffset(topic);
    }


    @Override
    public ConsumerProgress examineConsumerProgress(String consumerGroup, String topic) {
        return defaultMQAdminExtImpl.examineConsumerProgress(consumerGroup, topic);
    }


    @Override
    public void putKVConfig(String namespace, String key, String value) {
        defaultMQAdminExtImpl.putKVConfig(namespace, key, value);
    }


    @Override
    public String getKVConfig(String namespace, String key) {
        return defaultMQAdminExtImpl.getKVConfig(namespace, key);
    }


    @Override
    public TopicConfig examineTopicConfig(String addr, String topic) {
        return defaultMQAdminExtImpl.examineTopicConfig(addr, topic);
    }
}
