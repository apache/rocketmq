package com.alibaba.rocketmq.tools.admin;

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


public class DefaultMQAdminExtImpl implements MQAdminExt {
    private final DefaultMQAdminExt defaultMQAdminExt;


    public DefaultMQAdminExtImpl(DefaultMQAdminExt defaultMQAdminExt) {
        this.defaultMQAdminExt = defaultMQAdminExt;
    }


    @Override
    public void start() {
        // TODO Auto-generated method stub

    }


    @Override
    public void shutdown() {
    }


    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        // TODO Auto-generated method stub

    }


    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public void createAndUpdateTopicConfigByCluster(String cluster, TopicConfig config) {
        // TODO Auto-generated method stub

    }


    @Override
    public void createAndUpdateTopicConfigByAddr(String addr, TopicConfig config) {
        // TODO Auto-generated method stub

    }


    @Override
    public void createAndUpdateSubscriptionGroupConfigByCluster(String cluster, SubscriptionGroupConfig config) {
        // TODO Auto-generated method stub

    }


    @Override
    public void createAndUpdateSubscriptionGroupConfigByAddr(String addr, SubscriptionGroupConfig config) {
        // TODO Auto-generated method stub

    }


    @Override
    public SubscriptionGroupConfig examineSubscriptionGroupConfig(String addr, String group) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public TopicOffsetTable examineTopicOffset(String topic) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public ConsumerProgress examineConsumerProgress(String consumerGroup, String topic) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public void putKVConfig(String namespace, String key, String value) {
        // TODO Auto-generated method stub

    }


    @Override
    public String getKVConfig(String namespace, String key) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public TopicConfig examineTopicConfig(String addr, String topic) {
        // TODO Auto-generated method stub
        return null;
    }

}
