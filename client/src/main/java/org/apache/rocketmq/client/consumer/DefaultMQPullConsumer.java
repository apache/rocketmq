/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.consumer;

import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * Default pulling consumer
 */
public class DefaultMQPullConsumer extends ClientConfig implements MQPullConsumer {
    protected final transient DefaultMQPullConsumerImpl defaultMQPullConsumerImpl;

    /**
     * Do the same thing for the same Group, the application must be set,and
     * guarantee Globally unique
     */
    private String consumerGroup;
    /**
     * Long polling mode, the Consumer connection max suspend time, it is not
     * recommended to modify
     */
    private long brokerSuspendMaxTimeMillis = 1000 * 20;
    /**
     * Long polling mode, the Consumer connection timeout(must greater than
     * brokerSuspendMaxTimeMillis), it is not recommended to modify
     */
    private long consumerTimeoutMillisWhenSuspend = 1000 * 30;
    /**
     * The socket timeout in milliseconds
     */
    private long consumerPullTimeoutMillis = 1000 * 10;
    /**
     * Consumption pattern,default is clustering
     */
    private MessageModel messageModel = MessageModel.CLUSTERING;
    /**
     * Message queue listener
     */
    private MessageQueueListener messageQueueListener;
    /**
     * Offset Storage
     */
    private OffsetStore offsetStore;
    /**
     * Topic set you want to register
     */
    private Set<String> registerTopics = new HashSet<String>();
    /**
     * Queue allocation algorithm
     */
    private AllocateMessageQueueStrategy allocateMessageQueueStrategy = new AllocateMessageQueueAveragely();
    /**
     * Whether the unit of subscription group
     */
    private boolean unitMode = false;

    private int maxReconsumeTimes = 16;

    public DefaultMQPullConsumer() {
        this(MixAll.DEFAULT_CONSUMER_GROUP, null);
    }

    public DefaultMQPullConsumer(final String consumerGroup, RPCHook rpcHook) {
        this.consumerGroup = consumerGroup;
        defaultMQPullConsumerImpl = new DefaultMQPullConsumerImpl(this, rpcHook);
    }

    public DefaultMQPullConsumer(final String consumerGroup) {
        this(consumerGroup, null);
    }

    public DefaultMQPullConsumer(RPCHook rpcHook) {
        this(MixAll.DEFAULT_CONSUMER_GROUP, rpcHook);
    }

    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.defaultMQPullConsumerImpl.createTopic(key, newTopic, queueNum, topicSysFlag);
    }

    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.defaultMQPullConsumerImpl.searchOffset(mq, timestamp);
    }

    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPullConsumerImpl.maxOffset(mq);
    }

    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPullConsumerImpl.minOffset(mq);
    }

    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.defaultMQPullConsumerImpl.earliestMsgStoreTime(mq);
    }

    @Override
    public MessageExt viewMessage(String offsetMsgId) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException {
        return this.defaultMQPullConsumerImpl.viewMessage(offsetMsgId);
    }

    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException {
        return this.defaultMQPullConsumerImpl.queryMessage(topic, key, maxNum, begin, end);
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public long getBrokerSuspendMaxTimeMillis() {
        return brokerSuspendMaxTimeMillis;
    }

    public void setBrokerSuspendMaxTimeMillis(long brokerSuspendMaxTimeMillis) {
        this.brokerSuspendMaxTimeMillis = brokerSuspendMaxTimeMillis;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public long getConsumerPullTimeoutMillis() {
        return consumerPullTimeoutMillis;
    }

    public void setConsumerPullTimeoutMillis(long consumerPullTimeoutMillis) {
        this.consumerPullTimeoutMillis = consumerPullTimeoutMillis;
    }

    public long getConsumerTimeoutMillisWhenSuspend() {
        return consumerTimeoutMillisWhenSuspend;
    }

    public void setConsumerTimeoutMillisWhenSuspend(long consumerTimeoutMillisWhenSuspend) {
        this.consumerTimeoutMillisWhenSuspend = consumerTimeoutMillisWhenSuspend;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public MessageQueueListener getMessageQueueListener() {
        return messageQueueListener;
    }

    public void setMessageQueueListener(MessageQueueListener messageQueueListener) {
        this.messageQueueListener = messageQueueListener;
    }

    public Set<String> getRegisterTopics() {
        return registerTopics;
    }

    public void setRegisterTopics(Set<String> registerTopics) {
        this.registerTopics = registerTopics;
    }

    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        this.defaultMQPullConsumerImpl.sendMessageBack(msg, delayLevel, null);
    }

    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel, String brokerName)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        this.defaultMQPullConsumerImpl.sendMessageBack(msg, delayLevel, brokerName);
    }

    @Override
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        return this.defaultMQPullConsumerImpl.fetchSubscribeMessageQueues(topic);
    }

    @Override
    public void start() throws MQClientException {
        this.defaultMQPullConsumerImpl.start();
    }

    @Override
    public void shutdown() {
        this.defaultMQPullConsumerImpl.shutdown();
    }

    @Override
    public void registerMessageQueueListener(String topic, MessageQueueListener listener) {
        synchronized (this.registerTopics) {
            this.registerTopics.add(topic);
            if (listener != null) {
                this.messageQueueListener = listener;
            }
        }
    }

    @Override
    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.pull(mq, subExpression, offset, maxNums);
    }

    @Override
    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.pull(mq, subExpression, offset, maxNums, timeout);
    }

    @Override
    public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback)
        throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQPullConsumerImpl.pull(mq, subExpression, offset, maxNums, pullCallback);
    }

    @Override
    public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback,
        long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQPullConsumerImpl.pull(mq, subExpression, offset, maxNums, pullCallback, timeout);
    }

    @Override
    public PullResult pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.pullBlockIfNotFound(mq, subExpression, offset, maxNums);
    }

    @Override
    public void pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums,
        PullCallback pullCallback)
        throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQPullConsumerImpl.pullBlockIfNotFound(mq, subExpression, offset, maxNums, pullCallback);
    }

    @Override
    public void updateConsumeOffset(MessageQueue mq, long offset) throws MQClientException {
        this.defaultMQPullConsumerImpl.updateConsumeOffset(mq, offset);
    }

    @Override
    public long fetchConsumeOffset(MessageQueue mq, boolean fromStore) throws MQClientException {
        return this.defaultMQPullConsumerImpl.fetchConsumeOffset(mq, fromStore);
    }

    @Override
    public Set<MessageQueue> fetchMessageQueuesInBalance(String topic) throws MQClientException {
        return this.defaultMQPullConsumerImpl.fetchMessageQueuesInBalance(topic);
    }

    @Override
    public MessageExt viewMessage(String topic,
        String uniqKey) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            MessageDecoder.decodeMessageId(uniqKey);
            return this.viewMessage(uniqKey);
        } catch (Exception e) {
            // Ignore
        }
        return this.defaultMQPullConsumerImpl.queryMessageByUniqKey(topic, uniqKey);
    }

    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel, String brokerName, String consumerGroup)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        this.defaultMQPullConsumerImpl.sendMessageBack(msg, delayLevel, brokerName, consumerGroup);
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    public DefaultMQPullConsumerImpl getDefaultMQPullConsumerImpl() {
        return defaultMQPullConsumerImpl;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }

    public int getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    public void setMaxReconsumeTimes(final int maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
    }
}
