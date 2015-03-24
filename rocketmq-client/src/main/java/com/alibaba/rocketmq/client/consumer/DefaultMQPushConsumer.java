/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.client.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.alibaba.rocketmq.client.ClientConfig;
import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * Wrapped push consumer.in fact,it works as remarkable as the pull consumer
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-24
 */
public class DefaultMQPushConsumer extends ClientConfig implements MQPushConsumer {
    protected final transient DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    /**
     * Do the same thing for the same Group, the application must be set,and
     * guarantee Globally unique
     */
    private String consumerGroup;
    /**
     * Consumption pattern,default is clustering
     */
    private MessageModel messageModel = MessageModel.CLUSTERING;
    /**
     * Consumption offset
     */
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
    /**
     * Backtracking consumption time with second precision.time format is
     * 20131223171201<br>
     * Implying Seventeen twelve and 01 seconds on December 23, 2013 year<br>
     * Default backtracking consumption time Half an hour ago
     */
    private String consumeTimestamp = UtilAll.timeMillisToHumanString3(System.currentTimeMillis()
            - (1000 * 60 * 30));
    /**
     * Queue allocation algorithm
     */
    private AllocateMessageQueueStrategy allocateMessageQueueStrategy;

    /**
     * Subscription relationship
     */
    private Map<String /* topic */, String /* sub expression */> subscription = new HashMap<String, String>();
    /**
     * Message listener
     */
    private MessageListener messageListener;
    /**
     * Offset Storage
     */
    private OffsetStore offsetStore;
    /**
     * Minimum consumer thread number
     */
    private int consumeThreadMin = 20;
    /**
     * Max consumer thread number
     */
    private int consumeThreadMax = 64;

    /**
     * Threshold for dynamic adjustment of the number of thread pool
     */
    private long adjustThreadPoolNumsThreshold = 100000;

    /**
     * Concurrently max span offset.it has no effect on sequential consumption
     */
    private int consumeConcurrentlyMaxSpan = 2000;
    /**
     * Flow control threshold
     */
    private int pullThresholdForQueue = 1000;
    /**
     * Message pull Interval
     */
    private long pullInterval = 0;
    /**
     * Batch consumption size
     */
    private int consumeMessageBatchMaxSize = 1;
    /**
     * Batch pull size
     */
    private int pullBatchSize = 32;

    /**
     * Whether update subscription relationship when every pull
     */
    private boolean postSubscriptionWhenPull = false;

    /**
     * Whether the unit of subscription group
     */
    private boolean unitMode = false;


    public DefaultMQPushConsumer() {
        this(MixAll.DEFAULT_CONSUMER_GROUP, null, new AllocateMessageQueueAveragely());
    }


    public DefaultMQPushConsumer(RPCHook rpcHook) {
        this(MixAll.DEFAULT_CONSUMER_GROUP, rpcHook, new AllocateMessageQueueAveragely());
    }


    public DefaultMQPushConsumer(final String consumerGroup) {
        this(consumerGroup, null, new AllocateMessageQueueAveragely());
    }


    public DefaultMQPushConsumer(final String consumerGroup, RPCHook rpcHook,
            AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.consumerGroup = consumerGroup;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(this, rpcHook);
    }


    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }


    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag)
            throws MQClientException {
        this.defaultMQPushConsumerImpl.createTopic(key, newTopic, queueNum, topicSysFlag);
    }


    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.defaultMQPushConsumerImpl.searchOffset(mq, timestamp);
    }


    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPushConsumerImpl.maxOffset(mq);
    }


    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPushConsumerImpl.minOffset(mq);
    }


    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.defaultMQPushConsumerImpl.earliestMsgStoreTime(mq);
    }


    @Override
    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        return this.defaultMQPushConsumerImpl.viewMessage(msgId);
    }


    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        return this.defaultMQPushConsumerImpl.queryMessage(topic, key, maxNum, begin, end);
    }


    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }


    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }


    public int getConsumeConcurrentlyMaxSpan() {
        return consumeConcurrentlyMaxSpan;
    }


    public void setConsumeConcurrentlyMaxSpan(int consumeConcurrentlyMaxSpan) {
        this.consumeConcurrentlyMaxSpan = consumeConcurrentlyMaxSpan;
    }


    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }


    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }


    public int getConsumeMessageBatchMaxSize() {
        return consumeMessageBatchMaxSize;
    }


    public void setConsumeMessageBatchMaxSize(int consumeMessageBatchMaxSize) {
        this.consumeMessageBatchMaxSize = consumeMessageBatchMaxSize;
    }


    public String getConsumerGroup() {
        return consumerGroup;
    }


    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }


    public int getConsumeThreadMax() {
        return consumeThreadMax;
    }


    public void setConsumeThreadMax(int consumeThreadMax) {
        this.consumeThreadMax = consumeThreadMax;
    }


    public int getConsumeThreadMin() {
        return consumeThreadMin;
    }


    public void setConsumeThreadMin(int consumeThreadMin) {
        this.consumeThreadMin = consumeThreadMin;
    }


    public DefaultMQPushConsumerImpl getDefaultMQPushConsumerImpl() {
        return defaultMQPushConsumerImpl;
    }


    public MessageListener getMessageListener() {
        return messageListener;
    }


    public void setMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
    }


    public MessageModel getMessageModel() {
        return messageModel;
    }


    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }


    public int getPullBatchSize() {
        return pullBatchSize;
    }


    public void setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
    }


    public long getPullInterval() {
        return pullInterval;
    }


    public void setPullInterval(long pullInterval) {
        this.pullInterval = pullInterval;
    }


    public int getPullThresholdForQueue() {
        return pullThresholdForQueue;
    }


    public void setPullThresholdForQueue(int pullThresholdForQueue) {
        this.pullThresholdForQueue = pullThresholdForQueue;
    }


    public Map<String, String> getSubscription() {
        return subscription;
    }


    public void setSubscription(Map<String, String> subscription) {
        this.subscription = subscription;
    }


    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, null);
    }


    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel, String brokerName) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, brokerName);
    }


    @Override
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        return this.defaultMQPushConsumerImpl.fetchSubscribeMessageQueues(topic);
    }


    @Override
    public void start() throws MQClientException {
        this.defaultMQPushConsumerImpl.start();
    }


    @Override
    public void shutdown() {
        this.defaultMQPushConsumerImpl.shutdown();
    }


    @Override
    public void registerMessageListener(MessageListenerConcurrently messageListener) {
        this.messageListener = messageListener;
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }


    @Override
    public void registerMessageListener(MessageListenerOrderly messageListener) {
        this.messageListener = messageListener;
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }


    @Override
    public void subscribe(String topic, String subExpression) throws MQClientException {
        this.defaultMQPushConsumerImpl.subscribe(topic, subExpression);
    }


    @Override
    public void subscribe(String topic, String fullClassName, String filterClassSource)
            throws MQClientException {
        this.defaultMQPushConsumerImpl.subscribe(topic, fullClassName, filterClassSource);
    }


    @Override
    public void unsubscribe(String topic) {
        this.defaultMQPushConsumerImpl.unsubscribe(topic);
    }


    @Override
    public void updateCorePoolSize(int corePoolSize) {
        this.defaultMQPushConsumerImpl.updateCorePoolSize(corePoolSize);
    }


    @Override
    public void suspend() {
        this.defaultMQPushConsumerImpl.suspend();
    }


    @Override
    public void resume() {
        this.defaultMQPushConsumerImpl.resume();
    }


    public OffsetStore getOffsetStore() {
        return offsetStore;
    }


    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }


    public String getConsumeTimestamp() {
        return consumeTimestamp;
    }


    public void setConsumeTimestamp(String consumeTimestamp) {
        this.consumeTimestamp = consumeTimestamp;
    }


    public boolean isPostSubscriptionWhenPull() {
        return postSubscriptionWhenPull;
    }


    public void setPostSubscriptionWhenPull(boolean postSubscriptionWhenPull) {
        this.postSubscriptionWhenPull = postSubscriptionWhenPull;
    }


    public boolean isUnitMode() {
        return unitMode;
    }


    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }


    public long getAdjustThreadPoolNumsThreshold() {
        return adjustThreadPoolNumsThreshold;
    }


    public void setAdjustThreadPoolNumsThreshold(long adjustThreadPoolNumsThreshold) {
        this.adjustThreadPoolNumsThreshold = adjustThreadPoolNumsThreshold;
    }
}
