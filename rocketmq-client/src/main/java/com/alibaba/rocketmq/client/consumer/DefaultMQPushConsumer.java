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
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * 类似于Broker Push消息到Consumer方式，但实际仍然是Consumer内部后台从Broker Pull消息<br>
 * 采用长轮询方式拉消息，实时性同push方式一致，且不会无谓的拉消息导致Broker、Consumer压力增大
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-24
 */
public class DefaultMQPushConsumer extends ClientConfig implements MQPushConsumer {
    protected final transient DefaultMQPushConsumerImpl defaultMQPushConsumerImpl =
            new DefaultMQPushConsumerImpl(this);
    /**
     * 做同样事情的Consumer归为同一个Group，应用必须设置，并保证命名唯一
     */
    private String consumerGroup = MixAll.DEFAULT_CONSUMER_GROUP;
    /**
     * 集群消费/广播消费
     */
    private MessageModel messageModel = MessageModel.CLUSTERING;
    /**
     * Consumer第一次启动时，从哪里开始消费
     */
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
    /**
     * Consumer第一次启动时，如果回溯消费，默认回溯到哪个时间点，数据格式如下，时间精度秒：<br>
     * 20131223171201<br>
     * 表示2013年12月23日17点12分01秒<br>
     * 默认回溯到相对启动时间的半小时前
     */
    private String consumeTimestamp = UtilAll.timeMillisToHumanString3(System.currentTimeMillis()
            - (1000 * 60 * 30));
    /**
     * 队列分配算法，应用可重写
     */
    private AllocateMessageQueueStrategy allocateMessageQueueStrategy = new AllocateMessageQueueAveragely();
    /**
     * 订阅关系
     */
    private Map<String /* topic */, String /* sub expression */> subscription = new HashMap<String, String>();
    /**
     * 消息监听器
     */
    private MessageListener messageListener;
    /**
     * Offset存储，系统会根据客户端配置自动创建相应的实现，如果应用配置了，则以应用配置的为主
     */
    private OffsetStore offsetStore;
    /**
     * 消费消息线程，最小数目
     */
    private int consumeThreadMin = 20;
    /**
     * 消费消息线程，最大数目
     */
    private int consumeThreadMax = 20;
    /**
     * 同一队列并行消费的最大跨度，顺序消费方式情况下，此参数无效
     */
    private int consumeConcurrentlyMaxSpan = 2000;
    /**
     * 本地队列消息数超过此阀值，开始流控
     */
    private int pullThresholdForQueue = 1000;
    /**
     * 拉消息间隔，如果为了降低拉取速度，可以设置大于0的值
     */
    private long pullInterval = 0;
    /**
     * 消费一批消息，最大数
     */
    private int consumeMessageBatchMaxSize = 1;
    /**
     * 拉消息，一次拉多少条
     */
    private int pullBatchSize = 32;

    /**
     * 是否每次拉消息时，都上传订阅关系
     */
    private boolean postSubscriptionWhenPull = false;

    /**
     * 是否为单元化的订阅组
     */
    private boolean isUnitMode = false;


    public DefaultMQPushConsumer() {

    }


    public DefaultMQPushConsumer(final String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }


    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        this.defaultMQPushConsumerImpl.createTopic(key, newTopic, queueNum);
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
        this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel);
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
    public void registerMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }


    @Override
    public void subscribe(String topic, String subExpression) throws MQClientException {
        this.defaultMQPushConsumerImpl.subscribe(topic, subExpression);
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
		return isUnitMode;
	}

	public void setUnitMode(boolean isUnitMode) {
		this.isUnitMode = isUnitMode;
	}
}
