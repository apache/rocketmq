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

import java.util.Collection;
import java.util.List;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.DefaultLitePullConsumerImpl;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.RPCHook;

public class DefaultLitePullConsumer extends ClientConfig implements LitePullConsumer {

    private final DefaultLitePullConsumerImpl defaultLitePullConsumerImpl;

    /**
     * Do the same thing for the same Group, the application must be set,and guarantee Globally unique
     */
    private String consumerGroup;

    /**
     * Long polling mode, the Consumer connection max suspend time, it is not recommended to modify
     */
    private long brokerSuspendMaxTimeMillis = 1000 * 20;

    /**
     * Long polling mode, the Consumer connection timeout(must greater than brokerSuspendMaxTimeMillis), it is not
     * recommended to modify
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
     * Queue allocation algorithm
     */
    private AllocateMessageQueueStrategy allocateMessageQueueStrategy = new AllocateMessageQueueAveragely();
    /**
     * Whether the unit of subscription group
     */
    private boolean unitMode = false;

    /**
     * The flag for auto commit offset
     */
    private boolean autoCommit = true;

    /**
     * Pull thread number
     */
    private int pullThreadNums = 20;

    /**
     * Maximum commit offset interval time in milliseconds.
     */
    private long autoCommitIntervalMillis = 5 * 1000;

    /**
     * Maximum number of messages pulled each time.
     */
    private int pullBatchSize = 10;

    /**
     * Flow control threshold for consume request, each consumer will cache at most 10000 consume requests by default.
     * Consider the {@code pullBatchSize}, the instantaneous value may exceed the limit
     */
    private long pullThresholdForAll = 10000;

    /**
     * Consume max span offset.
     */
    private int consumeMaxSpan = 2000;

    /**
     * Flow control threshold on queue level, each message queue will cache at most 1000 messages by default, Consider
     * the {@code pullBatchSize}, the instantaneous value may exceed the limit
     */
    private int pullThresholdForQueue = 1000;

    /**
     * Limit the cached message size on queue level, each message queue will cache at most 100 MiB messages by default,
     * Consider the {@code pullBatchSize}, the instantaneous value may exceed the limit
     *
     * <p>
     * The size of a message only measured by message body, so it's not accurate
     */
    private int pullThresholdSizeForQueue = 100;

    /**
     * The poll timeout in milliseconds
     */
    private long pollTimeoutMillis = 1000 * 5;

    /**
     * Default constructor.
     */
    public DefaultLitePullConsumer() {
        this(null, MixAll.DEFAULT_CONSUMER_GROUP, null);
    }

    /**
     * Constructor specifying consumer group.
     *
     * @param consumerGroup Consumer group.
     */
    public DefaultLitePullConsumer(final String consumerGroup) {
        this(null, consumerGroup, null);
    }

    /**
     * Constructor specifying RPC hook.
     *
     * @param rpcHook RPC hook to execute before each remoting command.
     */
    public DefaultLitePullConsumer(RPCHook rpcHook) {
        this(null, MixAll.DEFAULT_CONSUMER_GROUP, rpcHook);
    }

    /**
     * Constructor specifying consumer group, RPC hook
     *
     * @param consumerGroup Consumer group.
     * @param rpcHook RPC hook to execute before each remoting command.
     */
    public DefaultLitePullConsumer(final String consumerGroup, RPCHook rpcHook) {
        this(null, consumerGroup, rpcHook);
    }

    /**
     * Constructor specifying namespace, consumer group and RPC hook.
     *
     * @param consumerGroup Consumer group.
     * @param rpcHook RPC hook to execute before each remoting command.
     */
    public DefaultLitePullConsumer(final String namespace, final String consumerGroup, RPCHook rpcHook) {
        this.namespace = namespace;
        this.consumerGroup = consumerGroup;
        defaultLitePullConsumerImpl = new DefaultLitePullConsumerImpl(this, rpcHook);
    }

    /**
     * Start the consumer
     */
    @Override
    public void start() throws MQClientException {
        this.defaultLitePullConsumerImpl.start();
    }

    /**
     * Shutdown the consumer
     */
    @Override
    public void shutdown() {
        this.defaultLitePullConsumerImpl.shutdown();
    }

    /**
     * Subscribe some topic with subExpression
     *
     * @param subExpression subscription expression.it only support or operation such as "tag1 || tag2 || tag3" <br> if
     * null or * expression,meaning subscribe all
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void subscribe(String topic, String subExpression) throws MQClientException {
        this.defaultLitePullConsumerImpl.subscribe(withNamespace(topic), subExpression);
    }

    /**
     * Subscribe some topic with selector.
     *
     * @param messageSelector message selector({@link MessageSelector}), can be null.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void subscribe(String topic, MessageSelector messageSelector) throws MQClientException {
        this.defaultLitePullConsumerImpl.subscribe(withNamespace(topic), messageSelector);
    }

    /**
     * Unsubscribe consumption some topic
     *
     * @param topic Message topic that needs to be unsubscribe.
     */
    @Override
    public void unsubscribe(String topic) {
        this.defaultLitePullConsumerImpl.unsubscribe(withNamespace(topic));
    }

    /**
     * Manually assign a list of message queues to this consumer. This interface does not allow for incremental
     * assignment and will replace the previous assignment (if there is one).
     *
     * @param messageQueues Message queues that needs to be assigned.
     */
    @Override
    public void assign(Collection<MessageQueue> messageQueues) {
        defaultLitePullConsumerImpl.assign(queuesWithNamespace(messageQueues));
    }

    /**
     * Fetch data for the topics or partitions specified using assign API
     *
     * @return list of message, can be null.
     */
    @Override
    public List<MessageExt> poll() {
        return defaultLitePullConsumerImpl.poll(this.getPollTimeoutMillis());
    }

    /**
     * Fetch data for the topics or partitions specified using assign API
     *
     * @param timeout The amount time, in milliseconds, spent waiting in poll if data is not available. Must not be
     * negative
     * @return list of message, can be null.
     */
    @Override
    public List<MessageExt> poll(long timeout) {
        return defaultLitePullConsumerImpl.poll(timeout);
    }

    /**
     * Overrides the fetch offsets that the consumer will use on the next poll. If this API is invoked for the same
     * message queue more than once, the latest offset will be used on the next poll(). Note that you may lose data if
     * this API is arbitrarily used in the middle of consumption.
     *
     * @param messageQueue
     * @param offset
     */
    @Override
    public void seek(MessageQueue messageQueue, long offset) throws MQClientException {
        this.defaultLitePullConsumerImpl.seek(queueWithNamespace(messageQueue), offset);
    }

    /**
     * Suspend pulling from the requested message queues.
     *
     * Because of the implementation of pre-pull, fetch data in {@link #poll()} will not stop immediately until the
     * messages of the requested message queues drain.
     *
     * Note that this method does not affect message queue subscription. In particular, it does not cause a group
     * rebalance.
     *
     * @param messageQueues Message queues that needs to be paused.
     */
    @Override
    public void pause(Collection<MessageQueue> messageQueues) {
        this.defaultLitePullConsumerImpl.pause(queuesWithNamespace(messageQueues));
    }

    /**
     * Resume specified message queues which have been paused with {@link #pause(Collection)}.
     *
     * @param messageQueues Message queues that needs to be resumed.
     */
    @Override
    public void resume(Collection<MessageQueue> messageQueues) {
        this.defaultLitePullConsumerImpl.resume(queuesWithNamespace(messageQueues));
    }

    /**
     * Get metadata about the message queues for a given topic.
     *
     * @param topic The topic that need to get metadata.
     * @return collection of message queues
     * @throws MQClientException if there is any client error.
     */
    @Override
    public Collection<MessageQueue> fetchMessageQueues(String topic) throws MQClientException {
        return this.defaultLitePullConsumerImpl.fetchMessageQueues(withNamespace(topic));
    }

    /**
     * Look up the offsets for the given message queue by timestamp. The returned offset for each message queue is the
     * earliest offset whose timestamp is greater than or equal to the given timestamp in the corresponding message
     * queue.
     *
     * @param messageQueue Message queues that needs to get offset by timestamp.
     * @param timestamp
     * @return offset
     * @throws MQClientException if there is any client error.
     */
    @Override
    public Long offsetForTimestamp(MessageQueue messageQueue, Long timestamp) throws MQClientException {
        return this.defaultLitePullConsumerImpl.searchOffset(queueWithNamespace(messageQueue), timestamp);
    }

    /**
     * Register a callback for sensing topic metadata changes.
     *
     * @param topic The topic that need to monitor.
     * @param topicMessageQueueChangeListener Callback when topic metadata changes.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void registerTopicMessageQueueChangeListener(String topic,
        TopicMessageQueueChangeListener topicMessageQueueChangeListener) throws MQClientException {
        this.defaultLitePullConsumerImpl.registerTopicMessageQueueChangeListener(withNamespace(topic), topicMessageQueueChangeListener);
    }

    /**
     * Manually commit consume offset.
     */
    @Override
    public void commitSync() {
        this.defaultLitePullConsumerImpl.commitSync();
    }

    /**
     * Get the last committed offset for the given message queue.
     *
     * @param messageQueue
     * @return offset, if offset equals -1 means no offset in broker.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public Long committed(MessageQueue messageQueue) throws MQClientException {
        return this.defaultLitePullConsumerImpl.committed(messageQueue);
    }

    /**
     * Whether to enable auto-commit consume offset.
     *
     * @return true if enable auto-commit, false if disable auto-commit.
     */
    @Override
    public boolean isAutoCommit() {
        return autoCommit;
    }

    /**
     * Set whether to enable auto-commit consume offset.
     *
     * @param autoCommit Whether to enable auto-commit.
     */
    @Override
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public int getPullThreadNums() {
        return pullThreadNums;
    }

    public void setPullThreadNums(int pullThreadNums) {
        this.pullThreadNums = pullThreadNums;
    }

    public long getAutoCommitIntervalMillis() {
        return autoCommitIntervalMillis;
    }

    public void setAutoCommitIntervalMillis(long autoCommitIntervalMillis) {
        this.autoCommitIntervalMillis = autoCommitIntervalMillis;
    }

    public int getPullBatchNums() {
        return pullBatchSize;
    }

    public void setPullBatchNums(int pullBatchNums) {
        this.pullBatchSize = pullBatchNums;
    }

    public long getPullThresholdForAll() {
        return pullThresholdForAll;
    }

    public void setPullThresholdForAll(long pullThresholdForAll) {
        this.pullThresholdForAll = pullThresholdForAll;
    }

    public int getConsumeMaxSpan() {
        return consumeMaxSpan;
    }

    public void setConsumeMaxSpan(int consumeMaxSpan) {
        this.consumeMaxSpan = consumeMaxSpan;
    }

    public int getPullThresholdForQueue() {
        return pullThresholdForQueue;
    }

    public void setPullThresholdForQueue(int pullThresholdForQueue) {
        this.pullThresholdForQueue = pullThresholdForQueue;
    }

    public int getPullThresholdSizeForQueue() {
        return pullThresholdSizeForQueue;
    }

    public void setPullThresholdSizeForQueue(int pullThresholdSizeForQueue) {
        this.pullThresholdSizeForQueue = pullThresholdSizeForQueue;
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

    public long getPollTimeoutMillis() {
        return pollTimeoutMillis;
    }

    public void setPollTimeoutMillis(long pollTimeoutMillis) {
        this.pollTimeoutMillis = pollTimeoutMillis;
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public MessageQueueListener getMessageQueueListener() {
        return messageQueueListener;
    }

    public void setMessageQueueListener(MessageQueueListener messageQueueListener) {
        this.messageQueueListener = messageQueueListener;
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
}
