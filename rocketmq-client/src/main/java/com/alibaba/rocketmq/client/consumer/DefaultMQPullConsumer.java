/**
 * $Id: DefaultMQPullConsumer.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.consumer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.alibaba.rocketmq.client.ClientConfig;
import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicFilterType;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * 消息消费者，主动消费
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class DefaultMQPullConsumer extends ClientConfig implements MQPullConsumer {
    /**
     * 做同样事情的Consumer归为同一个Group，应用必须设置，并保证命名唯一
     */
    private String consumerGroup = MixAll.DEFAULT_CONSUMER_GROUP;
    /**
     * 长轮询模式，Consumer连接在Broker挂起最长时间，不建议修改
     */
    private long brokerSuspendMaxTimeMillis = 1000 * 20;
    /**
     * 长轮询模式，Consumer超时时间（必须要大于brokerSuspendMaxTimeMillis），不建议修改
     */
    private long consumerTimeoutMillisWhenSuspend = 1000 * 30;
    /**
     * 非阻塞拉模式，Consumer超时时间，不建议修改
     */
    private long consumerPullTimeoutMillis = 1000 * 10;
    /**
     * 集群消费/广播消费
     */
    private MessageModel messageModel = MessageModel.BROADCASTING;
    /**
     * Consumer从Master还是Slave拉消息
     */
    private ConsumeFromWhichNode consumeFromWhichNode = ConsumeFromWhichNode.CONSUME_FROM_MASTER_FIRST;
    /**
     * 队列变化监听器
     */
    private MessageQueueListener messageQueueListener;
    /**
     * 需要监听哪些Topic的队列变化
     */
    private Set<String> registerTopics = new HashSet<String>();

    private final transient DefaultMQPullConsumerImpl defaultMQPullConsumerImpl = new DefaultMQPullConsumerImpl(
        this);


    public DefaultMQPullConsumer() {
    }


    public DefaultMQPullConsumer(final String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }


    @Override
    public void createTopic(String key, String newTopic, int queueNum, TopicFilterType topicFilterType,
            boolean order) throws MQClientException {
        this.defaultMQPullConsumerImpl.createTopic(key, newTopic, queueNum, topicFilterType, order);
    }


    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.defaultMQPullConsumerImpl.searchOffset(mq, timestamp);
    }


    @Override
    public long getMaxOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPullConsumerImpl.getMaxOffset(mq);
    }


    @Override
    public long getMinOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPullConsumerImpl.getMinOffset(mq);
    }


    @Override
    public long getEarliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.defaultMQPullConsumerImpl.getEarliestMsgStoreTime(mq);
    }


    @Override
    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException, InterruptedException,
            MQClientException {
        return this.defaultMQPullConsumerImpl.viewMessage(msgId);
    }


    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        return this.defaultMQPullConsumerImpl.queryMessage(topic, key, maxNum, begin, end);
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
    public PullResult pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.pullBlockIfNotFound(mq, subExpression, offset, maxNums);
    }


    @Override
    public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQPullConsumerImpl.pull(mq, subExpression, offset, maxNums, pullCallback);
    }


    @Override
    public void pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums,
            PullCallback pullCallback) throws MQClientException, RemotingException, InterruptedException {
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
    public Set<MessageQueue> fetchMessageQueuesInBalance(String topic) {
        return this.defaultMQPullConsumerImpl.fetchMessageQueuesInBalance(topic);
    }


    @Override
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        return this.defaultMQPullConsumerImpl.fetchSubscribeMessageQueues(topic);
    }


    public long getBrokerSuspendMaxTimeMillis() {
        return brokerSuspendMaxTimeMillis;
    }


    public void setBrokerSuspendMaxTimeMillis(long brokerSuspendMaxTimeMillis) {
        this.brokerSuspendMaxTimeMillis = brokerSuspendMaxTimeMillis;
    }


    public long getConsumerTimeoutMillisWhenSuspend() {
        return consumerTimeoutMillisWhenSuspend;
    }


    public void setConsumerTimeoutMillisWhenSuspend(long consumerTimeoutMillisWhenSuspend) {
        this.consumerTimeoutMillisWhenSuspend = consumerTimeoutMillisWhenSuspend;
    }


    public long getConsumerPullTimeoutMillis() {
        return consumerPullTimeoutMillis;
    }


    public void setConsumerPullTimeoutMillis(long consumerPullTimeoutMillis) {
        this.consumerPullTimeoutMillis = consumerPullTimeoutMillis;
    }


    public String getConsumerGroup() {
        return consumerGroup;
    }


    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
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
    public void sendMessageBack(MessageExt msg, MessageQueue mq, int delayLevel) {
        // TODO
    }


    public ConsumeFromWhichNode getConsumeFromWhichNode() {
        return consumeFromWhichNode;
    }


    public void setConsumeFromWhichNode(ConsumeFromWhichNode consumeFromWhichNode) {
        this.consumeFromWhichNode = consumeFromWhichNode;
    }


    public MessageModel getMessageModel() {
        return messageModel;
    }


    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }
}
