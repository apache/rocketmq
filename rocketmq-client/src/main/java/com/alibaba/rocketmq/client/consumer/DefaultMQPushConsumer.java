/**
 * $Id: DefaultMQPushConsumer.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.rocketmq.client.ClientConfig;
import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.consumer.loadbalance.AllocateMessageQueueAveragely;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicFilterType;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * 类似于Broker Push消息到Consumer方式，但实际仍然是Consumer内部后台从Broker Pull消息<br>
 * 采用长轮询方式拉消息，实时性同push方式一致，且不会无谓的拉消息导致Broker压力增大
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class DefaultMQPushConsumer implements MQPushConsumer {
    /**
     * 客户端公共配置，不建议修改
     */
    private ClientConfig clientConfig = new ClientConfig();
    /**
     * 做同样事情的Consumer归为同一个Group，应用必须设置，并保证命名唯一
     */
    private String consumerGroup = MixAll.DEFAULT_CONSUMER_GROUP;
    /**
     * 集群消费/广播消费
     */
    private MessageModel messageModel = MessageModel.CLUSTERING;
    /**
     * Consumer从Master还是Slave拉消息
     */
    private ConsumeFromWhichNode consumeFromWhichNode = ConsumeFromWhichNode.CONSUME_FROM_MASTER_FIRST;
    /**
     * Consumer启动时，从哪里开始消费
     */
    private ConsumeFromWhereOffset consumeFromWhereOffset = ConsumeFromWhereOffset.CONSUME_FROM_LAST_OFFSET;
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
     * 消费消息线程数
     */
    private int consumeThreadCount = 10;
    /**
     * 同一队列并行消费的最大跨度，顺序消费方式情况下，此参数无效
     */
    private int consumeConcurrentlyMaxSpan = 1000;
    /**
     * 消费一批消息，最大数
     */
    private int consumeMessageBatchMaxSize = 1;
    /**
     * 拉消息，一次拉多少条
     */
    private int pullBatchSize = 32;

    private final transient DefaultMQPushConsumerImpl defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(
        this);


    public DefaultMQPushConsumer() {

    }


    public DefaultMQPushConsumer(final String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }


    @Override
    public void sendMessageBack(MessageExt msg, MessageQueue mq, int delayLevel) {
        // TODO Auto-generated method stub
    }


    @Override
    public void createTopic(String key, String newTopic, int queueNum, TopicFilterType topicFilterType,
            boolean order) throws MQClientException {
        // TODO Auto-generated method stub

    }


    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public long getMaxOffset(MessageQueue mq) throws MQClientException {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public long getMinOffset(MessageQueue mq) throws MQClientException {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public long getEarliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException, InterruptedException,
            MQClientException {
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
    public void start() {
        // TODO Auto-generated method stub

    }


    @Override
    public void shutdown() {
        // TODO Auto-generated method stub

    }


    @Override
    public void registerMessageListener(MessageListener messageListener) {
        // TODO Auto-generated method stub

    }


    @Override
    public void subscribe(String topic, String subExpression) {
        // TODO Auto-generated method stub

    }


    @Override
    public void unsubscribe(String topic) {
        // TODO Auto-generated method stub

    }


    @Override
    public void suspend() {
        // TODO Auto-generated method stub

    }


    @Override
    public void resume() {
        // TODO Auto-generated method stub

    }


    @Override
    public List<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        // TODO Auto-generated method stub
        return null;
    }


    public String getConsumerGroup() {
        return consumerGroup;
    }


    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }


    public ConsumeFromWhichNode getConsumeFromWhichNode() {
        return consumeFromWhichNode;
    }


    public void setConsumeFromWhichNode(ConsumeFromWhichNode consumeFromWhichNode) {
        this.consumeFromWhichNode = consumeFromWhichNode;
    }


    public ConsumeFromWhereOffset getConsumeFromWhereOffset() {
        return consumeFromWhereOffset;
    }


    public void setConsumeFromWhereOffset(ConsumeFromWhereOffset consumeFromWhereOffset) {
        this.consumeFromWhereOffset = consumeFromWhereOffset;
    }


    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }


    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }


    public Map<String, String> getSubscription() {
        return subscription;
    }


    public void setSubscription(Map<String, String> subscription) {
        this.subscription = subscription;
    }


    public MessageListener getMessageListener() {
        return messageListener;
    }


    public void setMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
    }


    public int getConsumeThreadCount() {
        return consumeThreadCount;
    }


    public void setConsumeThreadCount(int consumeThreadCount) {
        this.consumeThreadCount = consumeThreadCount;
    }


    public int getConsumeConcurrentlyMaxSpan() {
        return consumeConcurrentlyMaxSpan;
    }


    public void setConsumeConcurrentlyMaxSpan(int consumeConcurrentlyMaxSpan) {
        this.consumeConcurrentlyMaxSpan = consumeConcurrentlyMaxSpan;
    }


    public int getConsumeMessageBatchMaxSize() {
        return consumeMessageBatchMaxSize;
    }


    public void setConsumeMessageBatchMaxSize(int consumeMessageBatchMaxSize) {
        this.consumeMessageBatchMaxSize = consumeMessageBatchMaxSize;
    }


    public int getPullBatchSize() {
        return pullBatchSize;
    }


    public void setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
    }


    public DefaultMQPushConsumerImpl getDefaultMQPushConsumerImpl() {
        return defaultMQPushConsumerImpl;
    }


    public ClientConfig getClientConfig() {
        return clientConfig;
    }


    public void setClientConfig(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }


    public MessageModel getMessageModel() {
        return messageModel;
    }


    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }
}
