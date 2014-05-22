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
package com.alibaba.rocketmq.client.producer;

import java.util.List;

import com.alibaba.rocketmq.client.ClientConfig;
import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * 消息生产者，适合使用spring初始化
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-25
 */
public class DefaultMQProducer extends ClientConfig implements MQProducer {
    protected final transient DefaultMQProducerImpl defaultMQProducerImpl = new DefaultMQProducerImpl(this);
    /**
     * 一般发送同样消息的Producer，归为同一个Group，应用必须设置，并保证命名唯一
     */
    private String producerGroup = MixAll.DEFAULT_PRODUCER_GROUP;
    /**
     * 支持在发送消息时，如果Topic不存在，自动创建Topic，但是要指定Key
     */
    private String createTopicKey = MixAll.DEFAULT_TOPIC;
    /**
     * 发送消息，自动创建Topic时，默认队列数
     */
    private volatile int defaultTopicQueueNums = 4;
    /**
     * 发送消息超时，不建议修改
     */
    private int sendMsgTimeout = 3000;
    /**
     * Message Body大小超过阀值，则压缩
     */
    private int compressMsgBodyOverHowmuch = 1024 * 4;
    /**
     * 发送失败后，重试几次
     */
    private int retryTimesWhenSendFailed = 2;
    /**
     * 消息已经成功写入Master，但是刷盘超时或者同步到Slave失败，则尝试重试另一个Broker，不建议修改默认值<br>
     * 顺序消息无效
     */
    private boolean retryAnotherBrokerWhenNotStoreOK = false;
    /**
     * 最大消息大小，默认512K
     */
    private int maxMessageSize = 1024 * 128;
	/**
	 * 是否为单元化的发布者
	 */
	private boolean isUnitMode = false;

    public DefaultMQProducer() {
    }


    public DefaultMQProducer(final String producerGroup) {
        this.producerGroup = producerGroup;
    }


    @Override
    public void start() throws MQClientException {
        this.defaultMQProducerImpl.start();
    }


    @Override
    public void shutdown() {
        this.defaultMQProducerImpl.shutdown();
    }


    @Override
    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        return this.defaultMQProducerImpl.fetchPublishMessageQueues(topic);
    }


    @Override
    public SendResult send(Message msg) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException {
        return this.defaultMQProducerImpl.send(msg);
    }


    @Override
    public void send(Message msg, SendCallback sendCallback) throws MQClientException, RemotingException,
            InterruptedException {
        this.defaultMQProducerImpl.send(msg, sendCallback);
    }


    @Override
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.sendOneway(msg);
    }


    @Override
    public SendResult send(Message msg, MessageQueue mq) throws MQClientException, RemotingException,
            MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg, mq);
    }


    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback) throws MQClientException,
            RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, mq, sendCallback);
    }


    @Override
    public void sendOneway(Message msg, MessageQueue mq) throws MQClientException, RemotingException,
            InterruptedException {
        this.defaultMQProducerImpl.sendOneway(msg, mq);
    }


    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg, selector, arg);
    }


    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback);
    }


    @Override
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg) throws MQClientException,
            RemotingException, InterruptedException {
        this.defaultMQProducerImpl.sendOneway(msg, selector, arg);
    }


    @Override
    public TransactionSendResult sendMessageInTransaction(Message msg, LocalTransactionExecuter tranExecuter,
            final Object arg) throws MQClientException {
        throw new RuntimeException(
            "sendMessageInTransaction not implement, please use TransactionMQProducer class");
    }


    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        this.defaultMQProducerImpl.createTopic(key, newTopic, queueNum);
    }


    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.defaultMQProducerImpl.searchOffset(mq, timestamp);
    }


    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.maxOffset(mq);
    }


    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.minOffset(mq);
    }


    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.earliestMsgStoreTime(mq);
    }


    @Override
    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        return this.defaultMQProducerImpl.viewMessage(msgId);
    }


    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        return this.defaultMQProducerImpl.queryMessage(topic, key, maxNum, begin, end);
    }


    public String getProducerGroup() {
        return producerGroup;
    }


    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }


    public String getCreateTopicKey() {
        return createTopicKey;
    }


    public void setCreateTopicKey(String createTopicKey) {
        this.createTopicKey = createTopicKey;
    }


    public int getSendMsgTimeout() {
        return sendMsgTimeout;
    }


    public void setSendMsgTimeout(int sendMsgTimeout) {
        this.sendMsgTimeout = sendMsgTimeout;
    }


    public int getCompressMsgBodyOverHowmuch() {
        return compressMsgBodyOverHowmuch;
    }


    public void setCompressMsgBodyOverHowmuch(int compressMsgBodyOverHowmuch) {
        this.compressMsgBodyOverHowmuch = compressMsgBodyOverHowmuch;
    }


    public DefaultMQProducerImpl getDefaultMQProducerImpl() {
        return defaultMQProducerImpl;
    }


    public boolean isRetryAnotherBrokerWhenNotStoreOK() {
        return retryAnotherBrokerWhenNotStoreOK;
    }


    public void setRetryAnotherBrokerWhenNotStoreOK(boolean retryAnotherBrokerWhenNotStoreOK) {
        this.retryAnotherBrokerWhenNotStoreOK = retryAnotherBrokerWhenNotStoreOK;
    }


    public int getMaxMessageSize() {
        return maxMessageSize;
    }


    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }


    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }


    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }


    public int getRetryTimesWhenSendFailed() {
        return retryTimesWhenSendFailed;
    }


    public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
        this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
    }

	public boolean isUnitMode() {
		return isUnitMode;
	}

	public void setUnitMode(boolean isUnitMode) {
		this.isUnitMode = isUnitMode;
	}
}
