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
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.LiteMQPullConsumerImpl;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;

public class DefaultLiteMQPullConsumer extends DefaultMQPullConsumer implements LiteMQPullConsumer {
    private LiteMQPullConsumerImpl liteMQPullConsumer;

    /**
     * Maximum amount of time in minutes a message may block the consuming thread.
     */
    private long consumeTimeout = 15;

    /**
     * Is auto commit offset
     */
    private boolean autoCommit = true;

    private int pullThreadNumbers = 20;

    /**
     * Maximum commit offset interval time in seconds.
     */
    private long autoCommitInterval = 5;

    /**
     * Maximum number of messages pulled each time.
     */
    private int pullBatchNums = 10;

    public DefaultLiteMQPullConsumer(String consumerGroup, RPCHook rpcHook) {
        this.setConsumerGroup(consumerGroup);
        this.liteMQPullConsumer = new LiteMQPullConsumerImpl(this, rpcHook);
    }

    public DefaultLiteMQPullConsumer(String consumerGroup) {
        this.setConsumerGroup(consumerGroup);
        this.liteMQPullConsumer = new LiteMQPullConsumerImpl(this, null);
    }

    @Override
    public void start() throws MQClientException {
        this.liteMQPullConsumer.start();
    }

    @Override
    public void subscribe(String topic, String subExpression) throws MQClientException {
        this.liteMQPullConsumer.subscribe(topic, subExpression);
    }

    @Override
    public void unsubscribe(String topic) {
        this.liteMQPullConsumer.unsubscribe(topic);
    }

    @Override
    public List<MessageExt> poll() {
        return poll(this.getConsumerPullTimeoutMillis());
    }

    @Override public List<MessageExt> poll(long timeout) {
        return liteMQPullConsumer.poll(timeout);
    }

    @Override
    public void seek(MessageQueue messageQueue, long offset) throws MQClientException {
        this.liteMQPullConsumer.seek(messageQueue, offset);
    }

    @Override
    public void pause(Collection<MessageQueue> messageQueues) {
        this.liteMQPullConsumer.pause(messageQueues);
    }

    @Override
    public void resume(Collection<MessageQueue> messageQueues) {
        this.liteMQPullConsumer.resume(messageQueues);
    }

    @Override
    public void commitSync() {
        this.liteMQPullConsumer.commitSync();
    }

    public long getConsumeTimeout() {
        return consumeTimeout;
    }

    public void setConsumeTimeout(long consumeTimeout) {
        this.consumeTimeout = consumeTimeout;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public int getPullThreadNumbers() {
        return pullThreadNumbers;
    }

    public void setPullThreadNumbers(int pullThreadNumbers) {
        this.pullThreadNumbers = pullThreadNumbers;
    }

    public long getAutoCommitInterval() {
        return autoCommitInterval;
    }

    public void setAutoCommitInterval(long autoCommitInterval) {
        this.autoCommitInterval = autoCommitInterval;
    }

    public int getPullBatchNums() {
        return pullBatchNums;
    }

    public void setPullBatchNums(int pullBatchNums) {
        this.pullBatchNums = pullBatchNums;
    }

}
