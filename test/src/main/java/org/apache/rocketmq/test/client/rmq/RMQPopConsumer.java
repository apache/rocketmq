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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.client.rmq;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.test.factory.ConsumerFactory;
import org.apache.rocketmq.test.listener.AbstractListener;

public class RMQPopConsumer extends RMQNormalConsumer {

    private static final Logger log = LoggerFactory.getLogger(RMQPopConsumer.class);

    public static final long POP_TIMEOUT = 3000;
    public static final long DEFAULT_INVISIBLE_TIME = 30000;

    private RMQPopClient client;

    private int maxNum = 16;

    public RMQPopConsumer(String nsAddr, String topic, String subExpression,
        String consumerGroup, AbstractListener listener) {
        super(nsAddr, topic, subExpression, consumerGroup, listener);
    }

    public RMQPopConsumer(String nsAddr, String topic, String subExpression,
        String consumerGroup, AbstractListener listener, int maxNum) {
        super(nsAddr, topic, subExpression, consumerGroup, listener);
        this.maxNum = maxNum;
    }

    @Override
    public void start() {
        client = ConsumerFactory.getRMQPopClient();
        log.info("consumer[{}] started!", consumerGroup);
    }

    @Override
    public void shutdown() {
        client.shutdown();
    }

    public PopResult pop(String brokerAddr, MessageQueue mq) throws Exception {
        return this.pop(brokerAddr, mq, DEFAULT_INVISIBLE_TIME, 5000);
    }

    public PopResult pop(String brokerAddr, MessageQueue mq, long invisibleTime, long timeout)
        throws InterruptedException, RemotingException, MQClientException, MQBrokerException,
        ExecutionException, TimeoutException {

        CompletableFuture<PopResult> future = this.client.popMessageAsync(
            brokerAddr, mq, invisibleTime, maxNum, consumerGroup, timeout, true,
            ConsumeInitMode.MIN, false, ExpressionType.TAG, "*");

        return future.get();
    }

    public PopResult popOrderly(String brokerAddr, MessageQueue mq) throws Exception {
        return this.popOrderly(brokerAddr, mq, DEFAULT_INVISIBLE_TIME, 5000);
    }

    public PopResult popOrderly(String brokerAddr, MessageQueue mq, long invisibleTime, long timeout)
        throws InterruptedException, ExecutionException {

        CompletableFuture<PopResult> future = this.client.popMessageAsync(
            brokerAddr, mq, invisibleTime, maxNum, consumerGroup, timeout, true,
            ConsumeInitMode.MIN, true, ExpressionType.TAG, "*");

        return future.get();
    }

    public CompletableFuture<AckResult> ackAsync(String brokerAddr, String extraInfo) {
        return this.client.ackMessageAsync(brokerAddr, topic, consumerGroup, extraInfo);
    }
}
