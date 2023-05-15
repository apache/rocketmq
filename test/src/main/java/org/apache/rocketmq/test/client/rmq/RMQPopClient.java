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
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.AckCallback;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.PopCallback;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.impl.ClientRemotingProcessor;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.remoting.protocol.header.NotificationRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.test.clientinterface.MQConsumer;
import org.apache.rocketmq.test.util.RandomUtil;

public class RMQPopClient implements MQConsumer {

    private static final long DEFAULT_TIMEOUT = 3000;

    private MQClientAPIExt mqClientAPI;

    @Override
    public void create() {
        create(false);
    }

    @Override
    public void create(boolean useTLS) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setInstanceName(RandomUtil.getStringByUUID());

        NettyClientConfig nettyClientConfig = new NettyClientConfig();
        nettyClientConfig.setUseTLS(useTLS);
        this.mqClientAPI = new MQClientAPIExt(
            clientConfig, nettyClientConfig, new ClientRemotingProcessor(null), null);
    }

    @Override
    public void start() {
        this.mqClientAPI.start();
    }

    @Override
    public void shutdown() {
        this.mqClientAPI.shutdown();
    }

    public CompletableFuture<PopResult> popMessageAsync(String brokerAddr, MessageQueue mq, long invisibleTime,
        int maxNums, String consumerGroup, long timeout, boolean poll, int initMode, boolean order,
        String expressionType, String expression) {
        return popMessageAsync(brokerAddr, mq, invisibleTime, maxNums, consumerGroup, timeout, poll, initMode, order, expressionType, expression, null);
    }

    public CompletableFuture<PopResult> popMessageAsync(String brokerAddr, MessageQueue mq, long invisibleTime,
        int maxNums, String consumerGroup, long timeout, boolean poll, int initMode, boolean order,
        String expressionType, String expression, String attemptId) {
        PopMessageRequestHeader requestHeader = new PopMessageRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setTopic(mq.getTopic());
        requestHeader.setQueueId(mq.getQueueId());
        requestHeader.setMaxMsgNums(maxNums);
        requestHeader.setInvisibleTime(invisibleTime);
        requestHeader.setInitMode(initMode);
        requestHeader.setExpType(expressionType);
        requestHeader.setExp(expression);
        requestHeader.setOrder(order);
        requestHeader.setAttemptId(attemptId);
        if (poll) {
            requestHeader.setPollTime(timeout);
            requestHeader.setBornTime(System.currentTimeMillis());
            timeout += 10 * 1000;
        }
        CompletableFuture<PopResult> future = new CompletableFuture<>();
        try {
            this.mqClientAPI.popMessageAsync(mq.getBrokerName(), brokerAddr, requestHeader, timeout, new PopCallback() {
                @Override
                public void onSuccess(PopResult popResult) {
                    future.complete(popResult);
                }

                @Override
                public void onException(Throwable e) {
                    future.completeExceptionally(e);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<AckResult> ackMessageAsync(
        String brokerAddr, String topic, String consumerGroup, String extraInfo) {

        String[] extraInfoStrs = ExtraInfoUtil.split(extraInfo);
        AckMessageRequestHeader requestHeader = new AckMessageRequestHeader();
        requestHeader.setTopic(ExtraInfoUtil.getRealTopic(extraInfoStrs, topic, consumerGroup));
        requestHeader.setQueueId(ExtraInfoUtil.getQueueId(extraInfoStrs));
        requestHeader.setOffset(ExtraInfoUtil.getQueueOffset(extraInfoStrs));
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setExtraInfo(extraInfo);
        CompletableFuture<AckResult> future = new CompletableFuture<>();
        try {
            this.mqClientAPI.ackMessageAsync(brokerAddr, DEFAULT_TIMEOUT, new AckCallback() {
                @Override
                public void onSuccess(AckResult ackResult) {
                    future.complete(ackResult);
                }

                @Override
                public void onException(Throwable e) {
                    future.completeExceptionally(e);
                }
            }, requestHeader);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<AckResult> changeInvisibleTimeAsync(String brokerAddr, String brokerName, String topic,
        String consumerGroup, String extraInfo, long invisibleTime) {
        String[] extraInfoStrs = ExtraInfoUtil.split(extraInfo);
        ChangeInvisibleTimeRequestHeader requestHeader = new ChangeInvisibleTimeRequestHeader();
        requestHeader.setTopic(ExtraInfoUtil.getRealTopic(extraInfoStrs, topic, consumerGroup));
        requestHeader.setQueueId(ExtraInfoUtil.getQueueId(extraInfoStrs));
        requestHeader.setOffset(ExtraInfoUtil.getQueueOffset(extraInfoStrs));
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setExtraInfo(extraInfo);
        requestHeader.setInvisibleTime(invisibleTime);

        CompletableFuture<AckResult> future = new CompletableFuture<>();
        try {
            this.mqClientAPI.changeInvisibleTimeAsync(brokerName, brokerAddr, requestHeader, DEFAULT_TIMEOUT, new AckCallback() {
                @Override
                public void onSuccess(AckResult ackResult) {
                    future.complete(ackResult);
                }

                @Override
                public void onException(Throwable e) {
                    future.completeExceptionally(e);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<Boolean> notification(String brokerAddr, String topic,
        String consumerGroup, int queueId, long pollTime, long bornTime, long timeoutMillis) {
        return notification(brokerAddr, topic, consumerGroup, queueId, null, null, pollTime, bornTime, timeoutMillis);
    }

    public CompletableFuture<Boolean> notification(String brokerAddr, String topic,
        String consumerGroup, int queueId, Boolean order, String attemptId, long pollTime, long bornTime, long timeoutMillis) {
        NotificationRequestHeader requestHeader = new NotificationRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        requestHeader.setPollTime(pollTime);
        requestHeader.setBornTime(bornTime);
        requestHeader.setOrder(order);
        requestHeader.setAttemptId(attemptId);
        return this.mqClientAPI.notification(brokerAddr, requestHeader, timeoutMillis);
    }
}
