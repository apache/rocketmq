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
package org.apache.rocketmq.snode.service.impl;

import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.service.EnodeService;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.CodecHelper;
import org.apache.rocketmq.remoting.netty.NettyChannelHandlerContextImpl;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class LocalEnodeServiceImpl implements EnodeService {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);

    private BrokerController brokerController;

    public LocalEnodeServiceImpl(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override public void sendHeartbeat(RemotingCommand remotingCommand) {
        return;
    }

    @Override
    public CompletableFuture<RemotingCommand> sendMessage(final RemotingChannel remotingChannel, String enodeName,
        RemotingCommand request) {
        CompletableFuture<RemotingCommand> completableFuture = new CompletableFuture<>();
        try {
            log.debug("Send message request: {}", request);
            RemotingCommand remotingCommand = this.brokerController.getSendProcessor().processRequest(remotingChannel, request);
            CodecHelper.encodeHeader(remotingCommand);
            completableFuture.complete(remotingCommand);
        } catch (Exception ex) {
            log.error("[Local]Request local enode send message error", ex);
            completableFuture.completeExceptionally(ex);
        }
        return completableFuture;
    }

    @Override
    public CompletableFuture<RemotingCommand> pullMessage(RemotingChannel remotingChannel, String enodeName,
        RemotingCommand request) {
        CompletableFuture<RemotingCommand> completableFuture = new CompletableFuture<>();
        try {
            RemotingCommand response = this.brokerController.getSnodePullMessageProcessor().processRequest(remotingChannel, request);
            completableFuture.complete(response);
        } catch (Exception ex) {
            log.error("[Local]Request local enode pull message error", ex);
            completableFuture.completeExceptionally(ex);
        }
        return completableFuture;
    }

    @Override public RemotingCommand pullMessageSync(RemotingChannel remotingChannel, String enodeName,
        RemotingCommand request) {
        return null;
    }

    @Override public RemotingCommand creatRetryTopic(RemotingChannel remotingChannel, String enodeName,
        RemotingCommand request) {
        try {
            return this.brokerController.getClientManageProcessor().processRequest(remotingChannel, request);
        } catch (Exception ex) {
            log.error("[Local]Request create retry topic error", ex);
        }
        return null;
    }

    @Override public void updateEnodeAddress(
        String clusterName) {

    }

    @Override public boolean persistSubscriptionGroupConfig(
        final SubscriptionGroupConfig subscriptionGroupConfig) {
        boolean persist = false;
        if (subscriptionGroupConfig != null) {
            this.brokerController.getSubscriptionGroupManager().updateSubscriptionGroupConfig(subscriptionGroupConfig);
            persist = true;
        }
        return persist;
    }

    @Override
    public void persistOffset(RemotingChannel remotingChannel, String enodeName, String groupName, String topic,
        int queueId, long offset) {
        try {
            this.brokerController.getConsumerOffsetManager().commitOffset(remotingChannel.remoteAddress().toString(), groupName,
                topic, queueId, offset);
        } catch (Exception ex) {
            log.error("[Local]Persist offset to Enode error group: [{}], topic: [{}] queue: [{}]!", ex, groupName, topic, queueId);
        }
    }

    @Override
    public long queryOffset(String enodeName, String consumerGroup, String topic, int queueId) {
        return this.brokerController.getConsumerOffsetManager().queryOffset(consumerGroup, topic, queueId);
    }

    @Override
    public long getMaxOffsetInQueue(String enodeName, String topic, int queueId, RemotingCommand request) {
        return this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
    }

    @Override
    public long getMinOffsetInQueue(String enodeName, String topic, int queueId, RemotingCommand request) {
        return this.brokerController.getMessageStore().getMinOffsetInQueue(topic, queueId);
    }

    @Override
    public long getOffsetByTimestamp(String enodeName,
        String topic, int queueId, long timestamp, RemotingCommand request) {
        return this.brokerController.getMessageStore().getOffsetInQueueByTime(topic, queueId, timestamp);
    }

    @Override public RemotingCommand lockBatchMQ(RemotingChannel remotingChannel, RemotingCommand request) {
        NettyChannelHandlerContextImpl nettyChannelHandlerContext = (NettyChannelHandlerContextImpl) remotingChannel;
        ChannelHandlerContext ctx = nettyChannelHandlerContext.getChannelHandlerContext();
        return this.brokerController.getAdminProcessor().lockBatchMQ(ctx, request);
    }

    @Override public RemotingCommand unlockBatchMQ(RemotingChannel remotingChannel, RemotingCommand request) {
        NettyChannelHandlerContextImpl nettyChannelHandlerContext = (NettyChannelHandlerContextImpl) remotingChannel;
        ChannelHandlerContext ctx = nettyChannelHandlerContext.getChannelHandlerContext();
        log.info("un");
        return this.brokerController.getAdminProcessor().lockBatchMQ(ctx, request);
    }

    @Override public RemotingCommand requestMQTTInfoSync(
        RemotingCommand request) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        try {
            return this.brokerController.getMqttProcessor().processRequest(null, request);
        } catch (Exception e) {
            log.error("[Local]RequestMQTTInfo failed, error: {}", e);
        }
        return null;
    }

    @Override public CompletableFuture<RemotingCommand> requestMQTTInfoAsync(
        RemotingCommand request) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        CompletableFuture<RemotingCommand> completableFuture = new CompletableFuture<>();
        try {
            RemotingCommand remotingCommand = this.brokerController.getMqttProcessor().processRequest(null, request);
            CodecHelper.encodeHeader(remotingCommand);
            completableFuture.complete(remotingCommand);
        } catch (Exception ex) {
            log.error("[Local]RequestMQTTInfo failed, error: {}", ex);
            completableFuture.completeExceptionally(ex);
        }
        return completableFuture;
    }
}
