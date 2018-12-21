package org.apache.rocketmq.snode.service;/*
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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.CompleteFuture;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.snode.config.SnodeConfig;

public interface SnodeOuterService {
    void sendHearbeat(RemotingCommand remotingCommand);

    CompletableFuture<RemotingCommand> sendMessage(final RemotingCommand request);

    CompletableFuture<RemotingCommand> pullMessage(final ChannelHandlerContext context,
        final RemotingCommand remotingCommand);

    void saveSubscriptionData(RemotingCommand remotingCommand);

    void start();

    void shutdown();

    void registerSnode(SnodeConfig snodeConfig);

    void updateNameServerAddressList(final String addrs);

    String fetchNameServerAddr();

    void updateEnodeAddr(String clusterName) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, MQBrokerException;

    void notifyConsumerIdsChanged(final Channel channel, final String consumerGroup);

    RemotingCommand creatTopic(TopicConfig topicConfig);
}
