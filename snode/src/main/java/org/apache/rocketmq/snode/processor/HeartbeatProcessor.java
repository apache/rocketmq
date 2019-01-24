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
package org.apache.rocketmq.snode.processor;

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.common.protocol.header.UnregisterClientResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.RequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyChannelHandlerContextImpl;
import org.apache.rocketmq.remoting.netty.NettyChannelImpl;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.client.Client;
import org.apache.rocketmq.snode.client.impl.ClientRole;
import org.apache.rocketmq.snode.constant.SnodeConstant;

public class HeartbeatProcessor implements RequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);
    private final SnodeController snodeController;

    public HeartbeatProcessor(SnodeController snodeController) {
        this.snodeController = snodeController;
    }

    @Override
    public RemotingCommand processRequest(RemotingChannel remotingChannel,
        RemotingCommand request) throws Exception {
        switch (request.getCode()) {
            case RequestCode.HEART_BEAT:
                return register(remotingChannel, request);
            case RequestCode.UNREGISTER_CLIENT:
                return unregister(remotingChannel, request);
            default:
                break;
        }
        return null;
    }

    private RemotingCommand register(RemotingChannel remotingChannel, RemotingCommand request) {
        HeartbeatData heartbeatData = HeartbeatData.decode(request.getBody(), HeartbeatData.class);
        Channel channel = null;
        Attribute<Client> clientAttribute = null;
        if (remotingChannel instanceof NettyChannelHandlerContextImpl) {
            channel = ((NettyChannelHandlerContextImpl) remotingChannel).getChannelHandlerContext().channel();
            clientAttribute = channel.attr(SnodeConstant.NETTY_CLIENT_ATTRIBUTE_KEY);
        }
        Client client = new Client();
        client.setClientId(heartbeatData.getClientID());
        client.setRemotingChannel(remotingChannel);

        for (ProducerData producerData : heartbeatData.getProducerDataSet()) {
            client.setGroupId(producerData.getGroupName());
            client.setClientRole(ClientRole.Producer);
            this.snodeController.getProducerManager().register(client);
        }

        for (ConsumerData data : heartbeatData.getConsumerDataSet()) {
            client.setGroupId(data.getGroupName());
            client.setClientRole(ClientRole.Consumer);
            boolean channelChanged = this.snodeController.getConsumerManager().register(client);
            boolean subscriptionChanged = this.snodeController.getSubscriptionManager().subscribe(data.getGroupName(),
                data.getSubscriptionDataSet(),
                data.getConsumeType(),
                data.getMessageModel(),
                data.getConsumeFromWhere());
            if (data.getConsumeType() == ConsumeType.CONSUME_PUSH) {
                NettyChannelImpl nettyChannel = new NettyChannelImpl(channel);
                this.snodeController.getSubscriptionManager().registerPushSession(data.getSubscriptionDataSet(), nettyChannel, data.getGroupName());
            }
            if (subscriptionChanged || channelChanged) {
                this.snodeController.getClientService().notifyConsumer(data.getGroupName());
            }
        }

        clientAttribute.setIfAbsent(client);
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand unregister(RemotingChannel remotingChannel, RemotingCommand request) throws Exception {
        final RemotingCommand response =
            RemotingCommand.createResponseCommand(UnregisterClientResponseHeader.class);
        final UnregisterClientRequestHeader requestHeader =
            (UnregisterClientRequestHeader) request.decodeCommandCustomHeader(UnregisterClientRequestHeader.class);

        final String producerGroup = requestHeader.getProducerGroup();
        if (producerGroup != null) {
            this.snodeController.getProducerManager().unRegister(producerGroup, remotingChannel);
        }

        final String consumerGroup = requestHeader.getConsumerGroup();
        if (consumerGroup != null) {
            this.snodeController.getConsumerManager().unRegister(consumerGroup, remotingChannel);
            this.snodeController.getSubscriptionManager().removePushSession(remotingChannel);
            this.snodeController.getClientService().notifyConsumer(consumerGroup);
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
