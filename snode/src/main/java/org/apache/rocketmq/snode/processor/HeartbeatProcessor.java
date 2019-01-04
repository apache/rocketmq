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

import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.common.protocol.header.UnregisterClientResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.RequestProcessor;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.client.ClientChannelInfo;

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
                return heartbeat(remotingChannel, request);
            case RequestCode.UNREGISTER_CLIENT:
                return unregister(remotingChannel, request);
            default:
                break;
        }
        return null;
    }

    private RemotingCommand heartbeat(RemotingChannel remotingChannel, RemotingCommand request) {
        HeartbeatData heartbeatData = HeartbeatData.decode(request.getBody(), HeartbeatData.class);
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
            remotingChannel,
            heartbeatData.getClientID(),
            request.getLanguage(),
            request.getVersion()
        );

        if (heartbeatData.getProducerDataSet() != null) {
            for (ProducerData producerData : heartbeatData.getProducerDataSet()) {
                this.snodeController.getProducerManager().registerProducer(producerData.getGroupName(),
                    clientChannelInfo);
            }
        }

        if (heartbeatData.getConsumerDataSet() != null) {
            for (ConsumerData data : heartbeatData.getConsumerDataSet()) {
                SubscriptionGroupConfig subscriptionGroupConfig =
                    this.snodeController.getSubscriptionGroupManager().findSubscriptionGroupConfig(
                        data.getGroupName());
                boolean isNotifyConsumerIdsChangedEnable = subscriptionGroupConfig.isNotifyConsumerIdsChangedEnable();
                if (null != subscriptionGroupConfig) {
                    isNotifyConsumerIdsChangedEnable = subscriptionGroupConfig.isNotifyConsumerIdsChangedEnable();
                }
                boolean changed = this.snodeController.getConsumerManager().registerConsumer(
                    data.getGroupName(),
                    clientChannelInfo,
                    data.getConsumeType(),
                    data.getMessageModel(),
                    data.getConsumeFromWhere(),
                    data.getSubscriptionDataSet(),
                    isNotifyConsumerIdsChangedEnable
                );

                if (changed) {
                    log.info("registerConsumer info changed {} {}",
                        data.toString(),
                        RemotingHelper.parseChannelRemoteAddr(remotingChannel.remoteAddress())
                    );
                }

                if (subscriptionGroupConfig.isRealPushEnable()) {
                    this.snodeController.getConsumerManager().updateTopicConsumerTable(data.getSubscriptionDataSet(), clientChannelInfo);
                }
            }
        }
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

        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
            remotingChannel,
            requestHeader.getClientID(),
            request.getLanguage(),
            request.getVersion());
        {
            final String group = requestHeader.getProducerGroup();
            if (group != null) {
                this.snodeController.getProducerManager().unregisterProducer(group, clientChannelInfo);
            }
        }

        {
            final String group = requestHeader.getConsumerGroup();
            if (group != null) {
                SubscriptionGroupConfig subscriptionGroupConfig =
                    this.snodeController.getSubscriptionGroupManager().findSubscriptionGroupConfig(group);
                boolean isNotifyConsumerIdsChangedEnable = true;
                if (null != subscriptionGroupConfig) {
                    isNotifyConsumerIdsChangedEnable = subscriptionGroupConfig.isNotifyConsumerIdsChangedEnable();
                }
                this.snodeController.getConsumerManager().unregisterConsumer(group, clientChannelInfo, isNotifyConsumerIdsChangedEnable);
            }
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
