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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.serialize.RemotingSerializable;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.constant.SnodeConstant;
import org.apache.rocketmq.snode.service.EnodeService;

public class EnodeServiceImpl implements EnodeService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);

    private SnodeController snodeController;

    private final ConcurrentMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> enodeTable =
        new ConcurrentHashMap<>();

    public EnodeServiceImpl(SnodeController snodeController) {
        this.snodeController = snodeController;
    }

    @Override
    public void sendHearbeat(RemotingCommand remotingCommand) {
        for (Map.Entry<String, HashMap<Long, String>> entry : enodeTable.entrySet()) {
            String enodeAddr = entry.getValue().get(MixAll.MASTER_ID);
            if (enodeAddr != null) {
                try {
                    this.snodeController.getRemotingClient().invokeSync(enodeAddr, remotingCommand, SnodeConstant.DEFAULT_TIMEOUT_MILLS);
                } catch (Exception ex) {
                    log.warn("Send heart beat faild:{} ,ex:{}", enodeAddr, ex);
                }
            }
        }
    }

    @Override
    public CompletableFuture<RemotingCommand> pullMessage(final String enodeName, final RemotingCommand request) {

        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        try {
            String enodeAddress = this.snodeController.getNnodeService().getAddressByEnodeName(enodeName, false);
            this.snodeController.getRemotingClient().invokeAsync(enodeAddress, request, SnodeConstant.CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND, new InvokeCallback() {
                @Override
                public void operationComplete(ResponseFuture responseFuture) {
                    RemotingCommand response = responseFuture.getResponseCommand();
                    if (response != null) {
                        future.complete(response);
                    } else {
                        if (!responseFuture.isSendRequestOK()) {
                            log.error("Pull message error in async callback: {}", responseFuture.getCause());
                        } else if (responseFuture.isTimeout()) {
                            log.warn("Pull message timeout!");
                        } else {
                            log.error("Unknown pull message error occurred: {}", responseFuture.getCause());
                        }
                    }
                }
            });
        } catch (Exception ex) {
            log.error("Pull message async error:", ex);
            future.completeExceptionally(ex);
        }
        return future;
    }

    @Override
    public CompletableFuture<RemotingCommand> sendMessage(String enodeName, RemotingCommand request) {
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        try {
            String enodeAddress = this.snodeController.getNnodeService().getAddressByEnodeName(enodeName, false);
            this.snodeController.getRemotingClient().invokeAsync(enodeAddress, request, SnodeConstant.DEFAULT_TIMEOUT_MILLS, (responseFuture) -> {
                future.complete(responseFuture.getResponseCommand());
            });
        } catch (Exception ex) {
            log.error("Send message async error:{}", ex);
            future.completeExceptionally(ex);
        }
        return future;
    }

    private ClusterInfo getBrokerClusterInfo(
        final long timeoutMillis) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);
        RemotingCommand response = this.snodeController.getRemotingClient().invokeSync(null, request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return ClusterInfo.decode(response.getBody(), ClusterInfo.class);
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    @Override
    public void updateEnodeAddress(String clusterName) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        synchronized (this) {
            ClusterInfo clusterInfo = getBrokerClusterInfo(SnodeConstant.DEFAULT_TIMEOUT_MILLS);
            if (clusterInfo != null) {
                HashMap<String, Set<String>> enodeAddress = clusterInfo.getClusterAddrTable();
                for (Map.Entry<String, Set<String>> entry : enodeAddress.entrySet()) {
                    Set<String> enodeNames = entry.getValue();
                    if (enodeNames != null) {
                        for (String enodeName : enodeNames) {
                            enodeTable.put(enodeName, clusterInfo.getBrokerAddrTable().get(enodeName).getBrokerAddrs());
                        }
                    }
                }
            }
        }
    }

    @Override
    public boolean persistSubscriptionGroupConfig(SubscriptionGroupConfig subscriptionGroupConfig) {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, null);
        boolean persist = false;
        for (Map.Entry<String, HashMap<Long, String>> entry : enodeTable.entrySet()) {
            byte[] body = RemotingSerializable.encode(subscriptionGroupConfig);
            request.setBody(body);
            String enodeAddress = entry.getValue().get(MixAll.MASTER_ID);
            try {
                RemotingCommand response = this.snodeController.getRemotingClient().invokeSync(enodeAddress,
                    request, SnodeConstant.DEFAULT_TIMEOUT_MILLS);
                if (response != null && response.getCode() == ResponseCode.SUCCESS) {
                    persist = true;
                } else {
                    persist = false;
                }
                log.info("Persist to broker address: {} result: {}", enodeAddress, persist);
            } catch (Exception ex) {
                log.warn("Persist Subscription to Enode {} error", enodeAddress);
                persist = false;
            }
        }
        return persist;
    }

    @Override
    public void persistOffset(String enodeName, String groupName, String topic, int queueId, long offset) {
        try {
            String address = this.snodeController.getNnodeService().getAddressByEnodeName(enodeName, false);
            UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
            requestHeader.setTopic(topic);
            requestHeader.setConsumerGroup(groupName);
            requestHeader.setQueueId(queueId);
            requestHeader.setCommitOffset(offset);
            requestHeader.setEnodeName(enodeName);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader);
            this.snodeController.getRemotingClient().invokeOneway(address, request, SnodeConstant.DEFAULT_TIMEOUT_MILLS);
        } catch (Exception ex) {
            log.error("Persist offset to Enode error!");
        }
    }

    @Override
    public RemotingCommand getMinOffsetInQueue(String enodeName, String topic,
        int queueId) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException {
        GetMinOffsetRequestHeader requestHeader = new GetMinOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MIN_OFFSET, requestHeader);
        String addr = this.snodeController.getNnodeService().getAddressByEnodeName(enodeName, false);
        return this.snodeController.getRemotingClient().invokeSync(MixAll.brokerVIPChannel(snodeController.getSnodeConfig().isVipChannelEnabled(), addr),
            request, SnodeConstant.DEFAULT_TIMEOUT_MILLS);
    }

    @Override
    public RemotingCommand loadOffset(String enodeName, String consumerGroup, String topic,
        int queueId) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException {
        QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setQueueId(queueId);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, requestHeader);
        String addr = this.snodeController.getNnodeService().getAddressByEnodeName(enodeName, false);
        return this.snodeController.getRemotingClient().invokeSync(MixAll.brokerVIPChannel(this.snodeController.getSnodeConfig().isVipChannelEnabled(), addr),
            request, SnodeConstant.DEFAULT_TIMEOUT_MILLS);
    }

    @Override
    public RemotingCommand getMaxOffsetInQueue(String enodeName,
        RemotingCommand request) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, RemotingCommandException {
        String addr = this.snodeController.getNnodeService().getAddressByEnodeName(enodeName, false);
        return this.snodeController.getRemotingClient().invokeSync(MixAll.brokerVIPChannel(snodeController.getSnodeConfig().isVipChannelEnabled(), addr),
            request, SnodeConstant.DEFAULT_TIMEOUT_MILLS);
    }

    @Override
    public RemotingCommand getOffsetByTimestamp(String enodeName,
        RemotingCommand request) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        String addr = this.snodeController.getNnodeService().getAddressByEnodeName(enodeName, false);
        return this.snodeController.getRemotingClient().invokeSync(MixAll.brokerVIPChannel(snodeController.getSnodeConfig().isVipChannelEnabled(), addr),
            request, SnodeConstant.DEFAULT_TIMEOUT_MILLS);
    }

    @Override
    public RemotingCommand creatTopic(String enodeName,
        TopicConfig topicConfig) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        CreateTopicRequestHeader requestHeader = new CreateTopicRequestHeader();
        requestHeader.setTopic(topicConfig.getTopicName());
        requestHeader.setReadQueueNums(topicConfig.getReadQueueNums());
        requestHeader.setWriteQueueNums(topicConfig.getWriteQueueNums());
        requestHeader.setPerm(topicConfig.getPerm());
        requestHeader.setTopicFilterType(topicConfig.getTopicFilterType().name());
        requestHeader.setTopicSysFlag(topicConfig.getTopicSysFlag());
        requestHeader.setOrder(topicConfig.isOrder());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC, requestHeader);
        String address = this.snodeController.getNnodeService().getAddressByEnodeName(enodeName, false);
        return this.snodeController.getRemotingClient().invokeSync(address,
            request, SnodeConstant.DEFAULT_TIMEOUT_MILLS);
    }
}
