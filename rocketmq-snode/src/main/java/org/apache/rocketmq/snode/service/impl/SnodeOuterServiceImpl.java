package org.apache.rocketmq.snode.service.impl;/*
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.TopAddressing;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.common.protocol.header.NotifyConsumerIdsChangedRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeaderV2;
import org.apache.rocketmq.common.protocol.header.namesrv.RegisterSnodeRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.RemotingClientFactory;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.config.SnodeConfig;
import org.apache.rocketmq.snode.service.SnodeOuterService;

public class SnodeOuterServiceImpl implements SnodeOuterService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);
    private final TopAddressing topAddressing = new TopAddressing(MixAll.getWSAddr());
    private String nameSrvAddr = null;
    private RemotingClient client;
    private SnodeController snodeController;
    private static SnodeOuterServiceImpl snodeOuterService;
    private final ConcurrentMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> enodeTable =
        new ConcurrentHashMap<>();
    private final long defaultTimeoutMills = 3000L;

    private SnodeOuterServiceImpl() {

    }

    public static SnodeOuterServiceImpl getInstance(SnodeController snodeController) {
        if (snodeOuterService == null) {
            synchronized (SnodeOuterServiceImpl.class) {
                if (snodeOuterService == null) {
                    snodeOuterService = new SnodeOuterServiceImpl(snodeController);
                    return snodeOuterService;
                }
            }
        }
        return snodeOuterService;
    }

    private SnodeOuterServiceImpl(SnodeController snodeController) {
        this.snodeController = snodeController;
        this.client = RemotingClientFactory.createInstance().init(snodeController.getNettyClientConfig(), null);
    }

    @Override
    public void start() {
        this.client.start();
    }

    @Override
    public void shutdown() {
        this.client.shutdown();
    }

    @Override
    public void sendHearbeat(RemotingCommand remotingCommand) {
        for (Map.Entry<String, HashMap<Long, String>> entry : enodeTable.entrySet()) {
            String enodeAddr = entry.getValue().get(MixAll.MASTER_ID);
            if (enodeAddr != null) {
                try {
                    RemotingCommand response = this.client.invokeSync(enodeAddr, remotingCommand, defaultTimeoutMills);
                } catch (Exception ex) {
                    log.warn("Send heart beat faild:{} ,ex:{}", enodeAddr, ex);
                }
            }
        }
    }

    @Override
    public RemotingCommand sendMessage(RemotingCommand request) {
        try {
            SendMessageRequestHeaderV2 sendMessageRequestHeaderV2 = (SendMessageRequestHeaderV2) request.decodeCommandCustomHeader(SendMessageRequestHeaderV2.class);
            RemotingCommand response =
                this.client.invokeSync(sendMessageRequestHeaderV2.getN(), request, defaultTimeoutMills);
            return response;
        } catch (Exception ex) {
            log.error("Send message async error:", ex);
        }
        return null;
    }

    @Override
    public RemotingCommand pullMessage(RemotingCommand request) {
        try {
            final PullMessageRequestHeader requestHeader =
                (PullMessageRequestHeader) request.decodeCommandCustomHeader(PullMessageRequestHeader.class);
            RemotingCommand remotingCommand =  this.client.invokeSync(requestHeader.getEnodeAddr(), request, 20 * defaultTimeoutMills);
            log.info("Pull message response:{}", remotingCommand);
            log.info("Pull message response:{}", remotingCommand.getBody().length);
            return remotingCommand;
        } catch (Exception ex) {
            log.error("pull message async error:", ex);
        }
        return null;
    }

    @Override
    public void saveSubscriptionData(RemotingCommand remotingCommand) {

    }

    @Override
    public String fetchNameServerAddr() {
        try {
            String addrs = this.topAddressing.fetchNSAddr();
            if (addrs != null) {
                if (!addrs.equals(this.nameSrvAddr)) {
                    log.info("name server address changed, old: {} new: {}", this.nameSrvAddr, addrs);
                    this.updateNameServerAddressList(addrs);
                    this.nameSrvAddr = addrs;
                    return nameSrvAddr;
                }
            }
        } catch (Exception e) {
            log.error("fetchNameServerAddr Exception", e);
        }
        return nameSrvAddr;
    }

    private ClusterInfo getBrokerClusterInfo(
        final long timeoutMillis) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);
        RemotingCommand response = this.client.invokeSync(null, request, timeoutMillis);
        assert response != null;
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
    public void updateEnodeAddr(String clusterName) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        synchronized (this) {
            ClusterInfo clusterInfo = getBrokerClusterInfo(defaultTimeoutMills);
            if (clusterInfo != null) {
                HashMap<String, Set<String>> brokerAddrs = clusterInfo.getClusterAddrTable();
                for (Map.Entry<String, Set<String>> entry : brokerAddrs.entrySet()) {
                    Set<String> brokerNames = entry.getValue();
                    if (brokerNames != null) {
                        for (String brokerName : brokerNames) {
                            enodeTable.put(brokerName, clusterInfo.getBrokerAddrTable().get(brokerName).getBrokerAddrs());
                        }
                    }
                }
            }
        }
    }

    public void updateNameServerAddressList(final String addrs) {
        List<String> list = new ArrayList<String>();
        String[] addrArray = addrs.split(";");
        for (String addr : addrArray) {
            list.add(addr);
        }
        this.client.updateNameServerAddressList(list);
    }

    public void registerSnode(SnodeConfig snodeConfig) {
        List<String> nameServerAddressList = this.client.getNameServerAddressList();
        RemotingCommand remotingCommand = new RemotingCommand();
        RegisterSnodeRequestHeader requestHeader = new RegisterSnodeRequestHeader();
        requestHeader.setSnodeAddr(snodeConfig.getSnodeAddr());
        requestHeader.setSnodeName(snodeConfig.getSnodeName());
        requestHeader.setClusterName(snodeConfig.getClusterName());
        remotingCommand.setCustomHeader(requestHeader);
        remotingCommand.setCode(RequestCode.REGISTER_SNODE);
        if (nameServerAddressList != null && nameServerAddressList.size() > 0) {
            for (String nameServer : nameServerAddressList) {
                try {
                    this.client.invokeSync(nameSrvAddr, remotingCommand, 3000L);
                } catch (Exception ex) {
                    log.warn("Register Snode to Nameserver addr: {} error, ex:{} ", nameServer, ex);
                }
            }
        }
    }

    @Override
    public void notifyConsumerIdsChanged(
        final Channel channel,
        final String consumerGroup) {
        if (null == consumerGroup) {
            log.error("notifyConsumerIdsChanged consumerGroup is null");
            return;
        }

        NotifyConsumerIdsChangedRequestHeader requestHeader = new NotifyConsumerIdsChangedRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        RemotingCommand request =
            RemotingCommand.createRequestCommand(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, requestHeader);

        try {
            this.snodeController.getSnodeServer().invokeOneway(channel, request, 10);
        } catch (Exception e) {
            log.error("notifyConsumerIdsChanged exception, " + consumerGroup, e.getMessage());
        }
    }

    @Override
    public RemotingCommand creatTopic(TopicConfig topicConfig) {
//        CreateTopicRequestHeader requestHeader = new CreateTopicRequestHeader();
//        requestHeader.setTopic(topicConfig.getTopicName());
//        requestHeader.setDefaultTopic(defaultTopic);
//        requestHeader.setReadQueueNums(topicConfig.getReadQueueNums());
//        requestHeader.setWriteQueueNums(topicConfig.getWriteQueueNums());
//        requestHeader.setPerm(topicConfig.getPerm());
//        requestHeader.setTopicFilterType(topicConfig.getTopicFilterType().name());
//        requestHeader.setTopicSysFlag(topicConfig.getTopicSysFlag());
//        requestHeader.setOrder(topicConfig.isOrder());
//
//        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC, requestHeader);
//
//        RemotingCommand response = this.client.invokeSync(,
//            request, defaultTimeoutMills);
//        assert response != null;
//        switch (response.getCode()) {
//            case ResponseCode.SUCCESS: {
//                return;
//            }
//            default:
//                break;
//        }
        return null;
    }
}
