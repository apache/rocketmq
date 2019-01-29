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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.TopAddressing;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.RegisterSnodeRequestHeader;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.config.SnodeConfig;
import org.apache.rocketmq.snode.constant.SnodeConstant;
import org.apache.rocketmq.snode.service.NnodeService;

public class NnodeServiceImpl implements NnodeService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);
    private final TopAddressing topAddressing = new TopAddressing(MixAll.getWSAddr());
    private String nameSrvAddr = null;
    private SnodeController snodeController;
    private ConcurrentHashMap<String /*Topic*/, TopicRouteData> topicRouteDataMap = new ConcurrentHashMap<>(1000);
    private ClusterInfo clusterInfo;

    public NnodeServiceImpl(SnodeController snodeController) {
        this.snodeController = snodeController;
    }

    public String getSnodeAddress() {
        return this.snodeController.getSnodeConfig().getSnodeIP1() + ":" + this.snodeController.getSnodeConfig().getListenPort();
    }

    @Override
    public void registerSnode(SnodeConfig snodeConfig) {
        List<String> nnodeAddressList = this.snodeController.getRemotingClient().getNameServerAddressList();
        RemotingCommand remotingCommand = new RemotingCommand();
        RegisterSnodeRequestHeader requestHeader = new RegisterSnodeRequestHeader();
        requestHeader.setSnodeAddr(getSnodeAddress());
        requestHeader.setSnodeName(snodeConfig.getSnodeName());
        requestHeader.setClusterName(snodeConfig.getClusterName());
        remotingCommand.setCustomHeader(requestHeader);
        remotingCommand.setCode(RequestCode.REGISTER_SNODE);
        if (nnodeAddressList != null && nnodeAddressList.size() > 0) {
            for (String nodeAddress : nnodeAddressList) {
                try {
                    this.snodeController.getRemotingClient().invokeSync(nodeAddress, remotingCommand, SnodeConstant.HEARTBEAT_TIME_OUT);
                } catch (Exception ex) {
                    log.warn("Register Snode to Nnode addr: {} error, ex:{} ", nodeAddress, ex);
                }
            }
        }
    }

    @Override
    public void updateTopicRouteDataByTopic() {
        Set<String> topSet = topicRouteDataMap.keySet();
        for (String topic : topSet) {
            try {
                TopicRouteData topicRouteData = getTopicRouteDataByTopic(topic, false);
                topicRouteDataMap.put(topic, topicRouteData);
            } catch (Exception ex) {
                log.error("Update topic {} error: {}", topic, ex);
            }
        }
    }

    private TopicRouteData getTopicRouteDataByTopicFromNnode(String topic,
        boolean allowTopicNotExist) throws MQClientException, InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINTO_BY_TOPIC, requestHeader);
        RemotingCommand response = this.snodeController.getRemotingClient().invokeSync(null, request, SnodeConstant.DEFAULT_TIMEOUT_MILLS);
        log.info("GetTopicRouteInfoFromNameServer response: " + response);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.TOPIC_NOT_EXIST: {
                if (allowTopicNotExist && !topic.equals(MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC)) {
                    log.warn("Topic [{}] RouteInfo is not exist value", topic);
                }
                break;
            }
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    return TopicRouteData.decode(body, TopicRouteData.class);
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    @Override
    public TopicRouteData getTopicRouteDataByTopic(
        String topic,
        boolean allowTopicNotExist) throws MQClientException, InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        if (topic == null || "".equals(topic)) {
            return null;
        }

        TopicRouteData topicRouteData = topicRouteDataMap.get(topic);
        if (topicRouteData == null) {
            topicRouteData = getTopicRouteDataByTopicFromNnode(topic, allowTopicNotExist);
            if (topicRouteData != null) {
                topicRouteDataMap.put(topic, topicRouteData);
            }
        }
        return topicRouteData;
    }

    @Override
    public void updateNnodeAddressList(final String addrs) {
        List<String> list = new ArrayList<String>();
        String[] addrArray = addrs.split(";");
        for (String addr : addrArray) {
            list.add(addr);
        }
        this.snodeController.getRemotingClient().updateNameServerAddressList(list);
    }

    @Override
    public String fetchNnodeAdress() {
        try {
            String addrs = this.topAddressing.fetchNSAddr();
            if (addrs != null) {
                if (!addrs.equals(this.nameSrvAddr)) {
                    log.info("Nnode server address changed, old: {} new: {}", this.nameSrvAddr, addrs);
                    this.updateNnodeAddressList(addrs);
                    this.nameSrvAddr = addrs;
                    return nameSrvAddr;
                }
            }
        } catch (Exception e) {
            log.error("FetchNnodeServerAddr Exception", e);
        }
        return nameSrvAddr;
    }

    @Override
    public ClusterInfo updateEnodeClusterInfo() throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);

        RemotingCommand response = this.snodeController.getRemotingClient().invokeSync(null, request, SnodeConstant.DEFAULT_TIMEOUT_MILLS);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                ClusterInfo clusterInfo = ClusterInfo.decode(response.getBody(), ClusterInfo.class);
                this.clusterInfo = clusterInfo;
                return clusterInfo;
            }
            default:
                break;
        }
        log.error("Update Cluster info error: {}", response);
        return clusterInfo;
    }

    public Set<String> getEnodeClusterInfo(String clusterName) {
        if (this.clusterInfo == null) {
            try {
                updateEnodeClusterInfo();
            } catch (Exception ex) {
                log.error("Update Cluster info error:{}", ex);
            }
        }
        return this.clusterInfo.getClusterAddrTable().get(clusterName);
    }

    @Override
    public String getAddressByEnodeName(String enodeName,
        boolean isUseSlave) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException {
        if (this.clusterInfo == null) {
            clusterInfo = this.updateEnodeClusterInfo();
        }
        if (this.clusterInfo != null) {
            return this.clusterInfo.getBrokerAddrTable().get(enodeName).getBrokerAddrs().get(MixAll.MASTER_ID);
        }
        return null;
    }
}
