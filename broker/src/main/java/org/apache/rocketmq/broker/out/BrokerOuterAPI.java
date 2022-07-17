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
package org.apache.rocketmq.broker.out;

import org.apache.rocketmq.broker.latency.BrokerFixedThreadPoolExecutor;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.AbstractBrokerRunnable;
import org.apache.rocketmq.common.BrokerIdentity;
import org.apache.rocketmq.common.BrokerSyncInfo;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.LockCallback;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UnlockCallback;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.namesrv.DefaultTopAddressing;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.namesrv.TopAddressing;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.GetBrokerMemberGroupResponseBody;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.LockBatchResponseBody;
import org.apache.rocketmq.common.protocol.body.MessageRequestModeSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.RegisterBrokerBody;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigAndMappingSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.header.ExchangeHAInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.ExchangeHAInfoResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetBrokerMemberGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMaxOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMinOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeaderV2;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.BrokerHeartbeatRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.QueryDataVersionRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.QueryDataVersionResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.UnRegisterBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.rpc.ClientMetadata;
import org.apache.rocketmq.common.rpc.RpcClient;
import org.apache.rocketmq.common.rpc.RpcClientImpl;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class BrokerOuterAPI {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final RemotingClient remotingClient;
    private final TopAddressing topAddressing = new DefaultTopAddressing(MixAll.getWSAddr());
    private String nameSrvAddr = null;
    private BrokerFixedThreadPoolExecutor brokerOuterExecutor = new BrokerFixedThreadPoolExecutor(4, 10, 1, TimeUnit.MINUTES,
        new ArrayBlockingQueue<>(32), new ThreadFactoryImpl("brokerOutApi_thread_", true));

    private ClientMetadata clientMetadata;
    private RpcClient rpcClient;

    public BrokerOuterAPI(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null, new ClientMetadata());
    }

    private BrokerOuterAPI(final NettyClientConfig nettyClientConfig, RPCHook rpcHook, ClientMetadata clientMetadata) {
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
        this.clientMetadata = clientMetadata;
        this.remotingClient.registerRPCHook(rpcHook);
        this.rpcClient = new RpcClientImpl(this.clientMetadata, this.remotingClient);
    }

    public void start() {
        this.remotingClient.start();
    }

    public void shutdown() {
        this.remotingClient.shutdown();
        this.brokerOuterExecutor.shutdown();
    }

    public List<String> getNameServerAddressList() {
        return this.remotingClient.getNameServerAddressList();
    }

    public String fetchNameServerAddr() {
        try {
            String addrs = this.topAddressing.fetchNSAddr();
            if (!UtilAll.isBlank(addrs)) {
                if (!addrs.equals(this.nameSrvAddr)) {
                    LOGGER.info("name server address changed, old: {} new: {}", this.nameSrvAddr, addrs);
                    this.updateNameServerAddressList(addrs);
                    this.nameSrvAddr = addrs;
                    return nameSrvAddr;
                }
            }
        } catch (Exception e) {
            LOGGER.error("fetchNameServerAddr Exception", e);
        }
        return nameSrvAddr;
    }

    public void updateNameServerAddressList(final String addrs) {
        String[] addrArray = addrs.split(";");
        List<String> lst = new ArrayList<String>(Arrays.asList(addrArray));
        this.remotingClient.updateNameServerAddressList(lst);
    }

    public BrokerMemberGroup syncBrokerMemberGroup(String clusterName, String brokerName)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        return syncBrokerMemberGroup(clusterName, brokerName, false);
    }

    public BrokerMemberGroup syncBrokerMemberGroup(String clusterName, String brokerName,
        boolean isCompatibleWithOldNameSrv)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        if (isCompatibleWithOldNameSrv) {
            return getBrokerMemberGroupCompatible(clusterName, brokerName);
        } else {
            return getBrokerMemberGroup(clusterName, brokerName);
        }
    }

    public BrokerMemberGroup getBrokerMemberGroup(String clusterName, String brokerName)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        BrokerMemberGroup brokerMemberGroup = new BrokerMemberGroup(clusterName, brokerName);

        GetBrokerMemberGroupRequestHeader requestHeader = new GetBrokerMemberGroupRequestHeader();
        requestHeader.setClusterName(clusterName);
        requestHeader.setBrokerName(brokerName);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_MEMBER_GROUP, requestHeader);

        RemotingCommand response = null;
        response = this.remotingClient.invokeSync(null, request, 3000);
        assert response != null;

        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    GetBrokerMemberGroupResponseBody brokerMemberGroupResponseBody =
                        GetBrokerMemberGroupResponseBody.decode(body, GetBrokerMemberGroupResponseBody.class);

                    return brokerMemberGroupResponseBody.getBrokerMemberGroup();
                }
            }
            default:
                break;
        }

        return brokerMemberGroup;
    }

    public BrokerMemberGroup getBrokerMemberGroupCompatible(String clusterName, String brokerName)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        BrokerMemberGroup brokerMemberGroup = new BrokerMemberGroup(clusterName, brokerName);

        GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
        requestHeader.setTopic(TopicValidator.SYNC_BROKER_MEMBER_GROUP_PREFIX + brokerName);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, requestHeader);

        RemotingCommand response;
        response = this.remotingClient.invokeSync(null, request, 3000);
        assert response != null;

        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    TopicRouteData topicRouteData = TopicRouteData.decode(body, TopicRouteData.class);
                    for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
                        if (brokerData != null
                            && brokerData.getBrokerName().equals(brokerName)
                            && brokerData.getCluster().equals(clusterName)) {
                            brokerMemberGroup.getBrokerAddrs().putAll(brokerData.getBrokerAddrs());
                            break;
                        }
                    }
                    return brokerMemberGroup;
                }
            }
            default:
                break;
        }

        return brokerMemberGroup;
    }

    public void sendHeartbeatViaDataVersion(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final Long brokerId,
        final int timeoutMillis,
        final DataVersion dataVersion,
        final boolean isInBrokerContainer) {
        List<String> nameServerAddressList = this.remotingClient.getAvailableNameSrvList();
        if (nameServerAddressList != null && nameServerAddressList.size() > 0) {
            final QueryDataVersionRequestHeader requestHeader = new QueryDataVersionRequestHeader();
            requestHeader.setBrokerAddr(brokerAddr);
            requestHeader.setBrokerName(brokerName);
            requestHeader.setBrokerId(brokerId);
            requestHeader.setClusterName(clusterName);

            for (final String namesrvAddr : nameServerAddressList) {
                brokerOuterExecutor.execute(new AbstractBrokerRunnable(new BrokerIdentity(clusterName, brokerName, brokerId, isInBrokerContainer)) {

                    @Override
                    public void run2() {
                        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_DATA_VERSION, requestHeader);
                        request.setBody(dataVersion.encode());

                        try {
                            BrokerOuterAPI.this.remotingClient.invokeOneway(namesrvAddr, request, timeoutMillis);
                        } catch (Exception e) {
                            LOGGER.error("sendHeartbeat Exception " + namesrvAddr, e);
                        }
                    }
                });
            }
        }
    }

    public void sendHeartbeat(final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final Long brokerId,
        final int timeoutMills,
        final boolean isInBrokerContainer) {
        List<String> nameServerAddressList = this.remotingClient.getAvailableNameSrvList();

        final BrokerHeartbeatRequestHeader requestHeader = new BrokerHeartbeatRequestHeader();
        requestHeader.setClusterName(clusterName);
        requestHeader.setBrokerAddr(brokerAddr);
        requestHeader.setBrokerName(brokerName);

        if (nameServerAddressList != null && nameServerAddressList.size() > 0) {
            for (final String namesrvAddr : nameServerAddressList) {
                brokerOuterExecutor.execute(new AbstractBrokerRunnable(new BrokerIdentity(clusterName, brokerName, brokerId, isInBrokerContainer)) {
                    @Override
                    public void run2() {
                        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.BROKER_HEARTBEAT, requestHeader);

                        try {
                            BrokerOuterAPI.this.remotingClient.invokeOneway(namesrvAddr, request, timeoutMills);
                        } catch (Exception e) {
                            LOGGER.error("sendHeartbeat Exception " + namesrvAddr, e);
                        }
                    }
                });
            }
        }
    }

    public BrokerSyncInfo retrieveBrokerHaInfo(String masterBrokerAddr)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException,
        MQBrokerException, RemotingCommandException {
        ExchangeHAInfoRequestHeader requestHeader = new ExchangeHAInfoRequestHeader();
        requestHeader.setMasterHaAddress(null);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.EXCHANGE_BROKER_HA_INFO, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(masterBrokerAddr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                ExchangeHAInfoResponseHeader responseHeader = (ExchangeHAInfoResponseHeader) response.decodeCommandCustomHeader(ExchangeHAInfoResponseHeader.class);
                return new BrokerSyncInfo(responseHeader.getMasterHaAddress(), responseHeader.getMasterFlushOffset(), responseHeader.getMasterAddress());
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void sendBrokerHaInfo(String brokerAddr, String masterHaAddr, long brokerInitMaxOffset, String masterAddr)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        ExchangeHAInfoRequestHeader requestHeader = new ExchangeHAInfoRequestHeader();
        requestHeader.setMasterHaAddress(masterHaAddr);
        requestHeader.setMasterFlushOffset(brokerInitMaxOffset);
        requestHeader.setMasterAddress(masterAddr);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.EXCHANGE_BROKER_HA_INFO, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(brokerAddr, request, 3000);

        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public List<RegisterBrokerResult> registerBrokerAll(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId,
        final String haServerAddr,
        final TopicConfigSerializeWrapper topicConfigWrapper,
        final List<String> filterServerList,
        final boolean oneway,
        final int timeoutMills,
        final boolean enableActingMaster,
        final boolean compressed,
        final BrokerIdentity brokerIdentity) {
        return registerBrokerAll(clusterName,
            brokerAddr,
            brokerName,
            brokerId,
            haServerAddr,
            topicConfigWrapper,
            filterServerList,
            oneway, timeoutMills,
            enableActingMaster,
            compressed,
            null,
            brokerIdentity);
    }

    /**
     * Considering compression brings much CPU overhead to name server, stream API will not support compression and
     * compression feature is deprecated.
     *
     * @param clusterName
     * @param brokerAddr
     * @param brokerName
     * @param brokerId
     * @param haServerAddr
     * @param topicConfigWrapper
     * @param filterServerList
     * @param oneway
     * @param timeoutMills
     * @param compressed default false
     * @return
     */
    public List<RegisterBrokerResult> registerBrokerAll(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId,
        final String haServerAddr,
        final TopicConfigSerializeWrapper topicConfigWrapper,
        final List<String> filterServerList,
        final boolean oneway,
        final int timeoutMills,
        final boolean enableActingMaster,
        final boolean compressed,
        final Long heartbeatTimeoutMillis,
        final BrokerIdentity brokerIdentity) {

        final List<RegisterBrokerResult> registerBrokerResultList = new CopyOnWriteArrayList<>();
        List<String> nameServerAddressList = this.remotingClient.getAvailableNameSrvList();
        if (nameServerAddressList != null && nameServerAddressList.size() > 0) {

            final RegisterBrokerRequestHeader requestHeader = new RegisterBrokerRequestHeader();
            requestHeader.setBrokerAddr(brokerAddr);
            requestHeader.setBrokerId(brokerId);
            requestHeader.setBrokerName(brokerName);
            requestHeader.setClusterName(clusterName);
            requestHeader.setHaServerAddr(haServerAddr);
            requestHeader.setEnableActingMaster(enableActingMaster);
            requestHeader.setCompressed(false);
            if (heartbeatTimeoutMillis != null) {
                requestHeader.setHeartbeatTimeoutMillis(heartbeatTimeoutMillis);
            }

            RegisterBrokerBody requestBody = new RegisterBrokerBody();
            requestBody.setTopicConfigSerializeWrapper(TopicConfigAndMappingSerializeWrapper.from(topicConfigWrapper));
            requestBody.setFilterServerList(filterServerList);
            final byte[] body = requestBody.encode(compressed);
            final int bodyCrc32 = UtilAll.crc32(body);
            requestHeader.setBodyCrc32(bodyCrc32);
            final CountDownLatch countDownLatch = new CountDownLatch(nameServerAddressList.size());
            for (final String namesrvAddr : nameServerAddressList) {
                brokerOuterExecutor.execute(new AbstractBrokerRunnable(brokerIdentity) {
                    @Override public void run2() {
                        try {
                            RegisterBrokerResult result = registerBroker(namesrvAddr, oneway, timeoutMills, requestHeader, body);
                            if (result != null) {
                                registerBrokerResultList.add(result);
                            }

                            LOGGER.info("Registering current broker to name server completed. TargetHost={}", namesrvAddr);
                        } catch (Exception e) {
                            LOGGER.error("Failed to register current broker to name server. TargetHost={}", namesrvAddr, e);
                        } finally {
                            countDownLatch.countDown();
                        }
                    }
                });
            }

            try {
                if (!countDownLatch.await(timeoutMills, TimeUnit.MILLISECONDS)) {
                    LOGGER.warn("Registration to one or more name servers does NOT complete within deadline. Timeout threshold: {}ms", timeoutMills);
                }
            } catch (InterruptedException ignore) {
            }
        }

        return registerBrokerResultList;
    }

    private RegisterBrokerResult registerBroker(
        final String namesrvAddr,
        final boolean oneway,
        final int timeoutMills,
        final RegisterBrokerRequestHeader requestHeader,
        final byte[] body
    ) throws RemotingCommandException, MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
        InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REGISTER_BROKER, requestHeader);
        request.setBody(body);

        if (oneway) {
            try {
                this.remotingClient.invokeOneway(namesrvAddr, request, timeoutMills);
            } catch (RemotingTooMuchRequestException e) {
                // Ignore
            }
            return null;
        }

        RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMills);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                RegisterBrokerResponseHeader responseHeader =
                    (RegisterBrokerResponseHeader) response.decodeCommandCustomHeader(RegisterBrokerResponseHeader.class);
                RegisterBrokerResult result = new RegisterBrokerResult();
                result.setMasterAddr(responseHeader.getMasterAddr());
                result.setHaServerAddr(responseHeader.getHaServerAddr());
                if (response.getBody() != null) {
                    result.setKvTable(KVTable.decode(response.getBody(), KVTable.class));
                }
                return result;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), requestHeader == null ? null : requestHeader.getBrokerAddr());
    }

    public void unregisterBrokerAll(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId
    ) {
        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null) {
            for (String namesrvAddr : nameServerAddressList) {
                try {
                    this.unregisterBroker(namesrvAddr, clusterName, brokerAddr, brokerName, brokerId);
                    LOGGER.info("unregisterBroker OK, NamesrvAddr: {}", namesrvAddr);
                } catch (Exception e) {
                    LOGGER.warn("unregisterBroker Exception, {}", namesrvAddr, e);
                }
            }
        }
    }

    public void unregisterBroker(
        final String namesrvAddr,
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId
    ) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        UnRegisterBrokerRequestHeader requestHeader = new UnRegisterBrokerRequestHeader();
        requestHeader.setBrokerAddr(brokerAddr);
        requestHeader.setBrokerId(brokerId);
        requestHeader.setBrokerName(brokerName);
        requestHeader.setClusterName(clusterName);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_BROKER, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), brokerAddr);
    }

    public List<Boolean> needRegister(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId,
        final TopicConfigSerializeWrapper topicConfigWrapper,
        final int timeoutMills,
        final boolean isInBrokerContainer) {
        final List<Boolean> changedList = new CopyOnWriteArrayList<>();
        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null && nameServerAddressList.size() > 0) {
            final CountDownLatch countDownLatch = new CountDownLatch(nameServerAddressList.size());
            for (final String namesrvAddr : nameServerAddressList) {
                brokerOuterExecutor.execute(new AbstractBrokerRunnable(new BrokerIdentity(clusterName, brokerName, brokerId, isInBrokerContainer)) {
                    @Override public void run2() {
                        try {
                            QueryDataVersionRequestHeader requestHeader = new QueryDataVersionRequestHeader();
                            requestHeader.setBrokerAddr(brokerAddr);
                            requestHeader.setBrokerId(brokerId);
                            requestHeader.setBrokerName(brokerName);
                            requestHeader.setClusterName(clusterName);
                            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_DATA_VERSION, requestHeader);
                            request.setBody(topicConfigWrapper.getDataVersion().encode());
                            RemotingCommand response = remotingClient.invokeSync(namesrvAddr, request, timeoutMills);
                            DataVersion nameServerDataVersion = null;
                            Boolean changed = false;
                            switch (response.getCode()) {
                                case ResponseCode.SUCCESS: {
                                    QueryDataVersionResponseHeader queryDataVersionResponseHeader =
                                        (QueryDataVersionResponseHeader) response.decodeCommandCustomHeader(QueryDataVersionResponseHeader.class);
                                    changed = queryDataVersionResponseHeader.getChanged();
                                    byte[] body = response.getBody();
                                    if (body != null) {
                                        nameServerDataVersion = DataVersion.decode(body, DataVersion.class);
                                        if (!topicConfigWrapper.getDataVersion().equals(nameServerDataVersion)) {
                                            changed = true;
                                        }
                                    }
                                    if (changed == null || changed) {
                                        changedList.add(Boolean.TRUE);
                                    }
                                }
                                default:
                                    break;
                            }
                            LOGGER.warn("Query data version from name server {} OK, changed {}, broker {},name server {}", namesrvAddr, changed, topicConfigWrapper.getDataVersion(), nameServerDataVersion == null ? "" : nameServerDataVersion);
                        } catch (Exception e) {
                            changedList.add(Boolean.TRUE);
                            LOGGER.error("Query data version from name server {}  Exception, {}", namesrvAddr, e);
                        } finally {
                            countDownLatch.countDown();
                        }
                    }
                });

            }
            try {
                countDownLatch.await(timeoutMills, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOGGER.error("query dataversion from nameserver countDownLatch await Exception", e);
            }
        }
        return changedList;
    }

    public TopicConfigAndMappingSerializeWrapper getAllTopicConfig(
        final String addr) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, InterruptedException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG, null);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(true, addr), request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return TopicConfigSerializeWrapper.decode(response.getBody(), TopicConfigAndMappingSerializeWrapper.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public ConsumerOffsetSerializeWrapper getAllConsumerOffset(
        final String addr) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_CONSUMER_OFFSET, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return ConsumerOffsetSerializeWrapper.decode(response.getBody(), ConsumerOffsetSerializeWrapper.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public String getAllDelayOffset(
        final String addr) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingConnectException, MQBrokerException, UnsupportedEncodingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_DELAY_OFFSET, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return new String(response.getBody(), MixAll.DEFAULT_CHARSET);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public SubscriptionGroupWrapper getAllSubscriptionGroupConfig(
        final String addr) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return SubscriptionGroupWrapper.decode(response.getBody(), SubscriptionGroupWrapper.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public void registerRPCHook(RPCHook rpcHook) {
        remotingClient.registerRPCHook(rpcHook);
    }

    public void clearRPCHook() {
        remotingClient.clearRPCHook();
    }

    public long getMaxOffset(final String addr, final String topic, final int queueId, final boolean committed,
        final boolean isOnlyThisBroker)
        throws RemotingException, MQBrokerException, InterruptedException {
        GetMaxOffsetRequestHeader requestHeader = new GetMaxOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        requestHeader.setCommitted(committed);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MAX_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetMaxOffsetResponseHeader responseHeader = (GetMaxOffsetResponseHeader) response.decodeCommandCustomHeader(GetMaxOffsetResponseHeader.class);

                return responseHeader.getOffset();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public long getMinOffset(final String addr, final String topic, final int queueId, final boolean isOnlyThisBroker)
        throws RemotingException, MQBrokerException, InterruptedException {
        GetMinOffsetRequestHeader requestHeader = new GetMinOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MIN_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetMinOffsetResponseHeader responseHeader = (GetMinOffsetResponseHeader) response.decodeCommandCustomHeader(GetMinOffsetResponseHeader.class);

                return responseHeader.getOffset();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void lockBatchMQAsync(
        final String addr,
        final LockBatchRequestBody requestBody,
        final long timeoutMillis,
        final LockCallback callback) throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);

        request.setBody(requestBody.encode());
        this.remotingClient.invokeAsync(addr, request, timeoutMillis, responseFuture -> {
            if (callback == null) {
                return;
            }

            try {
                RemotingCommand response = responseFuture.getResponseCommand();
                if (response != null) {
                    if (response.getCode() == ResponseCode.SUCCESS) {
                        LockBatchResponseBody responseBody = LockBatchResponseBody.decode(response.getBody(),
                            LockBatchResponseBody.class);
                        Set<MessageQueue> messageQueues = responseBody.getLockOKMQSet();
                        callback.onSuccess(messageQueues);
                    } else {
                        callback.onException(new MQBrokerException(response.getCode(), response.getRemark()));
                    }
                }
            } catch (Throwable ignored) {

            }
        });
    }

    public void unlockBatchMQAsync(
        final String addr,
        final UnlockBatchRequestBody requestBody,
        final long timeoutMillis,
        final UnlockCallback callback) throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNLOCK_BATCH_MQ, null);

        request.setBody(requestBody.encode());

        this.remotingClient.invokeAsync(addr, request, timeoutMillis, responseFuture -> {
            if (callback == null) {
                return;
            }

            try {
                RemotingCommand response = responseFuture.getResponseCommand();
                if (response != null) {
                    if (response.getCode() == ResponseCode.SUCCESS) {
                        callback.onSuccess();
                    } else {
                        callback.onException(new MQBrokerException(response.getCode(), response.getRemark()));
                    }
                }
            } catch (Throwable ignored) {

            }
        });
    }

    public RemotingClient getRemotingClient() {
        return this.remotingClient;
    }

    public SendResult sendMessageToSpecificBroker(String brokerAddr, final String brokerName,
        final MessageExt msg, String group,
        long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException {

        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        requestHeader.setProducerGroup(group);
        requestHeader.setTopic(msg.getTopic());
        requestHeader.setDefaultTopic(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC);
        requestHeader.setDefaultTopicQueueNums(8);
        requestHeader.setQueueId(msg.getQueueId());
        requestHeader.setSysFlag(msg.getSysFlag());
        requestHeader.setBornTimestamp(msg.getBornTimestamp());
        requestHeader.setFlag(msg.getFlag());
        requestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));
        requestHeader.setReconsumeTimes(msg.getReconsumeTimes());
        requestHeader.setBatch(false);

        SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(requestHeader);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, requestHeaderV2);

        request.setBody(msg.getBody());

        RemotingCommand response = this.remotingClient.invokeSync(brokerAddr, request, timeoutMillis);

        return this.processSendResponse(brokerName, msg, response);
    }

    private SendResult processSendResponse(
        final String brokerName,
        final Message msg,
        final RemotingCommand response
    ) throws MQBrokerException, RemotingCommandException {
        switch (response.getCode()) {
            case ResponseCode.FLUSH_DISK_TIMEOUT:
            case ResponseCode.FLUSH_SLAVE_TIMEOUT:
            case ResponseCode.SLAVE_NOT_AVAILABLE:
            case ResponseCode.SUCCESS: {
                SendStatus sendStatus = SendStatus.SEND_OK;
                switch (response.getCode()) {
                    case ResponseCode.FLUSH_DISK_TIMEOUT:
                        sendStatus = SendStatus.FLUSH_DISK_TIMEOUT;
                        break;
                    case ResponseCode.FLUSH_SLAVE_TIMEOUT:
                        sendStatus = SendStatus.FLUSH_SLAVE_TIMEOUT;
                        break;
                    case ResponseCode.SLAVE_NOT_AVAILABLE:
                        sendStatus = SendStatus.SLAVE_NOT_AVAILABLE;
                        break;
                    case ResponseCode.SUCCESS:
                        sendStatus = SendStatus.SEND_OK;
                        break;
                    default:
                        assert false;
                        break;
                }

                SendMessageResponseHeader responseHeader =
                    (SendMessageResponseHeader) response.decodeCommandCustomHeader(SendMessageResponseHeader.class);

                //If namespace not null , reset Topic without namespace.
                String topic = msg.getTopic();

                MessageQueue messageQueue = new MessageQueue(topic, brokerName, responseHeader.getQueueId());

                String uniqMsgId = MessageClientIDSetter.getUniqID(msg);
                if (msg instanceof MessageBatch) {
                    StringBuilder sb = new StringBuilder();
                    for (Message message : (MessageBatch) msg) {
                        sb.append(sb.length() == 0 ? "" : ",").append(MessageClientIDSetter.getUniqID(message));
                    }
                    uniqMsgId = sb.toString();
                }
                SendResult sendResult = new SendResult(sendStatus,
                    uniqMsgId,
                    responseHeader.getMsgId(), messageQueue, responseHeader.getQueueOffset());
                sendResult.setTransactionId(responseHeader.getTransactionId());
                String regionId = response.getExtFields().get(MessageConst.PROPERTY_MSG_REGION);
                String traceOn = response.getExtFields().get(MessageConst.PROPERTY_TRACE_SWITCH);
                if (regionId == null || regionId.isEmpty()) {
                    regionId = MixAll.DEFAULT_TRACE_REGION_ID;
                }
                if (traceOn != null && traceOn.equals("false")) {
                    sendResult.setTraceOn(false);
                } else {
                    sendResult.setTraceOn(true);
                }
                sendResult.setRegionId(regionId);
                return sendResult;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public BrokerFixedThreadPoolExecutor getBrokerOuterExecutor() {
        return brokerOuterExecutor;
    }

    public TopicRouteData getTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis)
        throws RemotingException, MQBrokerException, InterruptedException {
        return getTopicRouteInfoFromNameServer(topic, timeoutMillis, true);
    }

    public TopicRouteData getTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis,
        boolean allowTopicNotExist) throws MQBrokerException, InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.TOPIC_NOT_EXIST: {
                if (allowTopicNotExist) {
                    LOGGER.warn("get Topic [{}] RouteInfoFromNameServer is not exist value", topic);
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

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public ClusterInfo getBrokerClusterInfo() throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, 3_000);
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

    public void forwardRequest(String brokerAddr, RemotingCommand request, long timeoutMillis,
        InvokeCallback invokeCallback) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException, RemotingTooMuchRequestException, RemotingConnectException {
        this.remotingClient.invokeAsync(brokerAddr, request, timeoutMillis, invokeCallback);
    }

    public void refreshMetadata() throws Exception {
        ClusterInfo brokerClusterInfo = getBrokerClusterInfo();
        clientMetadata.refreshClusterInfo(brokerClusterInfo);
    }

    public ClientMetadata getClientMetadata() {
        return clientMetadata;
    }

    public RpcClient getRpcClient() {
        return rpcClient;
    }

    public MessageRequestModeSerializeWrapper getAllMessageRequestMode(
        final String addr) throws RemotingSendRequestException, RemotingConnectException,
        MQBrokerException, RemotingTimeoutException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_MESSAGE_REQUEST_MODE, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return MessageRequestModeSerializeWrapper.decode(response.getBody(), MessageRequestModeSerializeWrapper.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }
}
