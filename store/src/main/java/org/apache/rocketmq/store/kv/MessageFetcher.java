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

package org.apache.rocketmq.store.kv;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.function.BiFunction;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

public class MessageFetcher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final RemotingClient client;

    public MessageFetcher() {
        NettyClientConfig nettyClientConfig = new NettyClientConfig();
        nettyClientConfig.setUseTLS(false);
        this.client = new NettyRemotingClient(nettyClientConfig);
        this.client.start();
    }

    @Override
    public void close() throws IOException {
        this.client.shutdown();
    }

    private PullMessageRequestHeader createPullMessageRequest(String topic, int queueId, long queueOffset, long subVersion) {
        int sysFlag = PullSysFlag.buildSysFlag(false, false, false, false, true);

        PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
        requestHeader.setConsumerGroup(getConsumerGroup(topic, queueId));
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        requestHeader.setQueueOffset(queueOffset);
        requestHeader.setMaxMsgNums(10);
        requestHeader.setSysFlag(sysFlag);
        requestHeader.setCommitOffset(0L);
        requestHeader.setSuspendTimeoutMillis(20_000L);
//        requestHeader.setSubscription(subExpression);
        requestHeader.setSubVersion(subVersion);
        requestHeader.setMaxMsgBytes(Integer.MAX_VALUE);
//        requestHeader.setExpressionType(expressionType);
        return requestHeader;
    }

    private String getConsumerGroup(String topic, int queueId) {
        return String.join("-", topic, String.valueOf(queueId), "pull", "group");
    }

    private String getClientId() {
        return String.join("@", NetworkUtil.getLocalAddress(), "compactionIns", "compactionUnit");
    }

    private boolean prepare(String masterAddr, String topic, String groupName, long subVersion)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        HeartbeatData heartbeatData = new HeartbeatData();

        heartbeatData.setClientID(getClientId());

        ConsumerData consumerData = new ConsumerData();
        consumerData.setGroupName(groupName);
        consumerData.setConsumeType(ConsumeType.CONSUME_ACTIVELY);
        consumerData.setMessageModel(MessageModel.CLUSTERING);
        consumerData.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
//        consumerData.setSubscriptionDataSet();
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(SubscriptionData.SUB_ALL);
        subscriptionData.setSubVersion(subVersion);
        consumerData.setSubscriptionDataSet(Sets.newHashSet(subscriptionData));

        heartbeatData.getConsumerDataSet().add(consumerData);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        request.setLanguage(LanguageCode.JAVA);
        request.setBody(heartbeatData.encode());

        RemotingCommand response = client.invokeSync(masterAddr, request, 1000 * 30L);
        if (response != null && response.getCode() == ResponseCode.SUCCESS) {
            return true;
        }
        return false;
    }

    private boolean pullDone(String masterAddr, String groupName)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        UnregisterClientRequestHeader requestHeader = new UnregisterClientRequestHeader();
        requestHeader.setClientID(getClientId());
        requestHeader.setProducerGroup("");
        requestHeader.setConsumerGroup(groupName);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, requestHeader);

        RemotingCommand response = client.invokeSync(masterAddr, request, 1000 * 30L);
        if (response != null && response.getCode() == ResponseCode.SUCCESS) {
            return true;
        }
        return false;
    }

    private boolean stopPull(long currPullOffset, long endOffset) {
        return currPullOffset >= endOffset && endOffset != -1;
    }

    public void pullMessageFromMaster(String topic, int queueId, long endOffset, String masterAddr,
                                      BiFunction<Long, RemotingCommand, Boolean> responseHandler) throws Exception {
        long currentPullOffset = 0;

        try {
            long subVersion = System.currentTimeMillis();
            String groupName = getConsumerGroup(topic, queueId);
            if (!prepare(masterAddr, topic, groupName, subVersion)) {
                log.error("{}:{} prepare to {} pull message failed", topic, queueId, masterAddr);
                throw new RemotingCommandException(topic + ":" + queueId + " prepare to " + masterAddr + " pull message failed");
            }

            boolean noNewMsg = false;
            boolean keepPull = true;
//            PullMessageRequestHeader requestHeader = createPullMessageRequest(topic, queueId, subVersion, currentPullOffset);
            while (!stopPull(currentPullOffset, endOffset)) {
//                requestHeader.setQueueOffset(currentPullOffset);
                PullMessageRequestHeader requestHeader = createPullMessageRequest(topic, queueId, currentPullOffset, subVersion);

                RemotingCommand
                        request = RemotingCommand.createRequestCommand(RequestCode.LITE_PULL_MESSAGE, requestHeader);
                RemotingCommand response = client.invokeSync(masterAddr, request, 1000 * 30L);

                PullMessageResponseHeader responseHeader =
                        (PullMessageResponseHeader) response.decodeCommandCustomHeader(PullMessageResponseHeader.class);
                if (responseHeader == null) {
                    log.error("{}:{} pull message responseHeader is null", topic, queueId);
                    throw new RemotingCommandException(topic + ":" + queueId + " pull message responseHeader is null");
                }

                switch (response.getCode()) {
                    case ResponseCode.SUCCESS:
                        long curOffset = responseHeader.getNextBeginOffset() - 1;
                        keepPull = responseHandler.apply(curOffset, response);
                        currentPullOffset = responseHeader.getNextBeginOffset();
                        break;
                    case ResponseCode.PULL_NOT_FOUND:       // NO_NEW_MSG, need break loop
                        log.info("PULL_NOT_FOUND, topic:{}, queueId:{}, pullOffset:{},",
                                topic, queueId, currentPullOffset);
                        noNewMsg = true;
                        break;
                    case ResponseCode.PULL_RETRY_IMMEDIATELY:
                        log.info("PULL_RETRY_IMMEDIATE, topic:{}, queueId:{}, pullOffset:{},",
                                topic, queueId, currentPullOffset);
                        break;
                    case ResponseCode.PULL_OFFSET_MOVED:
                        log.info("PULL_OFFSET_MOVED, topic:{}, queueId:{}, pullOffset:{},",
                                topic, queueId, currentPullOffset);
                        break;
                    default:
                        log.warn("Pull Message error, response code: {}, remark: {}",
                                response.getCode(), response.getRemark());
                }

                if (noNewMsg || !keepPull) {
                    break;
                }
            }
            pullDone(masterAddr, groupName);
        } finally {
            if (client != null) {
                client.closeChannels(Lists.newArrayList(masterAddr));
            }
        }
    }
}
