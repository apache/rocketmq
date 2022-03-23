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
package org.apache.rocketmq.client.impl;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.AckCallback;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.PopCallback;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseBody;
import org.apache.rocketmq.common.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMaxOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SearchOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.SearchOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQClientAPIExt {
    private static final Logger LOGGER = LoggerFactory.getLogger(MQClientAPIExt.class);

    private final ClientConfig clientConfig;
    private final MQClientAPIImpl mqClientAPI;

    public MQClientAPIExt(
        ClientConfig clientConfig,
        NettyClientConfig nettyClientConfig,
        ClientRemotingProcessor clientRemotingProcessor,
        RPCHook rpcHook
    ) {
        this.clientConfig = clientConfig;
        this.mqClientAPI = new MQClientAPIImpl(nettyClientConfig, clientRemotingProcessor, rpcHook, clientConfig);
    }

    public void start() {
        this.mqClientAPI.start();
    }

    public void shutdown() {
        this.mqClientAPI.shutdown();
    }

    public void fetchNameServerAddr() {
        this.mqClientAPI.fetchNameServerAddr();
    }

    public boolean updateNameServerAddressList() {
        if (this.clientConfig.getNamesrvAddr() != null) {
            this.mqClientAPI.updateNameServerAddressList(this.clientConfig.getNamesrvAddr());
            LOGGER.info("user specified name server address: {}", this.clientConfig.getNamesrvAddr());
            return true;
        }
        return false;
    }

    protected static MQClientException processNullResponseErr(ResponseFuture responseFuture) {
        MQClientException ex;
        if (!responseFuture.isSendRequestOK()) {
            ex = new MQClientException("send request failed", responseFuture.getCause());
        } else if (responseFuture.isTimeout()) {
            ex = new MQClientException("wait response timeout " + responseFuture.getTimeoutMillis() + "ms",
                responseFuture.getCause());
        } else {
            ex = new MQClientException("unknown reason", responseFuture.getCause());
        }
        return ex;
    }

    protected RemotingClient getRemotingClient() {
        return this.mqClientAPI.getRemotingClient();
    }

    public CompletableFuture<Integer> sendHeartbeat(
        String brokerAddr,
        HeartbeatData heartbeatData,
        long timeoutMillis
    ) {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        request.setLanguage(clientConfig.getLanguage());
        request.setBody(heartbeatData.encode());

        CompletableFuture<Integer> future = new CompletableFuture<>();
        try {
            this.getRemotingClient().invokeAsync(brokerAddr, request, timeoutMillis, responseFuture -> {
                RemotingCommand response = responseFuture.getResponseCommand();
                if (response != null) {
                    if (ResponseCode.SUCCESS == response.getCode()) {
                        future.complete(response.getVersion());
                    } else {
                        future.completeExceptionally(new MQBrokerException(response.getCode(), response.getRemark(), brokerAddr));
                    }
                } else {
                    future.completeExceptionally(processNullResponseErr(responseFuture));
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public void endTransactionOneway(String brokerAddr, EndTransactionRequestHeader requestHeader, String remark, long timeoutMillis)
        throws MQBrokerException, RemotingException, InterruptedException {
        this.mqClientAPI.endTransactionOneway(brokerAddr, requestHeader, remark, timeoutMillis);
    }

    public CompletableFuture<SendResult> sendMessage(
        String brokerAddr,
        String brokerName,
        Message msg,
        SendMessageRequestHeader requestHeader,
        long timeoutMillis
    ) {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
        request.setBody(msg.getBody());

        CompletableFuture<SendResult> future = new CompletableFuture<>();
        try {
            this.getRemotingClient().invokeAsync(brokerAddr, request, timeoutMillis, responseFuture -> {
                RemotingCommand response = responseFuture.getResponseCommand();
                if (response != null) {
                    try {
                        future.complete(mqClientAPI.processSendResponse(brokerName, msg, response, brokerAddr));
                    } catch (Exception e) {
                        future.completeExceptionally(e);
                    }
                } else {
                    future.completeExceptionally(processNullResponseErr(responseFuture));
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<RemotingCommand> sendMessageBack(
        String brokerAddr,
        ConsumerSendMsgBackRequestHeader requestHeader,
        long timeoutMillis
    ) {
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        try {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, requestHeader);
            this.getRemotingClient().invokeAsync(brokerAddr, request, timeoutMillis, responseFuture -> {
                RemotingCommand response = responseFuture.getResponseCommand();
                if (response != null) {
                    future.complete(response);
                } else {
                    future.completeExceptionally(processNullResponseErr(responseFuture));
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<PopResult> popMessage(
        String brokerAddr,
        String brokerName,
        PopMessageRequestHeader requestHeader,
        long timeoutMillis
    ) {
        CompletableFuture<PopResult> future = new CompletableFuture<>();
        try {
            this.mqClientAPI.popMessageAsync(brokerName, brokerAddr, requestHeader, timeoutMillis, new PopCallback() {
                @Override
                public void onSuccess(PopResult popResult) {
                    future.complete(popResult);
                }

                @Override
                public void onException(Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<AckResult> ackMessage(
        String brokerAddr,
        AckMessageRequestHeader requestHeader,
        long timeoutMillis
    ) {
        CompletableFuture<AckResult> future = new CompletableFuture<>();
        try {
            this.mqClientAPI.ackMessageAsync(brokerAddr, timeoutMillis, new AckCallback() {
                @Override
                public void onSuccess(AckResult ackResult) {
                    future.complete(ackResult);
                }

                @Override
                public void onException(Throwable t) {
                    future.completeExceptionally(t);
                }
            }, requestHeader);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<AckResult> changeInvisibleTimeAsync(
        String brokerAddr,
        String brokerName,
        ChangeInvisibleTimeRequestHeader requestHeader,
        long timeoutMillis
    ) {
        CompletableFuture<AckResult> future = new CompletableFuture<>();
        try {
            this.mqClientAPI.changeInvisibleTimeAsync(brokerName, brokerAddr, requestHeader, timeoutMillis,
                new AckCallback() {
                    @Override
                    public void onSuccess(AckResult ackResult) {
                        future.complete(ackResult);
                    }

                    @Override
                    public void onException(Throwable t) {
                        future.completeExceptionally(t);
                    }
                }
            );
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<PullResult> pullMessage(
        String brokerAddr,
        PullMessageRequestHeader requestHeader,
        long timeoutMillis
    ) {
        CompletableFuture<PullResult> future = new CompletableFuture<>();
        try {
            this.mqClientAPI.pullMessage(brokerAddr, requestHeader, timeoutMillis, CommunicationMode.ASYNC,
                new PullCallback() {
                    @Override
                    public void onSuccess(PullResult pullResult) {
                        future.complete(pullResult);
                    }

                    @Override
                    public void onException(Throwable t) {
                        future.completeExceptionally(t);
                    }
                }
            );
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public void updateConsumerOffsetOneWay(
        String brokerAddr,
        UpdateConsumerOffsetRequestHeader header,
        long timeoutMillis
    ) throws InterruptedException, RemotingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, header);
        this.getRemotingClient().invokeOneway(brokerAddr, request, timeoutMillis);
    }

    public CompletableFuture<List<String>> getConsumerListByGroup(
        String brokerAddr,
        GetConsumerListByGroupRequestHeader requestHeader,
        long timeoutMillis
    ) {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP, requestHeader);

        CompletableFuture<List<String>> future = new CompletableFuture<>();
        try {
            this.getRemotingClient().invokeAsync(brokerAddr, request, timeoutMillis, responseFuture -> {
                RemotingCommand response = responseFuture.getResponseCommand();
                if (response != null) {
                    switch (response.getCode()) {
                        case ResponseCode.SUCCESS: {
                            if (response.getBody() != null) {
                                GetConsumerListByGroupResponseBody body =
                                    GetConsumerListByGroupResponseBody.decode(response.getBody(), GetConsumerListByGroupResponseBody.class);
                                future.complete(body.getConsumerIdList());
                                return;
                            }
                        }
                        /**
                         * @see org.apache.rocketmq.broker.processor.ConsumerManageProcessor#getConsumerListByGroup,
                         * broker will return {@link ResponseCode.SYSTEM_ERROR} if there is no consumer.
                         */
                        case ResponseCode.SYSTEM_ERROR: {
                            future.complete(Collections.emptyList());
                            return;
                        }
                        default:
                            break;
                    }
                    future.completeExceptionally(new MQBrokerException(response.getCode(), response.getRemark()));
                } else {
                    future.completeExceptionally(processNullResponseErr(responseFuture));
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public TopicRouteData getTopicRouteInfoFromNameServer(String topic, long timeoutMillis)
        throws RemotingException, InterruptedException, MQClientException {
        return this.mqClientAPI.getTopicRouteInfoFromNameServer(topic, timeoutMillis);
    }

    public CompletableFuture<Long> getMaxOffset(String brokerAddr, String topic, int queueId, long timeoutMillis) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        try {
            GetMaxOffsetRequestHeader requestHeader = new GetMaxOffsetRequestHeader();
            requestHeader.setTopic(topic);
            requestHeader.setQueueId(queueId);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MAX_OFFSET, requestHeader);
            this.getRemotingClient().invokeAsync(brokerAddr, request, timeoutMillis, responseFuture -> {
                RemotingCommand response = responseFuture.getResponseCommand();
                if (response != null) {
                    if (ResponseCode.SUCCESS == response.getCode()) {
                        try {
                            GetMaxOffsetResponseHeader responseHeader =
                                (GetMaxOffsetResponseHeader) response.decodeCommandCustomHeader(GetMaxOffsetResponseHeader.class);
                            future.complete(responseHeader.getOffset());
                        } catch (Throwable t) {
                            future.completeExceptionally(t);
                        }
                    }
                    future.completeExceptionally(new MQBrokerException(response.getCode(), response.getRemark()));
                } else {
                    future.completeExceptionally(processNullResponseErr(responseFuture));
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<Long> searchOffset(String brokerAddr, String topic, int queueId , long timestamp, long timeoutMillis) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        try {
            SearchOffsetRequestHeader requestHeader = new SearchOffsetRequestHeader();
            requestHeader.setTopic(topic);
            requestHeader.setQueueId(queueId);
            requestHeader.setTimestamp(timestamp);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, requestHeader);
            this.getRemotingClient().invokeAsync(brokerAddr, request, timeoutMillis, responseFuture -> {
                RemotingCommand response = responseFuture.getResponseCommand();
                if (response != null) {
                    if (response.getCode() == ResponseCode.SUCCESS) {
                        try {
                            SearchOffsetResponseHeader responseHeader =
                                (SearchOffsetResponseHeader) response.decodeCommandCustomHeader(SearchOffsetResponseHeader.class);
                            future.complete(responseHeader.getOffset());
                        } catch (Throwable t) {
                            future.completeExceptionally(t);
                        }
                    }
                    future.completeExceptionally(new MQBrokerException(response.getCode(), response.getRemark()));
                } else {
                    future.completeExceptionally(processNullResponseErr(responseFuture));
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

}
