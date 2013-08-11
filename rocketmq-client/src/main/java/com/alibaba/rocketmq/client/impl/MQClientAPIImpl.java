/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.client.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.consumer.PullCallback;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.PullStatus;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.consumer.PullResultExt;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.common.MQVersion;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.admin.TopicStatsTable;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.namesrv.TopAddressing;
import com.alibaba.rocketmq.common.protocol.MQProtos.MQRequestCode;
import com.alibaba.rocketmq.common.protocol.MQProtos.MQResponseCode;
import com.alibaba.rocketmq.common.protocol.body.ClusterInfo;
import com.alibaba.rocketmq.common.protocol.body.ConsumerConnection;
import com.alibaba.rocketmq.common.protocol.body.LockBatchRequestBody;
import com.alibaba.rocketmq.common.protocol.body.LockBatchResponseBody;
import com.alibaba.rocketmq.common.protocol.body.ProducerConnection;
import com.alibaba.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import com.alibaba.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.CreateTopicRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetConsumerConnectionListRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetConsumerListByGroupRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetConsumerListByGroupResponseBody;
import com.alibaba.rocketmq.common.protocol.header.GetEarliestMsgStoretimeRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetEarliestMsgStoretimeResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.GetMaxOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetMaxOffsetResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.GetMinOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetMinOffsetResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.GetProducerConnectionListRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetTopicStatsInfoRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.PullMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.PullMessageResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.QueryConsumerOffsetResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.QueryMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.SearchOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.SearchOffsetResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.SendMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.SendMessageResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.ViewMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.WipeWritePermOfBrokerRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.WipeWritePermOfBrokerResponseHeader;
import com.alibaba.rocketmq.common.protocol.heartbeat.HeartbeatData;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.common.protocol.route.QueueData;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.remoting.InvokeCallback;
import com.alibaba.rocketmq.remoting.RemotingClient;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingClient;
import com.alibaba.rocketmq.remoting.netty.ResponseFuture;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.ResponseCode;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * 封装所有与服务器通信部分API
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-24
 */
public class MQClientAPIImpl {
    private final static Logger log = ClientLogger.getLog();
    private final RemotingClient remotingClient;
    private final TopAddressing topAddressing = new TopAddressing();
    private final ClientRemotingProcessor clientRemotingProcessor;
    static {
        System.setProperty(RemotingCommand.RemotingVersionKey, Integer.toString(MQVersion.CurrentVersion));
    }
    private String nameSrvAddr = null;


    public MQClientAPIImpl(final NettyClientConfig nettyClientConfig,
            final ClientRemotingProcessor clientRemotingProcessor) {
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
        this.clientRemotingProcessor = clientRemotingProcessor;

        /**
         * 注册客户端支持的RPC CODE
         */
        this.remotingClient.registerProcessor(MQRequestCode.CHECK_TRANSACTION_STATE_VALUE,
            this.clientRemotingProcessor, null);

        this.remotingClient.registerProcessor(MQRequestCode.NOTIFY_CONSUMER_IDS_CHANGED_VALUE,
            this.clientRemotingProcessor, null);
    }


    public List<String> getNameServerAddressList() {
        return this.remotingClient.getNameServerAddressList();
    }


    public RemotingClient getRemotingClient() {
        return remotingClient;
    }


    public String fetchNameServerAddr() {
        try {
            String addrs = this.topAddressing.fetchNSAddr();
            if (addrs != null) {
                if (!addrs.equals(this.nameSrvAddr)) {
                    log.info("name server address changed, old: " + this.nameSrvAddr + " new: " + addrs);
                    this.updateNameServerAddressList(addrs);
                    this.nameSrvAddr = addrs;
                    return nameSrvAddr;
                }
            }
        }
        catch (Exception e) {
            log.error("fetchNameServerAddr Exception", e);
        }
        return nameSrvAddr;
    }


    public void updateNameServerAddressList(final String addrs) {
        List<String> lst = new ArrayList<String>();
        String[] addrArray = addrs.split(";");
        if (addrArray != null) {
            for (String addr : addrArray) {
                lst.add(addr);
            }

            this.remotingClient.updateNameServerAddressList(lst);
        }
    }


    public void start() {
        this.remotingClient.start();
    }


    public void shutdown() {
        this.remotingClient.shutdown();
    }


    public void createSubscriptionGroup(final String addr, final SubscriptionGroupConfig config,
            final long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException,
            MQClientException {
        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_VALUE,
                    null);

        byte[] body = RemotingSerializable.encode(config);
        request.setBody(body);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
        case ResponseCode.SUCCESS_VALUE: {
            return;
        }
        default:
            break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());

    }


    public void createTopic(final String addr, final String defaultTopic, final TopicConfig topicConfig,
            final long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException,
            MQClientException {
        CreateTopicRequestHeader requestHeader = new CreateTopicRequestHeader();
        requestHeader.setTopic(topicConfig.getTopicName());
        requestHeader.setDefaultTopic(defaultTopic);
        requestHeader.setReadQueueNums(topicConfig.getReadQueueNums());
        requestHeader.setWriteQueueNums(topicConfig.getWriteQueueNums());
        requestHeader.setPerm(topicConfig.getPerm());
        requestHeader.setTopicFilterType(topicConfig.getTopicFilterType().name());

        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.UPDATE_AND_CREATE_TOPIC_VALUE,
                    requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
        case ResponseCode.SUCCESS_VALUE: {
            return;
        }
        default:
            break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }


    /**
     * 发送消息
     */
    public SendResult sendMessage(//
            final String addr,// 1
            final String brokerName,// 2
            final Message msg,// 3
            final SendMessageRequestHeader requestHeader,// 4
            final long timeoutMillis,// 5
            final CommunicationMode communicationMode,// 6
            final SendCallback sendCallback// 7
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.SEND_MESSAGE_VALUE, requestHeader);
        request.setBody(msg.getBody());

        switch (communicationMode) {
        case ONEWAY:
            this.remotingClient.invokeOneway(addr, request, timeoutMillis);
            return null;
        case ASYNC:
            this.sendMessageAsync(addr, brokerName, msg, timeoutMillis, request, sendCallback);
            return null;
        case SYNC:
            return this.sendMessageSync(addr, brokerName, msg, timeoutMillis, request);
        default:
            assert false;
            break;
        }

        return null;
    }


    private SendResult sendMessageSync(//
            final String addr,//
            final String brokerName,//
            final Message msg,//
            final long timeoutMillis,//
            final RemotingCommand request//
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        return this.processSendResponse(brokerName, msg, response);
    }


    private void sendMessageAsync(//
            final String addr,//
            final String brokerName,//
            final Message msg,//
            final long timeoutMillis,//
            final RemotingCommand request,//
            final SendCallback sendCallback//
    ) throws RemotingException, InterruptedException {
        this.remotingClient.invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {
                if (null == sendCallback)
                    return;

                RemotingCommand response = responseFuture.getResponseCommand();
                if (response != null) {
                    try {
                        SendResult sendResult =
                                MQClientAPIImpl.this.processSendResponse(brokerName, msg, response);
                        assert sendResult != null;
                        sendCallback.onSuccess(sendResult);
                    }
                    catch (Exception e) {
                        sendCallback.onException(e);
                    }
                }
                else {
                    if (!responseFuture.isSendRequestOK()) {
                        sendCallback.onException(new MQClientException("send request failed", responseFuture
                            .getCause()));
                    }
                    else if (responseFuture.isTimeout()) {
                        sendCallback.onException(new MQClientException("wait response timeout "
                                + responseFuture.getTimeoutMillis() + "ms", responseFuture.getCause()));
                    }
                    else {
                        sendCallback.onException(new MQClientException("unknow reseaon", responseFuture
                            .getCause()));
                    }
                }
            }
        });
    }


    private SendResult processSendResponse(//
            final String brokerName,//
            final Message msg,//
            final RemotingCommand response//
    ) throws MQBrokerException, RemotingCommandException {
        switch (response.getCode()) {
        case MQResponseCode.FLUSH_DISK_TIMEOUT_VALUE:
        case MQResponseCode.FLUSH_SLAVE_TIMEOUT_VALUE:
        case MQResponseCode.SLAVE_NOT_AVAILABLE_VALUE: {
            // TODO LOG
        }
        case ResponseCode.SUCCESS_VALUE: {
            SendStatus sendStatus = SendStatus.SEND_OK;
            switch (response.getCode()) {
            case MQResponseCode.FLUSH_DISK_TIMEOUT_VALUE:
                sendStatus = SendStatus.FLUSH_DISK_TIMEOUT;
                break;
            case MQResponseCode.FLUSH_SLAVE_TIMEOUT_VALUE:
                sendStatus = SendStatus.FLUSH_SLAVE_TIMEOUT;
                break;
            case MQResponseCode.SLAVE_NOT_AVAILABLE_VALUE:
                sendStatus = SendStatus.SLAVE_NOT_AVAILABLE;
                break;
            case ResponseCode.SUCCESS_VALUE:
                sendStatus = SendStatus.SEND_OK;
                break;
            default:
                assert false;
                break;
            }

            SendMessageResponseHeader responseHeader =
                    (SendMessageResponseHeader) response
                        .decodeCommandCustomHeader(SendMessageResponseHeader.class);

            MessageQueue messageQueue =
                    new MessageQueue(msg.getTopic(), brokerName, responseHeader.getQueueId());
            return new SendResult(sendStatus, responseHeader.getMsgId(), messageQueue,
                responseHeader.getQueueOffset());
        }
        default:
            break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    /**
     * 拉消息接口
     */
    public PullResult pullMessage(//
            final String addr,//
            final PullMessageRequestHeader requestHeader,//
            final long timeoutMillis,//
            final CommunicationMode communicationMode,//
            final PullCallback pullCallback//
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.PULL_MESSAGE_VALUE, requestHeader);

        switch (communicationMode) {
        case ONEWAY:
            assert false;
            return null;
        case ASYNC:
            this.pullMessageAsync(addr, request, timeoutMillis, pullCallback);
            return null;
        case SYNC:
            return this.pullMessageSync(addr, request, timeoutMillis);
        default:
            assert false;
            break;
        }

        return null;
    }


    private void pullMessageAsync(//
            final String addr,// 1
            final RemotingCommand request,//
            final long timeoutMillis,//
            final PullCallback pullCallback//
    ) throws RemotingException, InterruptedException {
        this.remotingClient.invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {
                RemotingCommand response = responseFuture.getResponseCommand();
                if (response != null) {
                    try {
                        PullResult pullResult = MQClientAPIImpl.this.processPullResponse(response);
                        assert pullResult != null;
                        pullCallback.onSuccess(pullResult);
                    }
                    catch (Exception e) {
                        pullCallback.onException(e);
                    }
                }
                else {
                    if (!responseFuture.isSendRequestOK()) {
                        pullCallback.onException(new MQClientException("send request failed", responseFuture
                            .getCause()));
                    }
                    else if (responseFuture.isTimeout()) {
                        pullCallback.onException(new MQClientException("wait response timeout "
                                + responseFuture.getTimeoutMillis() + "ms", responseFuture.getCause()));
                    }
                    else {
                        pullCallback.onException(new MQClientException("unknow reseaon", responseFuture
                            .getCause()));
                    }
                }
            }
        });
    }


    private PullResult processPullResponse(final RemotingCommand response) throws MQBrokerException,
            RemotingCommandException {
        PullStatus pullStatus = PullStatus.NO_NEW_MSG;
        switch (response.getCode()) {
        case ResponseCode.SUCCESS_VALUE:
            pullStatus = PullStatus.FOUND;
            break;
        case MQResponseCode.PULL_NOT_FOUND_VALUE:
            pullStatus = PullStatus.NO_NEW_MSG;
            break;
        case MQResponseCode.PULL_RETRY_IMMEDIATELY_VALUE:
            pullStatus = PullStatus.NO_MATCHED_MSG;
            break;
        case MQResponseCode.PULL_OFFSET_MOVED_VALUE:
            pullStatus = PullStatus.OFFSET_ILLEGAL;
            break;

        default:
            throw new MQBrokerException(response.getCode(), response.getRemark());
        }

        PullMessageResponseHeader responseHeader =
                (PullMessageResponseHeader) response
                    .decodeCommandCustomHeader(PullMessageResponseHeader.class);

        return new PullResultExt(pullStatus, responseHeader.getNextBeginOffset(),
            responseHeader.getMinOffset(), responseHeader.getMaxOffset(), null,
            responseHeader.getSuggestWhichBrokerId(), response.getBody());
    }


    private PullResult pullMessageSync(//
            final String addr,// 1
            final RemotingCommand request,// 2
            final long timeoutMillis// 3
    ) throws RemotingException, InterruptedException, MQBrokerException {
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        return this.processPullResponse(response);
    }


    /**
     * 根据时间查询Offset
     */
    public MessageExt viewMessage(final String addr, final long phyoffset, final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException {
        ViewMessageRequestHeader requestHeader = new ViewMessageRequestHeader();
        requestHeader.setOffset(phyoffset);
        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.VIEW_MESSAGE_BY_ID_VALUE, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
        case ResponseCode.SUCCESS_VALUE: {
            ByteBuffer byteBuffer = ByteBuffer.wrap(response.getBody());
            return MessageDecoder.decode(byteBuffer);
        }
        default:
            break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    /**
     * 根据时间查询Offset
     */
    public long searchOffset(final String addr, final String topic, final int queueId, final long timestamp,
            final long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException {
        SearchOffsetRequestHeader requestHeader = new SearchOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        requestHeader.setTimestamp(timestamp);
        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.SEARCH_OFFSET_BY_TIMESTAMP_VALUE,
                    requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
        case ResponseCode.SUCCESS_VALUE: {
            SearchOffsetResponseHeader responseHeader =
                    (SearchOffsetResponseHeader) response
                        .decodeCommandCustomHeader(SearchOffsetResponseHeader.class);
            return responseHeader.getOffset();
        }
        default:
            break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    /**
     * 获取队列的最大Offset
     */
    public long getMaxOffset(final String addr, final String topic, final int queueId,
            final long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException {
        GetMaxOffsetRequestHeader requestHeader = new GetMaxOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.GET_MAX_OFFSET_VALUE, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
        case ResponseCode.SUCCESS_VALUE: {
            GetMaxOffsetResponseHeader responseHeader =
                    (GetMaxOffsetResponseHeader) response
                        .decodeCommandCustomHeader(GetMaxOffsetResponseHeader.class);

            return responseHeader.getOffset();
        }
        default:
            break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    /**
     * 获取某个组的Consumer Id列表
     */
    public List<String> getConsumerIdListByGroup(//
            final String addr, //
            final String consumerGroup, //
            final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, MQBrokerException, InterruptedException {
        GetConsumerListByGroupRequestHeader requestHeader = new GetConsumerListByGroupRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.GET_CONSUMER_LIST_BY_GROUP_VALUE,
                    requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
        case ResponseCode.SUCCESS_VALUE: {
            if (response.getBody() != null) {
                GetConsumerListByGroupResponseBody body =
                        GetConsumerListByGroupResponseBody.decode(response.getBody(),
                            GetConsumerListByGroupResponseBody.class);
                return body.getConsumerIdList();
            }
        }
        default:
            break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    /**
     * 获取队列的最小Offset
     */
    public long getMinOffset(final String addr, final String topic, final int queueId,
            final long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException {
        GetMinOffsetRequestHeader requestHeader = new GetMinOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.GET_MIN_OFFSET_VALUE, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
        case ResponseCode.SUCCESS_VALUE: {
            GetMinOffsetResponseHeader responseHeader =
                    (GetMinOffsetResponseHeader) response
                        .decodeCommandCustomHeader(GetMinOffsetResponseHeader.class);

            return responseHeader.getOffset();
        }
        default:
            break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    /**
     * 获取队列的最早时间
     */
    public long getEarliestMsgStoretime(final String addr, final String topic, final int queueId,
            final long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException {

        GetEarliestMsgStoretimeRequestHeader requestHeader = new GetEarliestMsgStoretimeRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.GET_EARLIEST_MSG_STORETIME_VALUE,
                    requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
        case ResponseCode.SUCCESS_VALUE: {
            GetEarliestMsgStoretimeResponseHeader responseHeader =
                    (GetEarliestMsgStoretimeResponseHeader) response
                        .decodeCommandCustomHeader(GetEarliestMsgStoretimeResponseHeader.class);

            return responseHeader.getTimestamp();
        }
        default:
            break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    /**
     * 查询Consumer消费进度
     */
    public long queryConsumerOffset(//
            final String addr,//
            final QueryConsumerOffsetRequestHeader requestHeader,//
            final long timeoutMillis//
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request =
                RemotingCommand
                    .createRequestCommand(MQRequestCode.QUERY_CONSUMER_OFFSET_VALUE, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
        case ResponseCode.SUCCESS_VALUE: {
            QueryConsumerOffsetResponseHeader responseHeader =
                    (QueryConsumerOffsetResponseHeader) response
                        .decodeCommandCustomHeader(QueryConsumerOffsetResponseHeader.class);

            return responseHeader.getOffset();
        }
        default:
            break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    /**
     * 更新Consumer消费进度
     */
    public void updateConsumerOffset(//
            final String addr,//
            final UpdateConsumerOffsetRequestHeader requestHeader,//
            final long timeoutMillis//
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.UPDATE_CONSUMER_OFFSET_VALUE,
                    requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
        case ResponseCode.SUCCESS_VALUE: {
            return;
        }
        default:
            break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    /**
     * 更新Consumer消费进度
     * 
     * @throws InterruptedException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingTooMuchRequestException
     * 
     * @throws RemotingConnectException
     */
    public void updateConsumerOffsetOneway(//
            final String addr,//
            final UpdateConsumerOffsetRequestHeader requestHeader,//
            final long timeoutMillis//
    ) throws RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException,
            RemotingSendRequestException, InterruptedException {
        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.UPDATE_CONSUMER_OFFSET_VALUE,
                    requestHeader);
        this.remotingClient.invokeOneway(addr, request, timeoutMillis);
    }


    /**
     * 发送心跳
     */
    public void sendHearbeat(//
            final String addr,//
            final HeartbeatData heartbeatData,//
            final long timeoutMillis//
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(MQRequestCode.HEART_BEAT_VALUE, null);
        request.setBody(heartbeatData.encode());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
        case ResponseCode.SUCCESS_VALUE: {
            return;
        }
        default:
            break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    /**
     * 发送心跳
     */
    public void unregisterClient(//
            final String addr,//
            final String clientID,//
            final String producerGroup,//
            final String consumerGroup,//
            final long timeoutMillis//
    ) throws RemotingException, MQBrokerException, InterruptedException {
        final UnregisterClientRequestHeader requestHeader = new UnregisterClientRequestHeader();
        requestHeader.setClientID(clientID);
        requestHeader.setProducerGroup(producerGroup);
        requestHeader.setConsumerGroup(consumerGroup);
        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.UNREGISTER_CLIENT_VALUE, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
        case ResponseCode.SUCCESS_VALUE: {
            return;
        }
        default:
            break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    /**
     * 提交或者回滚事务
     */
    public void endTransactionOneway(//
            final String addr,//
            final EndTransactionRequestHeader requestHeader,//
            final String remark,//
            final long timeoutMillis//
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.END_TRANSACTION_VALUE, requestHeader);
        request.setRemark(remark);
        this.remotingClient.invokeOneway(addr, request, timeoutMillis);
    }


    /**
     * 查询消息
     */
    public void queryMessage(//
            final String addr,//
            final QueryMessageRequestHeader requestHeader,//
            final long timeoutMillis,//
            final InvokeCallback invokeCallback//
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.QUERY_MESSAGE_VALUE, requestHeader);
        this.remotingClient.invokeAsync(addr, request, timeoutMillis, invokeCallback);
    }


    public boolean registerClient(final String addr, final HeartbeatData heartbeat, final long timeoutMillis)
            throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(MQRequestCode.HEART_BEAT_VALUE, null);
        request.setBody(heartbeat.encode());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        return response.getCode() == ResponseCode.SUCCESS_VALUE;
    }


    /**
     * 失败的消息发回Broker
     */
    public void consumerSendMessageBack(//
            final MessageExt msg,//
            final String consumerGroup,//
            final int delayLevel,//
            final long timeoutMillis//
    ) throws RemotingException, MQBrokerException, InterruptedException {
        ConsumerSendMsgBackRequestHeader requestHeader = new ConsumerSendMsgBackRequestHeader();
        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.CONSUMER_SEND_MSG_BACK_VALUE,
                    requestHeader);
        requestHeader.setGroup(consumerGroup);
        requestHeader.setOffset(msg.getCommitLogOffset());
        requestHeader.setDelayLevel(delayLevel);

        String addr = RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
        case ResponseCode.SUCCESS_VALUE: {
            return;
        }
        default:
            break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    public Set<MessageQueue> lockBatchMQ(//
            final String addr,//
            final LockBatchRequestBody requestBody,//
            final long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.LOCK_BATCH_MQ_VALUE, null);
        request.setBody(requestBody.encode());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        switch (response.getCode()) {
        case ResponseCode.SUCCESS_VALUE: {
            LockBatchResponseBody responseBody =
                    LockBatchResponseBody.decode(response.getBody(), LockBatchResponseBody.class);
            return responseBody.getLockOKMQSet();
        }
        default:
            break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    public void unlockBatchMQ(//
            final String addr,//
            final UnlockBatchRequestBody requestBody,//
            final long timeoutMillis,//
            final boolean oneway//
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.UNLOCK_BATCH_MQ_VALUE, null);
        request.setBody(requestBody.encode());

        if (oneway) {
            this.remotingClient.invokeOneway(addr, request, timeoutMillis);
        }
        else {
            RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
            switch (response.getCode()) {
            case ResponseCode.SUCCESS_VALUE: {
                return;
            }
            default:
                break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark());
        }
    }


    public TopicStatsTable getTopicStatsInfo(final String addr, final String topic, final long timeoutMillis)
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
            RemotingConnectException, MQBrokerException {
        GetTopicStatsInfoRequestHeader requestHeader = new GetTopicStatsInfoRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.GET_TOPIC_STATS_INFO_VALUE, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        switch (response.getCode()) {
        case ResponseCode.SUCCESS_VALUE: {
            return TopicStatsTable.decode(response.getBody(), TopicStatsTable.class);
        }
        default:
            break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    /**
     * 根据ProducerGroup获取Producer连接列表
     */
    public ProducerConnection getProducerConnectionList(final String addr,
            final String producerGroup, final long timeoutMillis) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        GetProducerConnectionListRequestHeader requestHeader = new GetProducerConnectionListRequestHeader();
        requestHeader.setProducerGroup(producerGroup);

        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.GET_PRODUCER_CONNECTION_LIST_VALUE,
                    requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        switch (response.getCode()) {
        case ResponseCode.SUCCESS_VALUE: {
            return ProducerConnection.decode(response.getBody(),
                ProducerConnection.class);
        }
        default:
            break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    /**
     * 根据ConsumerGroup获取Consumer连接列表以及订阅关系
     */
    public ConsumerConnection getConsumerConnectionList(final String addr,
            final String consumerGroup, final long timeoutMillis) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        GetConsumerConnectionListRequestHeader requestHeader = new GetConsumerConnectionListRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);

        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.GET_CONSUMER_CONNECTION_LIST_VALUE,
                    requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        switch (response.getCode()) {
        case ResponseCode.SUCCESS_VALUE: {
            return ConsumerConnection.decode(response.getBody(),
                ConsumerConnection.class);
        }
        default:
            break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    /**
     * Name Server: 从Name Server获取集群信息
     */
    public ClusterInfo getBrokerClusterInfo(final long timeoutMillis)
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
            RemotingConnectException, MQBrokerException {
        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.GET_BROKER_CLUSTER_INFO_VALUE, null);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
        case ResponseCode.SUCCESS_VALUE: {
            ClusterInfo responseBody =
                    ClusterInfo.decode(response.getBody(), ClusterInfo.class);
            return responseBody;
        }
        default:
            break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());

    }


    /**
     * Name Server: 从Name Server获取Topic路由信息
     */
    public TopicRouteData getTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.GET_ROUTEINTO_BY_TOPIC_VALUE,
                    requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
        case MQResponseCode.TOPIC_NOT_EXIST_VALUE: {
            // TODO LOG
            break;
        }
        case ResponseCode.SUCCESS_VALUE: {
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


    public int wipeWritePermOfBroker(final String namesrvAddr, String brokerName, final long timeoutMillis)
            throws RemotingCommandException, RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, InterruptedException, MQClientException {
        WipeWritePermOfBrokerRequestHeader requestHeader = new WipeWritePermOfBrokerRequestHeader();
        requestHeader.setBrokerName(brokerName);

        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.WIPE_WRITE_PERM_OF_BROKER_VALUE,
                    requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
        case ResponseCode.SUCCESS_VALUE: {
            WipeWritePermOfBrokerResponseHeader responseHeader =
                    (WipeWritePermOfBrokerResponseHeader) response
                        .decodeCommandCustomHeader(WipeWritePermOfBrokerResponseHeader.class);
            return responseHeader.getWipeTopicCount();
        }
        default:
            break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }


    /**
     * Name Server: 仅仅为测试服务，可以绕过Name Server
     */
    public TopicRouteData getTopicRouteInfoFromNameServer_test(final String topic, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setOrderTopicConf("dev170021.sqa.cm6:4;");
        List<BrokerData> brokerDatas = new ArrayList<BrokerData>();
        BrokerData bd = new BrokerData();

        HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs = new HashMap<Long, String>();
        // brokerAddrs.put(0L, "10.235.170.21:10911");
        brokerAddrs.put(0L, "127.0.0.1:10911");
        brokerAddrs.put(1L, "127.0.0.1:10913");
        bd.setBrokerName("dev170021.sqa.cm6");
        bd.setBrokerAddrs(brokerAddrs);

        brokerDatas.add(bd);

        topicRouteData.setBrokerDatas(brokerDatas);
        List<QueueData> queueDatas = new ArrayList<QueueData>();
        QueueData queueData = new QueueData();
        queueData.setBrokerName("dev170021.sqa.cm6");
        queueData.setPerm(6);
        queueData.setReadQueueNums(4);
        queueData.setWriteQueueNums(4);
        queueDatas.add(queueData);
        topicRouteData.setQueueDatas(queueDatas);

        return topicRouteData;
    }
}
