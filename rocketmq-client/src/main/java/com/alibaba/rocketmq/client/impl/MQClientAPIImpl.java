/**
 * $Id: MQClientAPIImpl.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.alibaba.rocketmq.client.consumer.PullCallback;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.PullStatus;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.consumer.PullResultExt;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.common.Message;
import com.alibaba.rocketmq.common.MessageDecoder;
import com.alibaba.rocketmq.common.MessageExt;
import com.alibaba.rocketmq.common.MessageQueue;
import com.alibaba.rocketmq.common.MQVersion;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.namesrv.TopAddressing;
import com.alibaba.rocketmq.common.protocol.MQProtos.MQRequestCode;
import com.alibaba.rocketmq.common.protocol.MQProtos.MQResponseCode;
import com.alibaba.rocketmq.common.protocol.header.CreateTopicRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetEarliestMsgStoretimeRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetEarliestMsgStoretimeResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.GetMaxOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetMaxOffsetResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.GetMinOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetMinOffsetResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.PullMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.PullMessageResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.QueryConsumerOffsetResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.QueryMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.SearchOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.SearchOffsetResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.SendMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.SendMessageResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.ViewMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.RegisterOrderTopicRequestHeader;
import com.alibaba.rocketmq.common.protocol.heartbeat.HeartbeatData;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.common.protocol.route.QueueData;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.remoting.InvokeCallback;
import com.alibaba.rocketmq.remoting.RemotingClient;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingClient;
import com.alibaba.rocketmq.remoting.netty.ResponseFuture;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.ResponseCode;
import com.google.protobuf.InvalidProtocolBufferException;


/**
 * 封装所有与服务器通信部分API
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class MQClientAPIImpl {
    private final RemotingClient remotingClient;
    private final TopAddressing topAddressing = new TopAddressing();
    private String nameSrvAddr = null;

    static {
        System.setProperty(RemotingCommand.RemotingVersionKey, Integer.toString(MQVersion.CurrentVersion));
    }


    public MQClientAPIImpl(final NettyClientConfig nettyClientConfig) {
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
    }


    public void fetchNameServerAddr() {
        try {
            String addrs = this.topAddressing.fetchNSAddr();
            if (addrs != null) {
                if (!addrs.equals(this.nameSrvAddr)) {
                    // log.info("name server address changed, old: " +
                    // this.nameSrvAddr + " new: " + addrs);
                    this.updateNameServerAddressList(addrs);
                    this.nameSrvAddr = addrs;
                }
            }
        }
        catch (Exception e) {
            // log.error(e);
        }
    }


    public void start() {
        this.remotingClient.start();
    }


    public void shutdown() {
        this.remotingClient.shutdown();
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
                RemotingCommand.createRequestCommand(MQRequestCode.UPDATE_AND_CREATE_TOPIC_VALUE, requestHeader);

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
                        sendCallback.onException(new MQClientException("wait response timeout", responseFuture
                            .getCause()));
                    }
                    else {
                        sendCallback.onException(new MQClientException("unknow reseaon", responseFuture.getCause()));
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

            MessageQueue messageQueue = new MessageQueue(msg.getTopic(), brokerName, responseHeader.getQueueId());
            return new SendResult(sendStatus, responseHeader.getMsgId(), messageQueue,
                responseHeader.getQueueOffset());
        }
        default:
            break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
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
                (PullMessageResponseHeader) response.decodeCommandCustomHeader(PullMessageResponseHeader.class);

        return new PullResultExt(pullStatus, responseHeader.getNextBeginOffset(), responseHeader.getMinOffset(),
            responseHeader.getMaxOffset(), null, responseHeader.getSuggestPullingFromSlave(), response.getBody());
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
                        pullCallback.onException(new MQClientException("wait response timeout", responseFuture
                            .getCause()));
                    }
                    else {
                        pullCallback.onException(new MQClientException("unknow reseaon", responseFuture.getCause()));
                    }
                }
            }
        });
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
                RemotingCommand
                    .createRequestCommand(MQRequestCode.SEARCH_OFFSET_BY_TIMESTAMP_VALUE, requestHeader);

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
    public long getMaxOffset(final String addr, final String topic, final int queueId, final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException {
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
     * 获取队列的最小Offset
     */
    public long getMinOffset(final String addr, final String topic, final int queueId, final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException {
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
                RemotingCommand
                    .createRequestCommand(MQRequestCode.GET_EARLIEST_MSG_STORETIME_VALUE, requestHeader);

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
                RemotingCommand.createRequestCommand(MQRequestCode.QUERY_CONSUMER_OFFSET_VALUE, requestHeader);
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
                RemotingCommand.createRequestCommand(MQRequestCode.UPDATE_CONSUMER_OFFSET_VALUE, requestHeader);
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
    public void sendHearbeat(//
            final String addr,//
            final HeartbeatData heartbeatData,//
            final long timeoutMillis//
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(MQRequestCode.HEART_BEAT_VALUE, null);
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
     */
    public void endTransaction(//
            final String addr,//
            final EndTransactionRequestHeader requestHeader,//
            final long timeoutMillis//
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.END_TRANSACTION_VALUE, requestHeader);
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
     * 从Name Server获取Topic路由信息
     */
    public TopicRouteData getTopicRouteInfoFromNameServer_real(final String topic, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException, InvalidProtocolBufferException {
        GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.GET_ROUTEINTO_BY_TOPIC_VALUE, requestHeader);

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
                return TopicRouteData.decode(body);
            }
        }
        default:
            break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }


    /**
     * 仅仅为测试服务，可以绕过Name Server
     */
    public TopicRouteData getTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException, InvalidProtocolBufferException {
        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setOrderTopicConf("dev170021.sqa.cm6:4;");
        List<BrokerData> brokerDatas = new ArrayList<BrokerData>();
        BrokerData bd = new BrokerData();

        HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs = new HashMap<Long, String>();
        brokerAddrs.put(0L, "10.235.170.21:10911");

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


    /**
     * 向Name Server注册顺序消息配置
     */
    public void registerOrderTopic(final String topic, final String orderTopicString, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        RegisterOrderTopicRequestHeader requestHeader = new RegisterOrderTopicRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setOrderTopicString(orderTopicString);
        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.REGISTER_ORDER_TOPIC_VALUE, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
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
}
