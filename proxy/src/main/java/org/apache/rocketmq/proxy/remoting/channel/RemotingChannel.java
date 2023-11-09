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

package org.apache.rocketmq.proxy.remoting.channel;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.google.common.base.MoreObjects;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelMetadata;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.channel.ChannelHelper;
import org.apache.rocketmq.common.utils.ExceptionUtils;
import org.apache.rocketmq.common.utils.FutureUtils;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.processor.channel.ChannelExtendAttributeGetter;
import org.apache.rocketmq.proxy.processor.channel.ChannelProtocolType;
import org.apache.rocketmq.proxy.processor.channel.RemoteChannel;
import org.apache.rocketmq.proxy.processor.channel.RemoteChannelConverter;
import org.apache.rocketmq.proxy.remoting.RemotingProxyOutClient;
import org.apache.rocketmq.proxy.remoting.common.RemotingConverter;
import org.apache.rocketmq.proxy.service.relay.ProxyChannel;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayResult;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.proxy.service.transaction.TransactionData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

public class RemotingChannel extends ProxyChannel implements RemoteChannelConverter, ChannelExtendAttributeGetter {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private static final long DEFAULT_MQ_CLIENT_TIMEOUT = Duration.ofSeconds(3).toMillis();
    private final String clientId;
    private final String remoteAddress;
    private final String localAddress;
    private final RemotingProxyOutClient remotingProxyOutClient;
    private final Set<SubscriptionData> subscriptionData;

    public RemotingChannel(RemotingProxyOutClient remotingProxyOutClient, ProxyRelayService proxyRelayService,
        Channel parent,
        String clientId, Set<SubscriptionData> subscriptionData) {
        super(proxyRelayService, parent, parent.id(),
            NetworkUtil.socketAddress2String(parent.remoteAddress()),
            NetworkUtil.socketAddress2String(parent.localAddress()));
        this.remotingProxyOutClient = remotingProxyOutClient;
        this.clientId = clientId;
        this.remoteAddress = NetworkUtil.socketAddress2String(parent.remoteAddress());
        this.localAddress = NetworkUtil.socketAddress2String(parent.localAddress());
        this.subscriptionData = subscriptionData;
    }

    @Override
    public boolean isOpen() {
        return this.parent().isOpen();
    }

    @Override
    public boolean isActive() {
        return this.parent().isActive();
    }

    @Override
    public boolean isWritable() {
        return this.parent().isWritable();
    }

    @Override
    public ChannelFuture close() {
        return this.parent().close();
    }

    @Override
    public ChannelConfig config() {
        return this.parent().config();
    }

    @Override
    public ChannelMetadata metadata() {
        return this.parent().metadata();
    }

    @Override
    protected CompletableFuture<Void> processOtherMessage(Object msg) {
        this.parent().writeAndFlush(msg);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected CompletableFuture<Void> processCheckTransaction(CheckTransactionStateRequestHeader header,
        MessageExt messageExt, TransactionData transactionData,
        CompletableFuture<ProxyRelayResult<Void>> responseFuture) {
        CompletableFuture<Void> writeFuture = new CompletableFuture<>();
        try {
            CheckTransactionStateRequestHeader requestHeader = new CheckTransactionStateRequestHeader();
            requestHeader.setCommitLogOffset(transactionData.getCommitLogOffset());
            requestHeader.setTranStateTableOffset(transactionData.getTranStateTableOffset());
            requestHeader.setTransactionId(transactionData.getTransactionId());
            requestHeader.setMsgId(header.getMsgId());

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CHECK_TRANSACTION_STATE, requestHeader);
            request.setBody(RemotingConverter.getInstance().convertMsgToBytes(messageExt));

            this.parent().writeAndFlush(request).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    responseFuture.complete(null);
                    writeFuture.complete(null);
                } else {
                    Exception e = new RemotingException("write and flush data failed");
                    responseFuture.completeExceptionally(e);
                    writeFuture.completeExceptionally(e);
                }
            });
        } catch (Throwable t) {
            responseFuture.completeExceptionally(t);
            writeFuture.completeExceptionally(t);
        }
        return writeFuture;
    }

    @Override
    protected CompletableFuture<Void> processGetConsumerRunningInfo(RemotingCommand command,
        GetConsumerRunningInfoRequestHeader header,
        CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>> responseFuture) {
        try {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO, header);
            this.remotingProxyOutClient.invokeToClient(this.parent(), request, DEFAULT_MQ_CLIENT_TIMEOUT)
                .thenAccept(response -> {
                    if (response.getCode() == ResponseCode.SUCCESS) {
                        ConsumerRunningInfo consumerRunningInfo = ConsumerRunningInfo.decode(response.getBody(), ConsumerRunningInfo.class);
                        responseFuture.complete(new ProxyRelayResult<>(ResponseCode.SUCCESS, "", consumerRunningInfo));
                    } else {
                        String errMsg = String.format("get consumer running info failed, code:%s remark:%s", response.getCode(), response.getRemark());
                        RuntimeException e = new RuntimeException(errMsg);
                        responseFuture.completeExceptionally(e);
                    }
                })
                .exceptionally(t -> {
                    responseFuture.completeExceptionally(ExceptionUtils.getRealException(t));
                    return null;
                });
            return CompletableFuture.completedFuture(null);
        } catch (Throwable t) {
            responseFuture.completeExceptionally(t);
            return FutureUtils.completeExceptionally(t);
        }
    }

    @Override
    protected CompletableFuture<Void> processConsumeMessageDirectly(RemotingCommand command,
        ConsumeMessageDirectlyResultRequestHeader header, MessageExt messageExt,
        CompletableFuture<ProxyRelayResult<ConsumeMessageDirectlyResult>> responseFuture) {
        try {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUME_MESSAGE_DIRECTLY, header);
            request.setBody(RemotingConverter.getInstance().convertMsgToBytes(messageExt));

            this.remotingProxyOutClient.invokeToClient(this.parent(), request, DEFAULT_MQ_CLIENT_TIMEOUT)
                .thenAccept(response -> {
                    if (response.getCode() == ResponseCode.SUCCESS) {
                        ConsumeMessageDirectlyResult result = ConsumeMessageDirectlyResult.decode(response.getBody(), ConsumeMessageDirectlyResult.class);
                        responseFuture.complete(new ProxyRelayResult<>(ResponseCode.SUCCESS, "", result));
                    } else {
                        String errMsg = String.format("consume message directly failed, code:%s remark:%s", response.getCode(), response.getRemark());
                        RuntimeException e = new RuntimeException(errMsg);
                        responseFuture.completeExceptionally(e);
                    }
                })
                .exceptionally(t -> {
                    responseFuture.completeExceptionally(ExceptionUtils.getRealException(t));
                    return null;
                });
            return CompletableFuture.completedFuture(null);
        } catch (Throwable t) {
            responseFuture.completeExceptionally(t);
            return FutureUtils.completeExceptionally(t);
        }
    }

    public String getClientId() {
        return clientId;
    }

    @Override
    public String getChannelExtendAttribute() {
        if (this.subscriptionData == null) {
            return null;
        }
        return JSON.toJSONString(this.subscriptionData);
    }

    public static Set<SubscriptionData> parseChannelExtendAttribute(Channel channel) {
        if (ChannelHelper.getChannelProtocolType(channel).equals(ChannelProtocolType.REMOTING) &&
            channel instanceof ChannelExtendAttributeGetter) {
            String attr = ((ChannelExtendAttributeGetter) channel).getChannelExtendAttribute();
            if (attr == null) {
                return null;
            }

            try {
                return JSON.parseObject(attr, new TypeReference<Set<SubscriptionData>>() {
                });
            } catch (Exception e) {
                log.error("convert remoting extend attribute to subscriptionDataSet failed. data:{}", attr, e);
                return null;
            }
        }
        return null;
    }

    @Override
    public RemoteChannel toRemoteChannel() {
        return new RemoteChannel(
            ConfigurationManager.getProxyConfig().getLocalServeAddr(),
            this.getRemoteAddress(),
            this.getLocalAddress(),
            ChannelProtocolType.REMOTING,
            this.getChannelExtendAttribute());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("parent", parent())
            .add("clientId", clientId)
            .add("remoteAddress", remoteAddress)
            .add("localAddress", localAddress)
            .add("subscriptionData", subscriptionData)
            .toString();
    }
}
