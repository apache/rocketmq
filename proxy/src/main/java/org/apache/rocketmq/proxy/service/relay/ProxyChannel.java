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

package org.apache.rocketmq.proxy.service.relay;

import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.GlobalEventExecutor;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.channel.SimpleChannel;
import org.apache.rocketmq.proxy.service.transaction.TransactionData;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;

public abstract class ProxyChannel extends SimpleChannel {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    protected final SocketAddress remoteSocketAddress;
    protected final SocketAddress localSocketAddress;

    protected final ProxyRelayService proxyRelayService;

    protected ProxyChannel(ProxyRelayService proxyRelayService, Channel parent, String remoteAddress,
        String localAddress) {
        super(parent, remoteAddress, localAddress);
        this.proxyRelayService = proxyRelayService;
        this.remoteSocketAddress = NetworkUtil.string2SocketAddress(remoteAddress);
        this.localSocketAddress = NetworkUtil.string2SocketAddress(localAddress);
    }

    protected ProxyChannel(ProxyRelayService proxyRelayService, Channel parent, ChannelId id, String remoteAddress,
        String localAddress) {
        super(parent, id, remoteAddress, localAddress);
        this.proxyRelayService = proxyRelayService;
        this.remoteSocketAddress = NetworkUtil.string2SocketAddress(remoteAddress);
        this.localSocketAddress = NetworkUtil.string2SocketAddress(localAddress);
    }

    @Override
    public ChannelFuture writeAndFlush(Object msg) {
        CompletableFuture<Void> processFuture = new CompletableFuture<>();

        try {
            if (msg instanceof RemotingCommand) {
                ProxyContext context = ProxyContext.createForInner(this.getClass())
                    .setRemoteAddress(remoteAddress)
                    .setLocalAddress(localAddress);
                RemotingCommand command = (RemotingCommand) msg;
                if (command.getExtFields() == null) {
                    command.setExtFields(new HashMap<>());
                }
                switch (command.getCode()) {
                    case RequestCode.CHECK_TRANSACTION_STATE: {
                        CheckTransactionStateRequestHeader header = (CheckTransactionStateRequestHeader) command.readCustomHeader();
                        MessageExt messageExt = MessageDecoder.decode(ByteBuffer.wrap(command.getBody()), true, false, false);
                        RelayData<TransactionData, Void> relayData = this.proxyRelayService.processCheckTransactionState(context, command, header, messageExt);
                        processFuture = this.processCheckTransaction(header, messageExt, relayData.getProcessResult(), relayData.getRelayFuture());
                        break;
                    }
                    case RequestCode.GET_CONSUMER_RUNNING_INFO: {
                        GetConsumerRunningInfoRequestHeader header = (GetConsumerRunningInfoRequestHeader) command.readCustomHeader();
                        CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>> relayFuture = this.proxyRelayService.processGetConsumerRunningInfo(context, command, header);
                        processFuture = this.processGetConsumerRunningInfo(command, header, relayFuture);
                        break;
                    }
                    case RequestCode.CONSUME_MESSAGE_DIRECTLY: {
                        ConsumeMessageDirectlyResultRequestHeader header = (ConsumeMessageDirectlyResultRequestHeader) command.readCustomHeader();
                        MessageExt messageExt = MessageDecoder.decode(ByteBuffer.wrap(command.getBody()), true, false, false);
                        processFuture = this.processConsumeMessageDirectly(command, header, messageExt,
                            this.proxyRelayService.processConsumeMessageDirectly(context, command, header));
                        break;
                    }
                    default:
                        break;
                }
            } else {
                processFuture = processOtherMessage(msg);
            }
        } catch (Throwable t) {
            log.error("process failed. msg:{}", msg, t);
            processFuture.completeExceptionally(t);
        }

        DefaultChannelPromise promise = new DefaultChannelPromise(this, GlobalEventExecutor.INSTANCE);
        processFuture.thenAccept(ignore -> promise.setSuccess())
            .exceptionally(t -> {
                promise.setFailure(t);
                return null;
            });
        return promise;
    }

    protected abstract CompletableFuture<Void> processOtherMessage(Object msg);

    protected abstract CompletableFuture<Void> processCheckTransaction(
        CheckTransactionStateRequestHeader header,
        MessageExt messageExt,
        TransactionData transactionData,
        CompletableFuture<ProxyRelayResult<Void>> responseFuture);

    protected abstract CompletableFuture<Void> processGetConsumerRunningInfo(
        RemotingCommand command,
        GetConsumerRunningInfoRequestHeader header,
        CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>> responseFuture);

    protected abstract CompletableFuture<Void> processConsumeMessageDirectly(
        RemotingCommand command,
        ConsumeMessageDirectlyResultRequestHeader header,
        MessageExt messageExt,
        CompletableFuture<ProxyRelayResult<ConsumeMessageDirectlyResult>> responseFuture);

    @Override
    public ChannelConfig config() {
        return null;
    }

    @Override
    public ChannelMetadata metadata() {
        return null;
    }

    @Override
    protected AbstractUnsafe newUnsafe() {
        return null;
    }

    @Override
    protected boolean isCompatible(EventLoop loop) {
        return false;
    }

    @Override
    protected void doBind(SocketAddress localAddress) throws Exception {

    }

    @Override
    protected void doDisconnect() throws Exception {

    }

    @Override
    protected void doClose() throws Exception {

    }

    @Override
    protected void doBeginRead() throws Exception {

    }

    @Override
    protected void doWrite(ChannelOutboundBuffer in) throws Exception {

    }

    @Override
    protected SocketAddress localAddress0() {
        return this.localSocketAddress;
    }

    @Override
    protected SocketAddress remoteAddress0() {
        return this.remoteSocketAddress;
    }
}
