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
package org.apache.rocketmq.proxy.grpc.v2.channel;

import apache.rocketmq.v2.PrintThreadStackTraceCommand;
import apache.rocketmq.v2.RecoverOrphanedTransactionCommand;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.VerifyMessageCommand;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import com.google.protobuf.util.JsonFormat;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.channel.ChannelHelper;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcConverter;
import org.apache.rocketmq.proxy.processor.channel.ChannelExtendAttributeGetter;
import org.apache.rocketmq.proxy.processor.channel.ChannelProtocolType;
import org.apache.rocketmq.proxy.processor.channel.RemoteChannel;
import org.apache.rocketmq.proxy.processor.channel.RemoteChannelConverter;
import org.apache.rocketmq.proxy.service.relay.ProxyChannel;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayResult;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.proxy.service.transaction.TransactionData;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;

public class GrpcClientChannel extends ProxyChannel implements ChannelExtendAttributeGetter, RemoteChannelConverter {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final GrpcChannelManager grpcChannelManager;
    private final GrpcClientSettingsManager grpcClientSettingsManager;

    private final AtomicReference<StreamObserver<TelemetryCommand>> telemetryCommandRef = new AtomicReference<>();
    private final Object telemetryWriteLock = new Object();
    private final String clientId;

    public GrpcClientChannel(ProxyRelayService proxyRelayService, GrpcClientSettingsManager grpcClientSettingsManager,
        GrpcChannelManager grpcChannelManager, ProxyContext ctx, String clientId) {
        super(proxyRelayService, null, new GrpcChannelId(clientId),
            ctx.getRemoteAddress(),
            ctx.getLocalAddress());
        this.grpcChannelManager = grpcChannelManager;
        this.grpcClientSettingsManager = grpcClientSettingsManager;
        this.clientId = clientId;
    }

    @Override
    public String getChannelExtendAttribute() {
        Settings settings = this.grpcClientSettingsManager.getRawClientSettings(this.clientId);
        if (settings == null) {
            return null;
        }
        try {
            return JsonFormat.printer().print(settings);
        } catch (InvalidProtocolBufferException e) {
            log.error("convert settings to json data failed. settings:{}", settings, e);
        }
        return null;
    }

    public static Settings parseChannelExtendAttribute(Channel channel) {
        if (ChannelHelper.getChannelProtocolType(channel).equals(ChannelProtocolType.GRPC_V2) &&
            channel instanceof ChannelExtendAttributeGetter) {
            String attr = ((ChannelExtendAttributeGetter) channel).getChannelExtendAttribute();
            if (attr == null) {
                return null;
            }

            Settings.Builder builder = Settings.newBuilder();
            try {
                JsonFormat.parser().merge(attr, builder);
                return builder.build();
            } catch (InvalidProtocolBufferException e) {
                log.error("convert settings json data to settings failed. data:{}", attr, e);
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
            ChannelProtocolType.GRPC_V2,
            this.getChannelExtendAttribute());
    }

    protected static class GrpcChannelId implements ChannelId {

        private final String clientId;

        public GrpcChannelId(String clientId) {
            this.clientId = clientId;
        }

        @Override
        public String asShortText() {
            return this.clientId;
        }

        @Override
        public String asLongText() {
            return this.clientId;
        }

        @Override
        public int compareTo(ChannelId o) {
            if (this == o) {
                return 0;
            }
            if (o instanceof GrpcChannelId) {
                GrpcChannelId other = (GrpcChannelId) o;
                return ComparisonChain.start()
                    .compare(this.clientId, other.clientId)
                    .result();
            }

            return asLongText().compareTo(o.asLongText());
        }
    }

    public void setClientObserver(StreamObserver<TelemetryCommand> future) {
        this.telemetryCommandRef.set(future);
    }

    protected void clearClientObserver(StreamObserver<TelemetryCommand> future) {
        this.telemetryCommandRef.compareAndSet(future, null);
    }

    @Override
    public boolean isOpen() {
        return this.telemetryCommandRef.get() != null;
    }

    @Override
    public boolean isActive() {
        return this.telemetryCommandRef.get() != null;
    }

    @Override
    public boolean isWritable() {
        return this.telemetryCommandRef.get() != null;
    }

    @Override
    protected CompletableFuture<Void> processOtherMessage(Object msg) {
        if (msg instanceof TelemetryCommand) {
            TelemetryCommand response = (TelemetryCommand) msg;
            this.writeTelemetryCommand(response);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected CompletableFuture<Void> processCheckTransaction(CheckTransactionStateRequestHeader header,
        MessageExt messageExt, TransactionData transactionData, CompletableFuture<ProxyRelayResult<Void>> responseFuture) {
        CompletableFuture<Void> writeFuture = new CompletableFuture<>();
        try {
            this.writeTelemetryCommand(TelemetryCommand.newBuilder()
                .setRecoverOrphanedTransactionCommand(RecoverOrphanedTransactionCommand.newBuilder()
                    .setTransactionId(transactionData.getTransactionId())
                    .setMessage(GrpcConverter.getInstance().buildMessage(messageExt))
                    .build())
                .build());
            responseFuture.complete(null);
            writeFuture.complete(null);
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
        if (Objects.isNull(header) || !header.isJstackEnable()) {
            return CompletableFuture.completedFuture(null);
        }
        this.writeTelemetryCommand(TelemetryCommand.newBuilder()
            .setPrintThreadStackTraceCommand(PrintThreadStackTraceCommand.newBuilder()
                .setNonce(this.grpcChannelManager.addResponseFuture(responseFuture))
                .build())
            .build());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected CompletableFuture<Void> processConsumeMessageDirectly(RemotingCommand command,
        ConsumeMessageDirectlyResultRequestHeader header,
        MessageExt messageExt, CompletableFuture<ProxyRelayResult<ConsumeMessageDirectlyResult>> responseFuture) {
        this.writeTelemetryCommand(TelemetryCommand.newBuilder()
            .setVerifyMessageCommand(VerifyMessageCommand.newBuilder()
                .setNonce(this.grpcChannelManager.addResponseFuture(responseFuture))
                .setMessage(GrpcConverter.getInstance().buildMessage(messageExt))
                .build())
            .build());
        return CompletableFuture.completedFuture(null);
    }

    public String getClientId() {
        return clientId;
    }

    public void writeTelemetryCommand(TelemetryCommand command) {
        StreamObserver<TelemetryCommand> observer = this.telemetryCommandRef.get();
        if (observer == null) {
            log.warn("telemetry command observer is null when try to write data. command:{}, channel:{}", TextFormat.shortDebugString(command), this);
            return;
        }
        synchronized (this.telemetryWriteLock) {
            observer = this.telemetryCommandRef.get();
            if (observer == null) {
                log.warn("telemetry command observer is null when try to write data. command:{}, channel:{}", TextFormat.shortDebugString(command), this);
                return;
            }
            try {
                observer.onNext(command);
            } catch (StatusRuntimeException | IllegalStateException exception) {
                log.warn("write telemetry failed. command:{}", command, exception);
                this.clearClientObserver(observer);
            }
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("clientId", clientId)
            .add("remoteAddress", getRemoteAddress())
            .add("localAddress", getLocalAddress())
            .toString();
    }
}
