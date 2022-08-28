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
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.VerifyMessageCommand;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import com.google.protobuf.TextFormat;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.netty.channel.ChannelId;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcConverter;
import org.apache.rocketmq.proxy.service.relay.ProxyChannel;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayResult;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.proxy.service.transaction.TransactionData;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class GrpcClientChannel extends ProxyChannel {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    protected static final String SEPARATOR = "@";

    private final GrpcChannelManager grpcChannelManager;

    private final AtomicReference<StreamObserver<TelemetryCommand>> telemetryCommandRef = new AtomicReference<>();
    private final Object telemetryWriteLock = new Object();
    private final String group;
    private final String clientId;

    public GrpcClientChannel(ProxyRelayService proxyRelayService, GrpcChannelManager grpcChannelManager,
        ProxyContext ctx,
        String group, String clientId) {
        super(proxyRelayService, null, new GrpcChannelId(group, clientId),
            ctx.getRemoteAddress(),
            ctx.getLocalAddress());
        this.grpcChannelManager = grpcChannelManager;
        this.group = group;
        this.clientId = clientId;
    }

    protected static class GrpcChannelId implements ChannelId {

        private final String group;
        private final String clientId;

        public GrpcChannelId(String group, String clientId) {
            this.group = group;
            this.clientId = clientId;
        }

        @Override
        public String asShortText() {
            return this.clientId;
        }

        @Override
        public String asLongText() {
            return this.group + SEPARATOR + this.clientId;
        }

        @Override
        public int compareTo(ChannelId o) {
            if (this == o) {
                return 0;
            }
            if (o instanceof GrpcChannelId) {
                GrpcChannelId other = (GrpcChannelId) o;
                return ComparisonChain.start()
                    .compare(this.group, other.group)
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
        if (!header.isJstackEnable()) {
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

    public String getGroup() {
        return group;
    }

    public String getClientId() {
        return clientId;
    }

    public String getRemoteAddress() {
        return remoteAddress;
    }

    public String getLocalAddress() {
        return localAddress;
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
            .add("group", group)
            .add("clientId", clientId)
            .add("remoteAddress", getRemoteAddress())
            .add("localAddress", getLocalAddress())
            .toString();
    }
}
