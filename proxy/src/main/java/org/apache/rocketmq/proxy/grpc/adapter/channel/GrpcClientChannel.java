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
package org.apache.rocketmq.proxy.grpc.adapter.channel;

import apache.rocketmq.v1.PollCommandResponse;
import apache.rocketmq.v1.PrintThreadStackTraceCommand;
import apache.rocketmq.v1.RecoverOrphanedTransactionCommand;
import io.netty.channel.ChannelFuture;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.proxy.channel.ChannelManager;
import org.apache.rocketmq.proxy.channel.SimpleChannel;
import org.apache.rocketmq.proxy.grpc.adapter.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.adapter.PollResponseManager;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class GrpcClientChannel extends SimpleChannel {
    private final AtomicReference<CompletableFuture<PollCommandResponse>> pollCommandResponseFutureRef = new AtomicReference<>();

    private final String group;
    private final String clientId;
    private final PollResponseManager manager;

    private GrpcClientChannel(String group, String clientId, PollResponseManager manager) {
        super(ChannelManager.createSimpleChannelDirectly());
        this.group = group;
        this.clientId = clientId;
        this.manager = manager;
    }

    public void addClientObserver(CompletableFuture<PollCommandResponse> future) {
        this.pollCommandResponseFutureRef.set(future);
    }

    public static GrpcClientChannel create(
        ChannelManager channelManager,
        String group,
        String clientId,
        PollResponseManager manager
    ) {
        GrpcClientChannel channel = channelManager.createChannel(
            buildKey(group, clientId),
            () -> new GrpcClientChannel(group, clientId, manager),
            GrpcClientChannel.class
        );

        channelManager.addGroupClientId(group, clientId);
        return channel;
    }

    public static GrpcClientChannel getChannel(ChannelManager channelManager, String group, String clientId) {
        return channelManager.getChannel(buildKey(group, clientId), GrpcClientChannel.class);
    }

    public static GrpcClientChannel removeChannel(ChannelManager channelManager, String group, String clientId) {
        return channelManager.removeChannel(buildKey(group, clientId), GrpcClientChannel.class);
    }

    private static String buildKey(String group, String clientId) {
        return group + "@" + clientId;
    }

    /**
     * Write response to corresponding remote client
     *
     * @param msg Target write object, {@link RemotingCommand} or {@link PollCommandResponse}
     * @return Always success {@link ChannelFuture}
     * <p>
     * Case {@link RequestCode#CHECK_TRANSACTION_STATE}
     * @see org.apache.rocketmq.broker.client.net.Broker2Client#checkProducerTransactionState
     */
    @Override
    public ChannelFuture writeAndFlush(Object msg) {
        CompletableFuture<PollCommandResponse> future = pollCommandResponseFutureRef.get();
        if (msg instanceof RemotingCommand) {
            RemotingCommand command = (RemotingCommand) msg;
            try {
                switch (command.getCode()) {
                    case RequestCode.CHECK_TRANSACTION_STATE: {
                        final CheckTransactionStateRequestHeader requestHeader =
                            (CheckTransactionStateRequestHeader) command.decodeCommandCustomHeader(CheckTransactionStateRequestHeader.class);
                        MessageExt messageExt = MessageDecoder.decode(ByteBuffer.wrap(command.getBody()), true, false, false);
                        future.complete(PollCommandResponse.newBuilder()
                            .setRecoverOrphanedTransactionCommand(RecoverOrphanedTransactionCommand.newBuilder()
                                .setTransactionId(requestHeader.getTransactionId())
                                .setOrphanedTransactionalMessage(GrpcConverter.buildMessage(messageExt))
                                .build())
                            .build());
                        break;
                    }
                    case RequestCode.GET_CONSUMER_RUNNING_INFO: {
                        final GetConsumerRunningInfoRequestHeader requestHeader =
                            (GetConsumerRunningInfoRequestHeader) command.decodeCommandCustomHeader(GetConsumerRunningInfoRequestHeader.class);
                        if (!requestHeader.isJstackEnable()) {
                            break;
                        }
                        String commandId = manager.putResponse(command.getOpaque());
                        future.complete(PollCommandResponse.newBuilder()
                            .setPrintThreadStackTraceCommand(PrintThreadStackTraceCommand.newBuilder()
                                .setCommandId(commandId)
                                .build())
                            .build());
                        break;
                    }
                }
            } catch (Exception ignore) {

            }
        }
        if (msg instanceof PollCommandResponse) {
            PollCommandResponse response = (PollCommandResponse) msg;
            future.complete(response);
        }
        return super.writeAndFlush(msg);
    }

    public String getGroup() {
        return group;
    }

    public String getClientId() {
        return clientId;
    }
}
